package database

import (
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/core"
	"xorm.io/xorm"

	"gogs.io/gogs/internal/conf"
)

// newAttachmentTestEngine wires a fresh SQLite engine and an isolated
// attachment directory for the test.
func newAttachmentTestEngine(t *testing.T) {
	t.Helper()

	engine, err := xorm.NewEngine("sqlite3", filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetMapper(core.GonicMapper{})
	require.NoError(t, engine.StoreEngine("InnoDB").Sync2(new(Attachment)))

	prevEngine := x
	x = engine
	t.Cleanup(func() { x = prevEngine })

	prevPath := conf.Attachment.Path
	conf.Attachment.Path = t.TempDir()
	t.Cleanup(func() { conf.Attachment.Path = prevPath })

	prevOlderThan := conf.Cron.AttachmentCleanup.OlderThan
	conf.Cron.AttachmentCleanup.OlderThan = time.Hour
	t.Cleanup(func() { conf.Cron.AttachmentCleanup.OlderThan = prevOlderThan })
}

// openUploadFile wraps the content in a multipart.File and returns the first
// sniff buffer the same way the upload handler does.
func openUploadFile(t *testing.T, content []byte) (multipart.File, []byte) {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "upload-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	_, err = f.Write(content)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	buf := make([]byte, 1024)
	n, _ := f.Read(buf)
	return f, buf[:n]
}

func uploadTestAttachment(t *testing.T, uploaderID int64, name string, content []byte) *Attachment {
	t.Helper()

	file, buf := openUploadFile(t, content)
	attach, err := NewAttachment(uploaderID, name, buf, file)
	require.NoError(t, err)
	return attach
}

// insertAttachmentWithFile creates a database record and writes its file to
// disk at the canonical location.
func insertAttachmentWithFile(t *testing.T, a *Attachment) *Attachment {
	t.Helper()

	if a.UUID == "" {
		a.UUID = uuid.New().String()
	}
	require.NoError(t, os.MkdirAll(filepath.Dir(a.LocalPath()), os.ModePerm))
	require.NoError(t, os.WriteFile(a.LocalPath(), []byte("content"), 0o600))
	_, err := x.Insert(a)
	require.NoError(t, err)
	return a
}

// setAttachmentCreatedUnix backdates the record for retention tests.
func setAttachmentCreatedUnix(t *testing.T, id, createdUnix int64) {
	t.Helper()
	_, err := x.ID(id).Cols("created_unix").Update(&Attachment{CreatedUnix: createdUnix})
	require.NoError(t, err)
}

func TestNewAttachment_DeduplicateReplay(t *testing.T) {
	newAttachmentTestEngine(t)

	content := []byte("duplicated upload")

	first := uploadTestAttachment(t, 1, "a.png", content)
	second := uploadTestAttachment(t, 1, "a.png", content)
	assert.Equal(t, first.UUID, second.UUID, "replayed upload must reuse the pending attachment")

	count, err := x.Count(new(Attachment))
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.True(t, first.ID > 0)
	assert.Equal(t, int64(1), first.UploaderID)
	require.FileExists(t, first.LocalPath())

	// Different display names are different attachments even for same content.
	otherName := uploadTestAttachment(t, 1, "b.png", content)
	assert.NotEqual(t, first.UUID, otherName.UUID)

	// Another user uploading the same file gets an independent attachment.
	otherUser := uploadTestAttachment(t, 2, "a.png", content)
	assert.NotEqual(t, first.UUID, otherUser.UUID)

	// Same user and name with different content is a new attachment.
	otherContent := uploadTestAttachment(t, 1, "a.png", []byte("different"))
	assert.NotEqual(t, first.UUID, otherContent.UUID)

	count, err = x.Count(new(Attachment))
	require.NoError(t, err)
	assert.Equal(t, int64(4), count)
}

func TestNewAttachment_ConcurrentReplay(t *testing.T) {
	newAttachmentTestEngine(t)

	content := []byte("concurrent duplicated upload")
	type preparedUpload struct {
		file multipart.File
		buf  []byte
	}
	uploads := make([]preparedUpload, 10)
	for i := range uploads {
		uploads[i].file, uploads[i].buf = openUploadFile(t, content)
	}

	var wg sync.WaitGroup
	uuids := make(chan string, len(uploads))
	start := make(chan struct{})
	for _, u := range uploads {
		wg.Add(1)
		go func(u preparedUpload) {
			defer wg.Done()
			<-start
			attach, err := NewAttachment(1, "a.png", u.buf, u.file)
			assert.NoError(t, err)
			uuids <- attach.UUID
		}(u)
	}
	close(start)
	wg.Wait()
	close(uuids)

	first := ""
	for u := range uuids {
		if first == "" {
			first = u
		}
		assert.Equal(t, first, u)
	}

	count, err := x.Count(new(Attachment))
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.FileExists(t, AttachmentLocalPath(first))
}

func TestNewAttachment_ReplayHealsMissingFile(t *testing.T) {
	newAttachmentTestEngine(t)

	first := uploadTestAttachment(t, 1, "a.png", []byte("payload"))
	require.NoError(t, os.Remove(first.LocalPath()))

	second := uploadTestAttachment(t, 1, "a.png", []byte("payload"))
	assert.Equal(t, first.UUID, second.UUID)
	assert.FileExists(t, first.LocalPath())

	count, err := x.Count(new(Attachment))
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// The staging directory is empty after a successful move.
	entries, err := os.ReadDir(attachmentTempPath())
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestNewAttachment_LinkedUploadIsNotReused(t *testing.T) {
	newAttachmentTestEngine(t)

	content := []byte("linked then uploaded again")
	first := uploadTestAttachment(t, 1, "a.png", content)

	// Simulate the attachment being linked to an issue.
	_, err := x.ID(first.ID).Cols("issue_id").Update(&Attachment{IssueID: 42})
	require.NoError(t, err)

	second := uploadTestAttachment(t, 1, "a.png", content)
	assert.NotEqual(t, first.UUID, second.UUID, "linked attachment must not be reused as a pending upload")
	assert.True(t, second.IssueID == 0 && second.CommentID == 0 && second.ReleaseID == 0)
}

func TestClaimAttachments(t *testing.T) {
	newAttachmentTestEngine(t)

	ownedPending := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, Name: "owned.png"})
	otherUserPending := insertAttachmentWithFile(t, &Attachment{UploaderID: 2, Name: "other.png"})
	issueLinked := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, IssueID: 99, Name: "issue.png"})
	releaseLinked := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, ReleaseID: 7, Name: "release.png"})
	missingFile := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, Name: "missing.png"})
	require.NoError(t, os.Remove(missingFile.LocalPath()))

	claim := func(uploaderID, issueID, commentID int64, uuids []string) int {
		sess := x.NewSession()
		require.NoError(t, sess.Begin())
		n, err := claimAttachments(sess, uploaderID, issueID, commentID, uuids)
		require.NoError(t, err)
		require.NoError(t, sess.Commit())
		return n
	}

	uuids := []string{
		ownedPending.UUID,
		otherUserPending.UUID,
		issueLinked.UUID,
		releaseLinked.UUID,
		missingFile.UUID,
		"00000000-0000-0000-0000-000000000000",
	}
	assert.Equal(t, 1, claim(1, 10, 0, uuids))

	got, err := GetAttachmentByUUID(ownedPending.UUID)
	require.NoError(t, err)
	assert.Equal(t, int64(10), got.IssueID)
	assert.Equal(t, int64(0), got.CommentID)

	// Repeated submission cannot link the same attachment a second time.
	assert.Equal(t, 0, claim(1, 11, 0, []string{ownedPending.UUID}))
	got, err = GetAttachmentByUUID(ownedPending.UUID)
	require.NoError(t, err)
	assert.Equal(t, int64(10), got.IssueID)

	// Other users' pending uploads are claimable only by their uploader.
	assert.Equal(t, 1, claim(2, 10, 5, []string{otherUserPending.UUID}))
	got, err = GetAttachmentByUUID(otherUserPending.UUID)
	require.NoError(t, err)
	assert.Equal(t, int64(10), got.IssueID)
	assert.Equal(t, int64(5), got.CommentID)

	// Linked attachments and missing files remain untouched.
	for _, uuid := range []string{issueLinked.UUID, releaseLinked.UUID, missingFile.UUID} {
		a, err := GetAttachmentByUUID(uuid)
		require.NoError(t, err)
		assert.Equal(t, int64(0), a.CommentID)
	}
	got, err = GetAttachmentByUUID(issueLinked.UUID)
	require.NoError(t, err)
	assert.Equal(t, int64(99), got.IssueID)
	got, err = GetAttachmentByUUID(releaseLinked.UUID)
	require.NoError(t, err)
	assert.Equal(t, int64(7), got.ReleaseID)
}

func TestDeleteExpiredAttachments(t *testing.T) {
	newAttachmentTestEngine(t)

	now := time.Now().Unix()

	expiredPending := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, Name: "expired.png"})
	setAttachmentCreatedUnix(t, expiredPending.ID, now-2*int64(time.Hour/time.Second))

	freshPending := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, Name: "fresh.png"})
	setAttachmentCreatedUnix(t, freshPending.ID, now)

	linked := insertAttachmentWithFile(t, &Attachment{UploaderID: 1, IssueID: 10, Name: "linked.png"})
	setAttachmentCreatedUnix(t, linked.ID, now-2*int64(time.Hour/time.Second))

	otherUserExpired := insertAttachmentWithFile(t, &Attachment{UploaderID: 2, Name: "other.png"})
	setAttachmentCreatedUnix(t, otherUserExpired.ID, now-2*int64(time.Hour/time.Second))

	// A file placed manually into the attachment directory has no database
	// record and must never be removed.
	manualPath := filepath.Join(conf.Attachment.Path, "f", "f", "manually-placed.bin")
	require.NoError(t, os.MkdirAll(filepath.Dir(manualPath), os.ModePerm))
	require.NoError(t, os.WriteFile(manualPath, []byte("manual"), 0o600))

	// Stale staging files left by crashed uploads are swept, fresh ones stay.
	staleTmp := filepath.Join(attachmentTempPath(), "upload-stale.tmp")
	require.NoError(t, os.MkdirAll(attachmentTempPath(), os.ModePerm))
	require.NoError(t, os.WriteFile(staleTmp, []byte("stale"), 0o600))
	require.NoError(t, os.Chtimes(staleTmp, time.Now().Add(-2*time.Hour), time.Now().Add(-2*time.Hour)))
	freshTmp := filepath.Join(attachmentTempPath(), "upload-fresh.tmp")
	require.NoError(t, os.WriteFile(freshTmp, []byte("fresh"), 0o600))

	DeleteExpiredAttachments()

	_, err := GetAttachmentByUUID(expiredPending.UUID)
	assert.True(t, IsErrAttachmentNotExist(err))
	assert.NoFileExists(t, expiredPending.LocalPath())

	_, err = GetAttachmentByUUID(otherUserExpired.UUID)
	assert.True(t, IsErrAttachmentNotExist(err))
	assert.NoFileExists(t, otherUserExpired.LocalPath())

	assert.FileExists(t, freshPending.LocalPath())
	_, err = GetAttachmentByUUID(freshPending.UUID)
	assert.NoError(t, err)

	assert.FileExists(t, linked.LocalPath())
	_, err = GetAttachmentByUUID(linked.UUID)
	assert.NoError(t, err)

	assert.FileExists(t, manualPath)
	assert.NoFileExists(t, staleTmp)
	assert.FileExists(t, freshTmp)
}
