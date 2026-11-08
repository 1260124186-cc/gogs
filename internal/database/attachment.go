package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	log "unknwon.dev/clog/v2"
	"xorm.io/xorm"

	"gogs.io/gogs/internal/conf"
	"gogs.io/gogs/internal/errx"
	"gogs.io/gogs/internal/osx"
	"gogs.io/gogs/internal/sync"
)

// Attachment represent a attachment of issue/comment/release.
type Attachment struct {
	ID         int64
	UUID       string `xorm:"uuid UNIQUE"`
	UploaderID int64  `xorm:"INDEX"`
	IssueID    int64  `xorm:"INDEX"`
	CommentID  int64
	ReleaseID  int64 `xorm:"INDEX"`
	Name       string
	// Hash is the SHA256 hex digest of the file content. It deduplicates
	// replayed uploads from the same user while the attachment is still
	// pending.
	Hash string

	Created     time.Time `xorm:"-" json:"-" gorm:"-"`
	CreatedUnix int64
}

func (a *Attachment) BeforeInsert() {
	a.CreatedUnix = time.Now().Unix()
}

func (a *Attachment) AfterSet(colName string, _ xorm.Cell) {
	switch colName {
	case "created_unix":
		a.Created = time.Unix(a.CreatedUnix, 0).Local()
	}
}

// isLinked reports whether the attachment has been associated with an issue,
// comment, or release.
func (a *Attachment) isLinked() bool {
	return a.IssueID != 0 || a.CommentID != 0 || a.ReleaseID != 0
}

// AttachmentLocalPath returns where attachment is stored in local file system based on given UUID.
func AttachmentLocalPath(uuid string) string {
	return path.Join(conf.Attachment.Path, uuid[0:1], uuid[1:2], uuid)
}

// LocalPath returns where attachment is stored in local file system.
func (a *Attachment) LocalPath() string {
	return AttachmentLocalPath(a.UUID)
}

// attachmentTempPath is where in-progress uploads are staged before being
// atomically moved into the sharded attachment layout. The directory is fully
// managed by Gogs, so the cleanup task can remove stale temp files without
// touching anything placed into the attachment directory manually.
func attachmentTempPath() string {
	return filepath.Join(conf.Attachment.Path, "tmp")
}

// writeTempAttachment streams the upload into the temp directory and returns
// its path together with the SHA256 hex digest of the full content.
func writeTempAttachment(buf []byte, file multipart.File) (tmpPath, hash string, err error) {
	if err = os.MkdirAll(attachmentTempPath(), os.ModePerm); err != nil {
		return "", "", errors.Newf("MkdirAll temp dir: %v", err)
	}

	fw, err := os.CreateTemp(attachmentTempPath(), "upload-*.tmp")
	if err != nil {
		return "", "", errors.Newf("create temp file: %v", err)
	}
	defer fw.Close()

	h := sha256.New()
	if _, err = io.MultiWriter(fw, h).Write(buf); err != nil {
		_ = os.Remove(fw.Name())
		return "", "", errors.Newf("write: %v", err)
	} else if _, err = io.Copy(io.MultiWriter(fw, h), file); err != nil {
		_ = os.Remove(fw.Name())
		return "", "", errors.Newf("copy: %v", err)
	} else if err = fw.Sync(); err != nil {
		_ = os.Remove(fw.Name())
		return "", "", errors.Newf("sync: %v", err)
	}

	return fw.Name(), hex.EncodeToString(h.Sum(nil)), nil
}

// attachmentUploadPool serializes uploads of the same content from the same
// user, so retries cannot race each other into creating duplicate records.
var attachmentUploadPool = sync.NewExclusivePool()

// NewAttachment stores the uploaded file in a pending state. The attachment
// only takes effect after being claimed by an issue, comment, or release of the
// same uploader (see claimAttachments). Replaying the same upload (duplicated
// submit, request retry, or server restart) returns the existing pending
// attachment instead of creating another file.
func NewAttachment(uploaderID int64, name string, buf []byte, file multipart.File) (_ *Attachment, err error) {
	tmpPath, hash, err := writeTempAttachment(buf, file)
	if err != nil {
		return nil, err
	}

	lockKey := fmt.Sprintf("%d:%s", uploaderID, hash)
	attachmentUploadPool.CheckIn(lockKey)
	defer attachmentUploadPool.CheckOut(lockKey)

	attach := new(Attachment)
	has, err := x.Where("uploader_id = ? AND hash = ? AND name = ? AND issue_id = 0 AND comment_id = 0 AND release_id = 0",
		uploaderID, hash, name).Get(attach)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, errors.Newf("get pending attachment: %v", err)
	}

	if has {
		// The file can be missing when the previous attempt stopped between
		// inserting the record and moving the file into place.
		if !osx.IsFile(attach.LocalPath()) {
			if err = os.MkdirAll(path.Dir(attach.LocalPath()), os.ModePerm); err != nil {
				return nil, errors.Newf("MkdirAll: %v", err)
			} else if err = os.Rename(tmpPath, attach.LocalPath()); err != nil {
				return nil, errors.Newf("rename: %v", err)
			}
		} else {
			_ = os.Remove(tmpPath)
		}
		return attach, nil
	}

	attach = &Attachment{
		UUID:       uuid.New().String(),
		UploaderID: uploaderID,
		Name:       name,
		Hash:       hash,
	}
	// Insert the record before moving the file, so a crash in between is
	// healed by the next replay instead of producing a duplicate file.
	if _, err = x.Insert(attach); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}

	if err = os.MkdirAll(path.Dir(attach.LocalPath()), os.ModePerm); err != nil {
		_, _ = x.Delete(attach)
		_ = os.Remove(tmpPath)
		return nil, errors.Newf("MkdirAll: %v", err)
	} else if err = os.Rename(tmpPath, attach.LocalPath()); err != nil {
		_, _ = x.Delete(attach)
		_ = os.Remove(tmpPath)
		return nil, errors.Newf("rename: %v", err)
	}

	return attach, nil
}

var _ errx.NotFound = (*ErrAttachmentNotExist)(nil)

type ErrAttachmentNotExist struct {
	args map[string]any
}

func IsErrAttachmentNotExist(err error) bool {
	_, ok := err.(ErrAttachmentNotExist)
	return ok
}

func (err ErrAttachmentNotExist) Error() string {
	return fmt.Sprintf("attachment does not exist: %v", err.args)
}

func (ErrAttachmentNotExist) NotFound() bool {
	return true
}

func getAttachmentByUUID(e Engine, uuid string) (*Attachment, error) {
	attach := &Attachment{UUID: uuid}
	has, err := e.Get(attach)
	if err != nil {
		return nil, err
	} else if !has {
		return nil, ErrAttachmentNotExist{args: map[string]any{"uuid": uuid}}
	}
	return attach, nil
}

// GetAttachmentByUUID returns attachment by given UUID.
func GetAttachmentByUUID(uuid string) (*Attachment, error) {
	return getAttachmentByUUID(x, uuid)
}

func getAttachmentsByIssueID(e Engine, issueID int64) ([]*Attachment, error) {
	attachments := make([]*Attachment, 0, 5)
	return attachments, e.Where("issue_id = ? AND comment_id = 0", issueID).Find(&attachments)
}

// GetAttachmentsByIssueID returns all attachments of an issue.
func GetAttachmentsByIssueID(issueID int64) ([]*Attachment, error) {
	return getAttachmentsByIssueID(x, issueID)
}

func getAttachmentsByCommentID(e Engine, commentID int64) ([]*Attachment, error) {
	attachments := make([]*Attachment, 0, 5)
	return attachments, e.Where("comment_id=?", commentID).Find(&attachments)
}

// GetAttachmentsByCommentID returns all attachments of a comment.
func GetAttachmentsByCommentID(commentID int64) ([]*Attachment, error) {
	return getAttachmentsByCommentID(x, commentID)
}

func getAttachmentsByReleaseID(e Engine, releaseID int64) ([]*Attachment, error) {
	attachments := make([]*Attachment, 0, 10)
	return attachments, e.Where("release_id = ?", releaseID).Find(&attachments)
}

// GetAttachmentsByReleaseID returns all attachments of a release.
func GetAttachmentsByReleaseID(releaseID int64) ([]*Attachment, error) {
	return getAttachmentsByReleaseID(x, releaseID)
}

// claimAttachments links pending attachments owned by uploaderID to the given
// issue and comment. Unknown UUIDs, attachments owned by other users, and
// attachments already linked elsewhere are silently skipped. The conditional
// update makes repeated submissions unable to link the same attachment twice.
func claimAttachments(e *xorm.Session, uploaderID, issueID, commentID int64, uuids []string) (int, error) {
	if len(uuids) == 0 {
		return 0, nil
	}

	candidates := make([]*Attachment, 0, len(uuids))
	if err := e.In("uuid", uuids).Find(&candidates); err != nil {
		return 0, errors.Newf("find attachments [uuids: %v]: %v", uuids, err)
	}

	claimed := 0
	for _, a := range candidates {
		if a.UploaderID != uploaderID || a.isLinked() {
			continue
		} else if !osx.IsFile(a.LocalPath()) {
			// Never link to a file that is not on disk, the cleanup task
			// reclaims the expired record.
			continue
		}

		affected, err := e.ID(a.ID).
			Where("issue_id = 0 AND comment_id = 0 AND release_id = 0 AND uploader_id = ?", uploaderID).
			Cols("issue_id", "comment_id").
			Update(&Attachment{IssueID: issueID, CommentID: commentID})
		if err != nil {
			return claimed, errors.Newf("update attachment [id: %d]: %v", a.ID, err)
		} else if affected == 1 {
			claimed++
		}
	}
	return claimed, nil
}

// DeleteAttachment deletes the given attachment and optionally the associated file.
func DeleteAttachment(a *Attachment, remove bool) error {
	_, err := DeleteAttachments([]*Attachment{a}, remove)
	return err
}

// DeleteAttachments deletes the given attachments and optionally the associated files.
func DeleteAttachments(attachments []*Attachment, remove bool) (int, error) {
	for i, a := range attachments {
		if remove {
			if err := os.Remove(a.LocalPath()); err != nil {
				return i, err
			}
		}

		if _, err := x.Delete(a); err != nil {
			return i, err
		}
	}

	return len(attachments), nil
}

// DeleteAttachmentsByIssue deletes all attachments associated with the given issue.
func DeleteAttachmentsByIssue(issueID int64, remove bool) (int, error) {
	attachments, err := GetAttachmentsByIssueID(issueID)
	if err != nil {
		return 0, err
	}

	return DeleteAttachments(attachments, remove)
}

// DeleteAttachmentsByComment deletes all attachments associated with the given comment.
func DeleteAttachmentsByComment(commentID int64, remove bool) (int, error) {
	attachments, err := GetAttachmentsByCommentID(commentID)
	if err != nil {
		return 0, err
	}

	return DeleteAttachments(attachments, remove)
}

// DeleteExpiredAttachments deletes pending attachments that have never been
// linked to an issue, comment, or release and are older than the configured
// retention. Deletion is driven by database records, so files placed into the
// attachment directory manually and files of linked attachments are never
// removed.
func DeleteExpiredAttachments() {
	if taskStatusTable.IsRunning(taskNameCleanExpiredAttachments) {
		return
	}
	taskStatusTable.Start(taskNameCleanExpiredAttachments)
	defer taskStatusTable.Stop(taskNameCleanExpiredAttachments)

	log.Trace("Doing: DeleteExpiredAttachments")

	cutoff := time.Now().Add(-conf.Cron.AttachmentCleanup.OlderThan)
	cutoffUnix := cutoff.Unix()

	// Snapshot the candidates first, deleting rows while iterating the result
	// set is not supported by some databases (e.g. SQLite).
	expired := make([]*Attachment, 0, 10)
	if err := x.Where("issue_id = 0 AND comment_id = 0 AND release_id = 0 AND created_unix < ?", cutoffUnix).
		Find(&expired); err != nil {
		log.Error("DeleteExpiredAttachments: %v", err)
		return
	}

	for _, a := range expired {
		// Delete the record with the pending conditions first. If the
		// attachment gets linked concurrently, zero rows are affected and the
		// file must be kept.
		affected, err := x.ID(a.ID).
			Where("issue_id = 0 AND comment_id = 0 AND release_id = 0 AND created_unix < ?", cutoffUnix).
			Delete(new(Attachment))
		if err != nil {
			log.Error("DeleteExpiredAttachments: delete attachment [id: %d]: %v", a.ID, err)
			continue
		} else if affected == 0 {
			continue
		}

		if err = os.Remove(a.LocalPath()); err != nil && !os.IsNotExist(err) {
			const fmtStr = "Failed to delete expired attachment %q: %v"
			log.Warn(fmtStr, a.LocalPath(), err)
			if err = Handle.Notices().Create(
				context.TODO(),
				NoticeTypeRepository,
				fmt.Sprintf(fmtStr, a.LocalPath(), err),
			); err != nil {
				log.Error("Failed to create notice: %v", err)
			}
			continue
		}

		removeEmptyAttachmentDirs(a.LocalPath())
	}

	deleteStaleTempAttachments(cutoff)
}

// removeEmptyAttachmentDirs removes the shard directories of the attachment if
// they became empty. It never removes directories that still contain files.
func removeEmptyAttachmentDirs(localPath string) {
	dir := path.Dir(localPath)
	if err := os.Remove(dir); err == nil {
		_ = os.Remove(path.Dir(dir))
	}
}

// deleteStaleTempAttachments removes abandoned staging files left by crashed or
// aborted uploads. Only the Gogs-managed temp directory is scanned.
func deleteStaleTempAttachments(cutoff time.Time) {
	entries, err := os.ReadDir(attachmentTempPath())
	if err != nil {
		if !os.IsNotExist(err) {
			log.Error("Failed to read attachment temp directory: %v", err)
		}
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		} else if info.ModTime().After(cutoff) {
			continue
		}

		tmpPath := filepath.Join(attachmentTempPath(), entry.Name())
		if err = os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
			log.Warn("Failed to remove stale temp attachment %q: %v", tmpPath, err)
		}
	}
}
