package database

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/core"
	"xorm.io/xorm"

	apiv1types "gogs.io/gogs/internal/route/api/v1/types"
)

// useTestWebhookEngine points the global XORM engine at an isolated on-disk
// SQLite database. The XORM-based webhook layer does not have a store
// abstraction, so its functions are tested through the engine directly.
func useTestWebhookEngine(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	dbPath := filepath.Join(t.TempDir(), "webhook-test.db")
	e, err := xorm.NewEngine("sqlite3", "file:"+dbPath+"?cache=shared&mode=rwc")
	require.NoError(t, err)
	e.SetMapper(core.GonicMapper{})
	require.NoError(t, e.StoreEngine("InnoDB").Sync2(legacyTables...))

	previous := x
	x = e
	t.Cleanup(func() {
		x = previous
		_ = e.Close()
	})
}

func newTestWebhook(t *testing.T, repoID int64, secret string, active bool) *Webhook {
	w := &Webhook{
		RepoID:       repoID,
		URL:          "https://example.com/hooks",
		ContentType:  JSON,
		Secret:       secret,
		HookEvent:    &HookEvent{SendEverything: true},
		IsActive:     active,
		HookTaskType: GOGS,
	}
	require.NoError(t, w.UpdateEvent())
	require.NoError(t, CreateWebhook(w))
	return w
}

func TestRotateWebhookSecret(t *testing.T) {
	useTestWebhookEngine(t)

	w := newTestWebhook(t, 1, "old-secret", false)

	// Start the first rotation, the previous secret is retained with an
	// absolute deadline.
	require.NoError(t, RotateWebhookSecret(w, "new-secret", time.Hour))
	assert.Equal(t, "new-secret", w.Secret)
	assert.Equal(t, "old-secret", w.PreviousSecret)
	assert.InDelta(t, time.Now().Add(time.Hour).Unix(), w.PreviousSecretExpiresUnix, 2)
	firstDeadline := w.PreviousSecretExpiresUnix

	got, err := GetWebhookByID(w.ID)
	require.NoError(t, err)
	assert.Equal(t, "new-secret", got.Secret)
	assert.Equal(t, "old-secret", got.PreviousSecret)
	assert.Equal(t, firstDeadline, got.PreviousSecretExpiresUnix)
	// Other configuration, including the disabled state, stays untouched.
	assert.False(t, got.IsActive)
	assert.Equal(t, "https://example.com/hooks", got.URL)
	assert.Equal(t, JSON, got.ContentType)

	// Replaying the same request after some time, even with a longer grace
	// period, must not extend the deadline or stack another previous secret.
	time.Sleep(1100 * time.Millisecond)
	require.NoError(t, RotateWebhookSecret(got, "new-secret", 2*time.Hour))
	assert.Equal(t, firstDeadline, got.PreviousSecretExpiresUnix)
	assert.Equal(t, "old-secret", got.PreviousSecret)
	assert.Equal(t, "new-secret", got.Secret)

	reloaded, err := GetWebhookByID(w.ID)
	require.NoError(t, err)
	assert.Equal(t, firstDeadline, reloaded.PreviousSecretExpiresUnix)
	assert.Equal(t, "old-secret", reloaded.PreviousSecret)

	// Rotating to a different secret during the grace period is rejected and
	// leaves the state untouched.
	err = RotateWebhookSecret(reloaded, "third-secret", time.Hour)
	require.ErrorIs(t, err, ErrWebhookSecretRotating)
	assert.Equal(t, "new-secret", reloaded.Secret)
	assert.Equal(t, "old-secret", reloaded.PreviousSecret)
	assert.Equal(t, firstDeadline, reloaded.PreviousSecretExpiresUnix)

	// An empty new secret is rejected.
	require.Error(t, RotateWebhookSecret(reloaded, "", time.Hour))

	// Once the deadline elapses, the sweeper purges the previous secret while
	// keeping the current one.
	reloaded.PreviousSecretExpiresUnix = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, UpdateWebhook(reloaded))
	require.NoError(t, FinalizeExpiredWebhookSecretRotations())
	got, err = GetWebhookByID(w.ID)
	require.NoError(t, err)
	assert.Equal(t, "new-secret", got.Secret)
	assert.Empty(t, got.PreviousSecret)
	assert.Zero(t, got.PreviousSecretExpiresUnix)
	assert.False(t, got.IsActive)

	// A follow-up rotation after the grace period keeps the current secret as
	// the single previous secret.
	require.NoError(t, RotateWebhookSecret(got, "newer-secret", time.Hour))
	assert.Equal(t, "newer-secret", got.Secret)
	assert.Equal(t, "new-secret", got.PreviousSecret)
	assert.NotZero(t, got.PreviousSecretExpiresUnix)
}

func hmacHex(secret string, payload []byte) string {
	sig := hmac.New(sha256.New, []byte(secret))
	_, _ = sig.Write(payload)
	return hex.EncodeToString(sig.Sum(nil))
}

func latestHookTask(t *testing.T, hookID int64) *HookTask {
	t.Helper()
	task := &HookTask{}
	has, err := x.Where("hook_id = ?", hookID).Desc("id").Get(task)
	require.NoError(t, err)
	require.True(t, has, "expected a hook task")
	return task
}

func TestWebhookSignaturesDuringRotation(t *testing.T) {
	useTestWebhookEngine(t)

	payload := &apiv1types.WebhookCreatePayload{
		Ref:           "refs/heads/main",
		RefType:       "branch",
		Sha:           "9b5c9c01a9b14c73af0a46a9b6c14d3e2f9a8b7c",
		DefaultBranch: "main",
	}
	payloadBytes, err := payload.JSONPayload()
	require.NoError(t, err)

	repo := &Repository{ID: 100}

	t.Run("both secrets match during grace period", func(t *testing.T) {
		w := newTestWebhook(t, 100, "old-secret", true)
		require.NoError(t, RotateWebhookSecret(w, "new-secret", time.Hour))

		require.NoError(t, prepareHookTasks(x, repo, HookEventTypeCreate, payload, []*Webhook{w}))
		task := latestHookTask(t, w.ID)

		assert.Equal(t, hmacHex("new-secret", payloadBytes), task.Signature)
		assert.Equal(t, hmacHex("old-secret", payloadBytes), task.PreviousSignature)
		assert.NotEmpty(t, task.Signature)
		assert.NotEqual(t, task.Signature, task.PreviousSignature)
	})

	t.Run("only new secret matches after grace period", func(t *testing.T) {
		w := newTestWebhook(t, 101, "old-secret", true)
		require.NoError(t, RotateWebhookSecret(w, "new-secret", time.Hour))
		w.PreviousSecretExpiresUnix = time.Now().Add(-time.Minute).Unix()
		require.NoError(t, UpdateWebhook(w))

		// Simulate a restart by using the state as loaded from the database.
		fresh, err := GetWebhookByID(w.ID)
		require.NoError(t, err)

		require.NoError(t, prepareHookTasks(x, repo, HookEventTypeCreate, payload, []*Webhook{fresh}))
		task := latestHookTask(t, w.ID)

		assert.Equal(t, hmacHex("new-secret", payloadBytes), task.Signature)
		assert.Empty(t, task.PreviousSignature)

		// Lazy finalization purged the previous secret in the database.
		finalized, err := GetWebhookByID(w.ID)
		require.NoError(t, err)
		assert.Equal(t, "new-secret", finalized.Secret)
		assert.Empty(t, finalized.PreviousSecret)
		assert.Zero(t, finalized.PreviousSecretExpiresUnix)
	})

	t.Run("no previous signature without a rotation", func(t *testing.T) {
		w := newTestWebhook(t, 102, "only-secret", true)
		require.NoError(t, prepareHookTasks(x, repo, HookEventTypeCreate, payload, []*Webhook{w}))
		task := latestHookTask(t, w.ID)

		assert.Equal(t, hmacHex("only-secret", payloadBytes), task.Signature)
		assert.Empty(t, task.PreviousSignature)
	})
}
