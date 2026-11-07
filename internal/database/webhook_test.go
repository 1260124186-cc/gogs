package database

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/core"
	"xorm.io/xorm"

	"gogs.io/gogs/internal/conf"
)

// fakeDrainer simulates per-webhook drain batches backed by an in-memory
// pending counter.
type fakeDrainer struct {
	mu        sync.Mutex
	pending   map[int64]int
	active    map[int64]int
	maxAct    map[int64]int
	blocks    map[int64]chan struct{}
	blockedOn map[int64]bool
}

func newFakeDrainer() *fakeDrainer {
	return &fakeDrainer{
		pending:   make(map[int64]int),
		active:    make(map[int64]int),
		maxAct:    make(map[int64]int),
		blocks:    make(map[int64]chan struct{}),
		blockedOn: make(map[int64]bool),
	}
}

func (f *fakeDrainer) drain(hookID int64) bool {
	f.mu.Lock()
	if release := f.blocks[hookID]; release != nil {
		f.blockedOn[hookID] = true
		f.mu.Unlock()
		<-release
		f.mu.Lock()
	}

	f.active[hookID]++
	if f.active[hookID] > f.maxAct[hookID] {
		f.maxAct[hookID] = f.active[hookID]
	}
	n := f.pending[hookID]
	if n > 0 {
		f.pending[hookID] = n - 1
	}
	f.mu.Unlock()

	time.Sleep(time.Millisecond)

	f.mu.Lock()
	f.active[hookID]--
	f.mu.Unlock()
	return n > 0
}

func (f *fakeDrainer) pendingOf(hookID int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pending[hookID]
}

func (f *fakeDrainer) hasPending(hookID int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pending[hookID] > 0
}

func (f *fakeDrainer) maxActiveOf(hookID int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxAct[hookID]
}

func (f *fakeDrainer) waitUntil(t *testing.T, hookID int64, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		ok := cond()
		f.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition on webhook %d", hookID)
}

func TestHookDeliveryQueue_CoalescesSignalsAndRearms(t *testing.T) {
	f := newFakeDrainer()
	q := newHookDeliveryQueue(2, f.drain, f.hasPending)
	f.pending[1] = 3

	// Repeated signals while the drainer is active must not start a second
	// drainer or lose pending tasks.
	for i := 0; i < 5; i++ {
		q.add(1)
	}
	q.wait()

	assert.Equal(t, 0, f.pendingOf(1), "all pending tasks must be drained")
	assert.Equal(t, 1, f.maxActiveOf(1), "the webhook must never drain concurrently")

	// A new signal after the drainer stopped must start it again.
	f.pending[1] = 2
	q.add(1)
	q.add(1)
	q.wait()
	assert.Equal(t, 0, f.pendingOf(1))
	assert.Equal(t, 1, f.maxActiveOf(1))
}

func TestHookDeliveryQueue_DifferentWebhooksDoNotBlock(t *testing.T) {
	f := newFakeDrainer()
	q := newHookDeliveryQueue(2, f.drain, f.hasPending)

	release := make(chan struct{})
	f.blocks[1] = release
	f.pending[1] = 1
	f.pending[2] = 1

	q.add(1)
	f.waitUntil(t, 1, func() bool { return f.blockedOn[1] })

	q.add(2)
	f.waitUntil(t, 2, func() bool { return f.pending[2] == 0 })

	assert.Equal(t, 1, f.pendingOf(1), "webhook 1 must still be stuck on its first blocked task")
	assert.Equal(t, 0, f.pendingOf(2), "webhook 2 must be delivered while webhook 1 is blocked")

	close(release)
	q.wait()
	assert.Equal(t, 1, f.maxActiveOf(1))
	assert.Equal(t, 1, f.maxActiveOf(2))
}

// TestHookDeliveryQueue_SignalDuringShutdownIsNotLost covers the race where a
// signal lands after the last empty drain but before the running marker is
// cleared: the locked re-check must observe the new pending task.
func TestHookDeliveryQueue_SignalDuringShutdownIsNotLost(t *testing.T) {
	base := newFakeDrainer()
	base.pending[1] = 1

	var parked atomic.Bool
	release := make(chan struct{})
	drain := func(hookID int64) bool {
		// Park on the first call that observes an empty queue, which is the
		// last drain before the shutdown re-check.
		if base.pendingOf(hookID) == 0 && parked.CompareAndSwap(false, true) {
			<-release
		}
		return base.drain(hookID)
	}
	q := newHookDeliveryQueue(2, drain, base.hasPending)
	q.add(1)

	deadline := time.Now().Add(2 * time.Second)
	for !parked.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, parked.Load(), "drainer must park on its final empty drain")

	// The signal is coalesced while the drainer is shutting down, and the
	// matching task row becomes committed before the drainer resumes.
	q.add(1)
	base.mu.Lock()
	base.pending[1] = 1
	base.mu.Unlock()
	close(release)

	q.wait()
	assert.Equal(t, 0, base.pendingOf(1), "the late signal must not be lost")
}

// recordingHandler records the order of delivery UUIDs and can gate or delay
// responses.
type recordingHandler struct {
	mu      sync.Mutex
	uuids   []string
	status  int
	started chan struct{}
	release chan struct{}
	delay   time.Duration
	once    sync.Once
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.release != nil {
		h.once.Do(func() { close(h.started) })
		<-h.release
	}
	if h.delay > 0 {
		time.Sleep(h.delay)
	}

	h.mu.Lock()
	h.uuids = append(h.uuids, r.Header.Get("X-Gogs-Delivery"))
	h.mu.Unlock()

	status := h.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte("ok"))
}

func (h *recordingHandler) deliveryOrder() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.uuids))
	copy(out, h.uuids)
	return out
}

func (h *recordingHandler) deliveryCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.uuids)
}

// setupHookDeliveryTest points the package-wide xorm engine at a fresh SQLite
// database and allows deliveries to loopback test servers.
func setupHookDeliveryTest(t *testing.T) *hookDeliveryQueue {
	t.Helper()
	const concurrency = 2
	dbFile := filepath.Join(t.TempDir(), "hooks.db")
	engine, err := xorm.NewEngine("sqlite3", "file:"+dbFile+"?cache=shared&mode=rwc")
	require.NoError(t, err)
	engine.SetMapper(core.GonicMapper{})
	require.NoError(t, engine.Sync2(new(Webhook), new(HookTask)))

	prevEngine := x
	x = engine
	t.Cleanup(func() {
		_ = engine.Close()
		x = prevEngine
	})

	prevAllowlist := conf.Security.LocalNetworkAllowlist
	prevTimeout := conf.Webhook.DeliverTimeout
	conf.Security.LocalNetworkAllowlist = []string{"127.0.0.1", "localhost"}
	conf.Webhook.DeliverTimeout = 5
	t.Cleanup(func() {
		conf.Security.LocalNetworkAllowlist = prevAllowlist
		conf.Webhook.DeliverTimeout = prevTimeout
	})

	return newHookDeliveryQueue(concurrency, drainPendingHookTasks, hasPendingHookTask)
}

func createTestWebhook(t *testing.T, hookID int64, url string) {
	t.Helper()
	_, err := x.Insert(&Webhook{
		ID:           hookID,
		URL:          url,
		Events:       "{}",
		IsActive:     true,
		HookTaskType: GOGS,
		ContentType:  JSON,
	})
	require.NoError(t, err)
}

func createTestHookTask(t *testing.T, hookID int64, uuidStr, url string) *HookTask {
	t.Helper()
	task := &HookTask{
		RepoID:         1,
		HookID:         hookID,
		UUID:           uuidStr,
		Type:           GOGS,
		URL:            url,
		ContentType:    JSON,
		EventType:      HookEventTypePush,
		PayloadContent: `{}`,
	}
	_, err := x.Insert(task)
	require.NoError(t, err)
	return task
}

func getTestHookTask(t *testing.T, id int64) *HookTask {
	t.Helper()
	task := &HookTask{ID: id}
	has, err := x.Get(task)
	require.NoError(t, err)
	require.True(t, has)
	return task
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for: " + msg)
}

func TestDrainPendingHookTasks_DeliversInIDOrder(t *testing.T) {
	q := setupHookDeliveryTest(t)

	h1 := &recordingHandler{}
	srv1 := httptest.NewServer(h1)
	t.Cleanup(srv1.Close)
	h2 := &recordingHandler{}
	srv2 := httptest.NewServer(h2)
	t.Cleanup(srv2.Close)

	createTestWebhook(t, 1, srv1.URL)
	createTestWebhook(t, 2, srv2.URL)

	// Insert in non-sequential hook order; per-hook ID order must still hold.
	uuids1 := []string{"u1-1", "u1-2", "u1-3"}
	uuids2 := []string{"u2-1", "u2-2"}
	createTestHookTask(t, 1, uuids1[0], srv1.URL)
	createTestHookTask(t, 2, uuids2[0], srv2.URL)
	createTestHookTask(t, 1, uuids1[1], srv1.URL)
	createTestHookTask(t, 2, uuids2[1], srv2.URL)
	createTestHookTask(t, 1, uuids1[2], srv1.URL)

	// No in-memory signals exist, as if the process restarted.
	enqueuePendingHookTasks(q)
	q.wait()

	assert.Equal(t, uuids1, h1.deliveryOrder())
	assert.Equal(t, uuids2, h2.deliveryOrder())

	var tasks []*HookTask
	require.NoError(t, x.Find(&tasks))
	require.Len(t, tasks, 5)
	for _, task := range tasks {
		assert.True(t, task.IsDelivered, "task %s must be marked delivered", task.UUID)
		assert.True(t, task.IsSucceed, "task %s must succeed", task.UUID)
		assert.NotZero(t, task.Delivered)
	}
}

func TestDrainPendingHookTasks_FailuresAreDefiniteAndDoNotBlockNext(t *testing.T) {
	q := setupHookDeliveryTest(t)

	h200 := &recordingHandler{status: http.StatusOK}
	srv200 := httptest.NewServer(h200)
	t.Cleanup(srv200.Close)
	h500 := &recordingHandler{status: http.StatusInternalServerError}
	srv500 := httptest.NewServer(h500)
	t.Cleanup(srv500.Close)

	createTestWebhook(t, 1, srv200.URL)

	invalid := createTestHookTask(t, 1, "invalid", "://")
	blocked := createTestHookTask(t, 1, "blocked", "http://169.254.1.1/")
	failed := createTestHookTask(t, 1, "failed", srv500.URL)
	succeeded := createTestHookTask(t, 1, "succeeded", srv200.URL)

	// A separate webhook ends in a failure to assert the last status update.
	failingHook := &recordingHandler{status: http.StatusInternalServerError}
	failingSrv := httptest.NewServer(failingHook)
	t.Cleanup(failingSrv.Close)
	createTestWebhook(t, 2, failingSrv.URL)
	failingTask := createTestHookTask(t, 2, "failing-only", failingSrv.URL)

	q.add(1)
	q.add(2)
	q.wait()

	// Only the two reachable tasks resulted in HTTP requests.
	assert.Equal(t, []string{"failed"}, h500.deliveryOrder())
	assert.Equal(t, []string{"succeeded"}, h200.deliveryOrder())

	for _, id := range []int64{invalid.ID, blocked.ID, failed.ID} {
		task := getTestHookTask(t, id)
		assert.True(t, task.IsDelivered, "task %d must have a definite result", id)
		assert.False(t, task.IsSucceed, "task %d must be marked failed", id)
		require.NotNil(t, task.ResponseInfo)
		assert.NotEmpty(t, task.ResponseInfo.Body, "task %d must record the failure reason", id)
	}

	succeededTask := getTestHookTask(t, succeeded.ID)
	assert.True(t, succeededTask.IsDelivered)
	assert.True(t, succeededTask.IsSucceed)

	failingDelivered := getTestHookTask(t, failingTask.ID)
	assert.True(t, failingDelivered.IsDelivered)
	assert.False(t, failingDelivered.IsSucceed)

	w1, err := GetWebhookByID(1)
	require.NoError(t, err)
	assert.EqualValues(t, HookStatusSucceed, w1.LastStatus)
	w2, err := GetWebhookByID(2)
	require.NoError(t, err)
	assert.EqualValues(t, HookStatusFailed, w2.LastStatus)
}

func TestDrainPendingHookTasks_TimeoutIsDefiniteAndContinues(t *testing.T) {
	q := setupHookDeliveryTest(t)
	conf.Webhook.DeliverTimeout = 1

	slow := &recordingHandler{delay: 2 * time.Second}
	slowSrv := httptest.NewServer(slow)
	t.Cleanup(slowSrv.Close)

	fast := &recordingHandler{}
	// Both tasks share the target host, but the slow one hangs before
	// responding; point the second task at a different server to emulate the
	// next event arriving after the timed-out one.
	fastSrv := httptest.NewServer(fast)
	t.Cleanup(fastSrv.Close)

	createTestWebhook(t, 1, slowSrv.URL)
	timedOut := createTestHookTask(t, 1, "timed-out", slowSrv.URL)
	after := createTestHookTask(t, 1, "after-timeout", fastSrv.URL)

	start := time.Now()
	q.add(1)
	q.wait()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 4*time.Second, "delivery must not wait far beyond the timeout")
	assert.Equal(t, []string{"after-timeout"}, fast.deliveryOrder(), "the next task must proceed after timeout")
	assert.Len(t, slow.deliveryOrder(), 0, "the slow server never produced a response")

	timedOutTask := getTestHookTask(t, timedOut.ID)
	assert.True(t, timedOutTask.IsDelivered)
	assert.False(t, timedOutTask.IsSucceed)
	require.NotNil(t, timedOutTask.ResponseInfo)
	assert.Contains(t, timedOutTask.ResponseInfo.Body, "Delivery:")

	afterTask := getTestHookTask(t, after.ID)
	assert.True(t, afterTask.IsDelivered)
	assert.True(t, afterTask.IsSucceed)
}

func TestDrainPendingHookTasks_DifferentWebhooksDontBlock(t *testing.T) {
	q := setupHookDeliveryTest(t)

	release := make(chan struct{})
	blocked := &recordingHandler{started: make(chan struct{}), release: release}
	blockedSrv := httptest.NewServer(blocked)
	t.Cleanup(blockedSrv.Close)

	fast := &recordingHandler{}
	fastSrv := httptest.NewServer(fast)
	t.Cleanup(fastSrv.Close)

	createTestWebhook(t, 1, blockedSrv.URL)
	createTestWebhook(t, 2, fastSrv.URL)
	blockedTask := createTestHookTask(t, 1, "blocked", blockedSrv.URL)
	fastTask := createTestHookTask(t, 2, "fast", fastSrv.URL)

	q.add(1)
	<-blocked.started
	q.add(2)

	waitFor(t, func() bool {
		task := getTestHookTask(t, fastTask.ID)
		return task.IsDelivered && task.IsSucceed
	}, "webhook 2 delivered while webhook 1 is in flight")

	blockedDelivered := getTestHookTask(t, blockedTask.ID)
	assert.False(t, blockedDelivered.IsDelivered, "webhook 1 must still be in flight")

	close(release)
	q.wait()

	assert.Equal(t, []string{"blocked"}, blocked.deliveryOrder())
	assert.Equal(t, []string{"fast"}, fast.deliveryOrder())
}

func TestHookDelivery_RestartAndDuplicateSignalsDoNotRequeueCompleted(t *testing.T) {
	q := setupHookDeliveryTest(t)

	h := &recordingHandler{}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	createTestWebhook(t, 1, srv.URL)
	tasks := make([]*HookTask, 3)
	for i := range tasks {
		uuidStr := fmt.Sprintf("u%d", i+1)
		tasks[i] = createTestHookTask(t, 1, uuidStr, srv.URL)
	}

	q.add(1)
	q.wait()
	require.Equal(t, []string{"u1", "u2", "u3"}, h.deliveryOrder())

	// Repeated triggers after completion must not deliver anything again.
	enqueuePendingHookTasks(q)
	q.wait()
	q.add(1)
	q.add(1)
	q.wait()
	assert.Equal(t, 3, h.deliveryCount())

	hookIDs, err := pendingHookIDs("repo_id = ?", int64(1))
	require.NoError(t, err)
	assert.Empty(t, hookIDs)

	// Simulate a fresh process recovering from a restart.
	q2 := newHookDeliveryQueue(2, drainPendingHookTasks, hasPendingHookTask)
	enqueuePendingHookTasks(q2)
	q2.wait()
	assert.Equal(t, 3, h.deliveryCount(), "a restart must not requeue completed tasks")

	// Manual redelivery marks a task pending again and must be picked up.
	redelivered := getTestHookTask(t, tasks[1].ID)
	redelivered.IsDelivered = false
	require.NoError(t, UpdateHookTask(redelivered))
	q2.add(1)
	q2.wait()
	assert.Equal(t, []string{"u1", "u2", "u3", "u2"}, h.deliveryOrder())
}

func TestPendingHookIDs_FiltersByStateAndRepo(t *testing.T) {
	setupHookDeliveryTest(t)

	h := &recordingHandler{}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	createTestWebhook(t, 1, srv.URL)
	createTestWebhook(t, 2, srv.URL)

	t1 := createTestHookTask(t, 1, "a", srv.URL) // delivered
	t2 := createTestHookTask(t, 1, "b", srv.URL) // pending
	t3 := createTestHookTask(t, 2, "c", srv.URL) // pending, other hook
	t1.IsDelivered = true
	require.NoError(t, UpdateHookTask(t1))
	t2.RepoID = 10
	require.NoError(t, UpdateHookTask(t2))
	t3.RepoID = 20
	require.NoError(t, UpdateHookTask(t3))

	all, err := pendingHookIDs("")
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, all)

	repoScoped, err := pendingHookIDs("repo_id = ?", int64(10))
	require.NoError(t, err)
	assert.Equal(t, []int64{1}, repoScoped)
}
