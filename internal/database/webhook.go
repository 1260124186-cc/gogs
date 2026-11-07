package database

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	log "unknwon.dev/clog/v2"
	"xorm.io/xorm"

	"gogs.io/gogs/internal/conf"
	"gogs.io/gogs/internal/errx"
	"gogs.io/gogs/internal/httplib"
	"gogs.io/gogs/internal/netx"
	apiv1types "gogs.io/gogs/internal/route/api/v1/types"
	"gogs.io/gogs/internal/testx"
)

// defaultHookDeliverConcurrency caps how many webhooks are delivered at the
// same time when [conf.Webhook.DeliverConcurrency] is not configured.
const defaultHookDeliverConcurrency = 10

// hookTaskUpdateMaxRetries is the number of attempts to persist a delivery
// result. A result that is not persisted would make the task eligible for
// redelivery, which could arrive at the target out of order.
const hookTaskUpdateMaxRetries = 3

// hookTaskUpdateRetryInterval is the wait between persistence attempts.
const hookTaskUpdateRetryInterval = 200 * time.Millisecond

// hookDrainBackoff is how long a drainer waits before retrying after a
// database error, to avoid spinning on a failing database.
const hookDrainBackoff = time.Second

// hookDeliveryQueue guarantees that at most one drainer runs per webhook.
// The drainer keeps processing pending tasks until the webhook has none left,
// so repeated signals while it is active are coalesced without losing work.
// Different webhooks acquire a shared concurrency semaphore independently,
// thus deliveries of one webhook never block another, except when the
// concurrency cap is reached.
type hookDeliveryQueue struct {
	mu      sync.Mutex
	running map[int64]struct{}
	sem     chan struct{}
	// drain processes one batch of pending tasks for the webhook and returns
	// true when the caller should drain again because more tasks were found.
	drain func(hookID int64) (keepGoing bool)
	// hasPending reports whether the webhook still has any pending task. It
	// must be cheap, the queue calls it while holding its lock.
	hasPending func(hookID int64) bool
	wg         sync.WaitGroup
}

func newHookDeliveryQueue(concurrency int, drain func(int64) bool, hasPending func(int64) bool) *hookDeliveryQueue {
	if concurrency <= 0 {
		concurrency = defaultHookDeliverConcurrency
	}
	return &hookDeliveryQueue{
		running:    make(map[int64]struct{}),
		sem:        make(chan struct{}, concurrency),
		drain:      drain,
		hasPending: hasPending,
	}
}

// add ensures a drainer is running for the webhook. Signals are coalesced
// while a drainer is active.
//
// Callers must only signal after the task rows are committed to the database,
// otherwise a drainer may finish its final emptiness check before the rows
// become visible.
func (q *hookDeliveryQueue) add(hookID int64) {
	q.mu.Lock()
	if _, ok := q.running[hookID]; ok {
		q.mu.Unlock()
		return
	}
	q.running[hookID] = struct{}{}
	q.mu.Unlock()

	q.wg.Add(1)
	go q.runDrainer(hookID)
}

func (q *hookDeliveryQueue) runDrainer(hookID int64) {
	defer q.wg.Done()

	q.sem <- struct{}{}
	defer func() { <-q.sem }()

	for {
		if !q.drain(hookID) {
			q.mu.Lock()
			// Re-check with the lock held so an add() cannot interleave
			// between the last empty result and clearing the running marker.
			// Task rows are always committed before add() is called, so a
			// signal coalesced here guarantees its rows are visible.
			if !q.hasPending(hookID) {
				delete(q.running, hookID)
				q.mu.Unlock()
				return
			}
			q.mu.Unlock()
		}
	}
}

// wait blocks until all drainer goroutines exit. It is intended for tests.
func (q *hookDeliveryQueue) wait() {
	q.wg.Wait()
}

// hookDeliverer is the process-wide webhook delivery queue. It stays nil in
// short-lived processes (e.g. Git hooks) where tasks are only persisted,
// those tasks are picked up on the next web process startup.
var hookDeliverer *hookDeliveryQueue

// QueueHookDelivery schedules pending tasks of the webhook for delivery. It
// is safe to call repeatedly, signals are coalesced and tasks already
// delivered are never queued again.
func QueueHookDelivery(hookID int64) {
	if hookDeliverer == nil {
		return
	}
	hookDeliverer.add(hookID)
}

// QueueUndeliveredRepoHookTasks schedules pending tasks of every webhook of
// the repository (e.g. after a manual delivery trigger).
func QueueUndeliveredRepoHookTasks(repoID int64) {
	if hookDeliverer == nil {
		return
	}

	hookIDs, err := pendingHookIDs("repo_id = ?", repoID)
	if err != nil {
		log.Error("Find undelivered hook tasks [repo_id: %d]: %v", repoID, err)
		return
	}
	for _, hookID := range hookIDs {
		hookDeliverer.add(hookID)
	}
}

type HookContentType int

const (
	JSON HookContentType = iota + 1
	FORM
)

var hookContentTypes = map[string]HookContentType{
	"json": JSON,
	"form": FORM,
}

// ToHookContentType returns HookContentType by given name.
func ToHookContentType(name string) HookContentType {
	return hookContentTypes[name]
}

func (t HookContentType) Name() string {
	switch t {
	case JSON:
		return "json"
	case FORM:
		return "form"
	}
	return ""
}

// IsValidHookContentType returns true if given name is a valid hook content type.
func IsValidHookContentType(name string) bool {
	_, ok := hookContentTypes[name]
	return ok
}

type HookEvents struct {
	Create       bool `json:"create"`
	Delete       bool `json:"delete"`
	Fork         bool `json:"fork"`
	Push         bool `json:"push"`
	Issues       bool `json:"issues"`
	PullRequest  bool `json:"pull_request"`
	IssueComment bool `json:"issue_comment"`
	Release      bool `json:"release"`
}

// HookEvent represents events that will delivery hook.
type HookEvent struct {
	PushOnly       bool `json:"push_only"`
	SendEverything bool `json:"send_everything"`
	ChooseEvents   bool `json:"choose_events"`

	HookEvents `json:"events"`
}

type HookStatus int

const (
	HookStatusNone = iota
	HookStatusSucceed
	HookStatusFailed
)

// Webhook represents a web hook object.
type Webhook struct {
	ID           int64
	RepoID       int64
	OrgID        int64
	URL          string `xorm:"url TEXT"`
	ContentType  HookContentType
	Secret       string     `xorm:"TEXT"`
	Events       string     `xorm:"TEXT"`
	*HookEvent   `xorm:"-"` // LEGACY [1.0]: Cannot ignore JSON (i.e. json:"-") here, it breaks old backup archive
	IsSSL        bool       `xorm:"is_ssl"`
	IsActive     bool
	HookTaskType HookTaskType
	Meta         string     `xorm:"TEXT"` // store hook-specific attributes
	LastStatus   HookStatus // Last delivery status

	Created     time.Time `xorm:"-" json:"-" gorm:"-"`
	CreatedUnix int64
	Updated     time.Time `xorm:"-" json:"-" gorm:"-"`
	UpdatedUnix int64
}

func (w *Webhook) BeforeInsert() {
	w.CreatedUnix = time.Now().Unix()
	w.UpdatedUnix = w.CreatedUnix
}

func (w *Webhook) BeforeUpdate() {
	w.UpdatedUnix = time.Now().Unix()
}

func (w *Webhook) AfterSet(colName string, _ xorm.Cell) {
	var err error
	switch colName {
	case "events":
		w.HookEvent = &HookEvent{}
		if err = json.Unmarshal([]byte(w.Events), w.HookEvent); err != nil {
			log.Error("Unmarshal [%d]: %v", w.ID, err)
		}
	case "created_unix":
		w.Created = time.Unix(w.CreatedUnix, 0).Local()
	case "updated_unix":
		w.Updated = time.Unix(w.UpdatedUnix, 0).Local()
	}
}

func (w *Webhook) SlackMeta() *SlackMeta {
	s := &SlackMeta{}
	if err := json.Unmarshal([]byte(w.Meta), s); err != nil {
		log.Error("Failed to get Slack meta [webhook_id: %d]: %v", w.ID, err)
	}
	return s
}

// History returns history of webhook by given conditions.
func (w *Webhook) History(page int) ([]*HookTask, error) {
	return HookTasks(w.ID, page)
}

// UpdateEvent handles conversion from HookEvent to Events.
func (w *Webhook) UpdateEvent() error {
	data, err := json.Marshal(w.HookEvent)
	w.Events = string(data)
	return err
}

// HasCreateEvent returns true if hook enabled create event.
func (w *Webhook) HasCreateEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.Create)
}

// HasDeleteEvent returns true if hook enabled delete event.
func (w *Webhook) HasDeleteEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.Delete)
}

// HasForkEvent returns true if hook enabled fork event.
func (w *Webhook) HasForkEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.Fork)
}

// HasPushEvent returns true if hook enabled push event.
func (w *Webhook) HasPushEvent() bool {
	return w.PushOnly || w.SendEverything ||
		(w.ChooseEvents && w.Push)
}

// HasIssuesEvent returns true if hook enabled issues event.
func (w *Webhook) HasIssuesEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.Issues)
}

// HasPullRequestEvent returns true if hook enabled pull request event.
func (w *Webhook) HasPullRequestEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.PullRequest)
}

// HasIssueCommentEvent returns true if hook enabled issue comment event.
func (w *Webhook) HasIssueCommentEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.IssueComment)
}

// HasReleaseEvent returns true if hook enabled release event.
func (w *Webhook) HasReleaseEvent() bool {
	return w.SendEverything ||
		(w.ChooseEvents && w.Release)
}

type eventChecker struct {
	checker func() bool
	typ     HookEventType
}

func (w *Webhook) EventsArray() []string {
	events := make([]string, 0, 8)
	eventCheckers := []eventChecker{
		{w.HasCreateEvent, HookEventTypeCreate},
		{w.HasDeleteEvent, HookEventTypeDelete},
		{w.HasForkEvent, HookEventTypeFork},
		{w.HasPushEvent, HookEventTypePush},
		{w.HasIssuesEvent, HookEventTypeIssues},
		{w.HasPullRequestEvent, HookEventTypePullRequest},
		{w.HasIssueCommentEvent, HookEventTypeIssueComment},
		{w.HasReleaseEvent, HookEventTypeRelease},
	}
	for _, c := range eventCheckers {
		if c.checker() {
			events = append(events, string(c.typ))
		}
	}
	return events
}

// CreateWebhook creates a new web hook.
func CreateWebhook(w *Webhook) error {
	_, err := x.Insert(w)
	return err
}

var _ errx.NotFound = (*ErrWebhookNotExist)(nil)

type ErrWebhookNotExist struct {
	args map[string]any
}

func IsErrWebhookNotExist(err error) bool {
	_, ok := err.(ErrWebhookNotExist)
	return ok
}

func (err ErrWebhookNotExist) Error() string {
	return fmt.Sprintf("webhook does not exist: %v", err.args)
}

func (ErrWebhookNotExist) NotFound() bool {
	return true
}

// getWebhook uses argument bean as query condition,
// ID must be specified and do not assign unnecessary fields.
func getWebhook(bean *Webhook) (*Webhook, error) {
	has, err := x.Get(bean)
	if err != nil {
		return nil, err
	} else if !has {
		return nil, ErrWebhookNotExist{args: map[string]any{"webhookID": bean.ID}}
	}
	return bean, nil
}

// GetWebhookByID returns webhook by given ID.
// Use this function with caution of accessing unauthorized webhook,
// which means should only be used in non-user interactive functions.
func GetWebhookByID(id int64) (*Webhook, error) {
	return getWebhook(&Webhook{
		ID: id,
	})
}

// GetWebhookOfRepoByID returns webhook of repository by given ID.
func GetWebhookOfRepoByID(repoID, id int64) (*Webhook, error) {
	return getWebhook(&Webhook{
		ID:     id,
		RepoID: repoID,
	})
}

// GetWebhookByOrgID returns webhook of organization by given ID.
func GetWebhookByOrgID(orgID, id int64) (*Webhook, error) {
	return getWebhook(&Webhook{
		ID:    id,
		OrgID: orgID,
	})
}

// getActiveWebhooksByRepoID returns all active webhooks of repository.
func getActiveWebhooksByRepoID(e Engine, repoID int64) ([]*Webhook, error) {
	webhooks := make([]*Webhook, 0, 5)
	return webhooks, e.Where("repo_id = ?", repoID).And("is_active = ?", true).Find(&webhooks)
}

// GetWebhooksByRepoID returns all webhooks of a repository.
func GetWebhooksByRepoID(repoID int64) ([]*Webhook, error) {
	webhooks := make([]*Webhook, 0, 5)
	return webhooks, x.Find(&webhooks, &Webhook{RepoID: repoID})
}

// UpdateWebhook updates information of webhook.
func UpdateWebhook(w *Webhook) error {
	_, err := x.Id(w.ID).AllCols().Update(w)
	return err
}

// deleteWebhook uses argument bean as query condition,
// ID must be specified and do not assign unnecessary fields.
func deleteWebhook(bean *Webhook) (err error) {
	sess := x.NewSession()
	defer sess.Close()
	if err = sess.Begin(); err != nil {
		return err
	}

	if _, err = sess.Delete(bean); err != nil {
		return err
	} else if _, err = sess.Delete(&HookTask{HookID: bean.ID}); err != nil {
		return err
	}

	return sess.Commit()
}

// DeleteWebhookOfRepoByID deletes webhook of repository by given ID.
func DeleteWebhookOfRepoByID(repoID, id int64) error {
	return deleteWebhook(&Webhook{
		ID:     id,
		RepoID: repoID,
	})
}

// DeleteWebhookOfOrgByID deletes webhook of organization by given ID.
func DeleteWebhookOfOrgByID(orgID, id int64) error {
	return deleteWebhook(&Webhook{
		ID:    id,
		OrgID: orgID,
	})
}

// GetWebhooksByOrgID returns all webhooks for an organization.
func GetWebhooksByOrgID(orgID int64) (ws []*Webhook, err error) {
	err = x.Find(&ws, &Webhook{OrgID: orgID})
	return ws, err
}

// getActiveWebhooksByOrgID returns all active webhooks for an organization.
func getActiveWebhooksByOrgID(e Engine, orgID int64) ([]*Webhook, error) {
	ws := make([]*Webhook, 0, 3)
	return ws, e.Where("org_id=?", orgID).And("is_active=?", true).Find(&ws)
}

//   ___ ___                __   ___________              __
//  /   |   \  ____   ____ |  | _\__    ___/____    _____|  | __
// /    ~    \/  _ \ /  _ \|  |/ / |    |  \__  \  /  ___/  |/ /
// \    Y    (  <_> |  <_> )    <  |    |   / __ \_\___ \|    <
//  \___|_  / \____/ \____/|__|_ \ |____|  (____  /____  >__|_ \
//        \/                    \/              \/     \/     \/

type HookTaskType int

const (
	GOGS HookTaskType = iota + 1
	SLACK
	DISCORD
	DINGTALK
)

var hookTaskTypes = map[string]HookTaskType{
	"gogs":     GOGS,
	"slack":    SLACK,
	"discord":  DISCORD,
	"dingtalk": DINGTALK,
}

// ToHookTaskType returns HookTaskType by given name.
func ToHookTaskType(name string) HookTaskType {
	return hookTaskTypes[name]
}

func (t HookTaskType) Name() string {
	switch t {
	case GOGS:
		return "gogs"
	case SLACK:
		return "slack"
	case DISCORD:
		return "discord"
	case DINGTALK:
		return "dingtalk"
	}
	return ""
}

// IsValidHookTaskType returns true if given name is a valid hook task type.
func IsValidHookTaskType(name string) bool {
	_, ok := hookTaskTypes[name]
	return ok
}

type HookEventType string

const (
	HookEventTypeCreate       HookEventType = "create"
	HookEventTypeDelete       HookEventType = "delete"
	HookEventTypeFork         HookEventType = "fork"
	HookEventTypePush         HookEventType = "push"
	HookEventTypeIssues       HookEventType = "issues"
	HookEventTypePullRequest  HookEventType = "pull_request"
	HookEventTypeIssueComment HookEventType = "issue_comment"
	HookEventTypeRelease      HookEventType = "release"
)

// HookRequest represents hook task request information.
type HookRequest struct {
	Headers map[string]string `json:"headers"`
}

// HookResponse represents hook task response information.
type HookResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// HookTask represents a hook task.
type HookTask struct {
	ID                          int64
	RepoID                      int64 `xorm:"INDEX"`
	HookID                      int64 `xorm:"INDEX"`
	UUID                        string
	Type                        HookTaskType
	URL                         string `xorm:"TEXT"`
	Signature                   string `xorm:"TEXT"`
	apiv1types.WebhookPayloader `xorm:"-" json:"-" gorm:"-"`
	PayloadContent              string `xorm:"TEXT"`
	ContentType                 HookContentType
	EventType                   HookEventType
	IsSSL                       bool
	IsDelivered                 bool
	Delivered                   int64
	DeliveredString             string `xorm:"-" json:"-" gorm:"-"`

	// History info.
	IsSucceed       bool
	RequestContent  string        `xorm:"TEXT"`
	RequestInfo     *HookRequest  `xorm:"-" json:"-" gorm:"-"`
	ResponseContent string        `xorm:"TEXT"`
	ResponseInfo    *HookResponse `xorm:"-" json:"-" gorm:"-"`
}

func (t *HookTask) BeforeUpdate() {
	if t.RequestInfo != nil {
		t.RequestContent = t.ToJSON(t.RequestInfo)
	}
	if t.ResponseInfo != nil {
		t.ResponseContent = t.ToJSON(t.ResponseInfo)
	}
}

func (t *HookTask) AfterSet(colName string, _ xorm.Cell) {
	var err error
	switch colName {
	case "delivered":
		t.DeliveredString = time.Unix(0, t.Delivered).Format("2006-01-02 15:04:05 MST")

	case "request_content":
		if t.RequestContent == "" {
			return
		}

		t.RequestInfo = &HookRequest{}
		if err = json.Unmarshal([]byte(t.RequestContent), t.RequestInfo); err != nil {
			log.Error("Unmarshal[%d]: %v", t.ID, err)
		}

	case "response_content":
		if t.ResponseContent == "" {
			return
		}

		t.ResponseInfo = &HookResponse{}
		if err = json.Unmarshal([]byte(t.ResponseContent), t.ResponseInfo); err != nil {
			log.Error("Unmarshal [%d]: %v", t.ID, err)
		}
	}
}

func (t *HookTask) ToJSON(v any) string {
	p, err := json.Marshal(v)
	if err != nil {
		log.Error("Marshal [%d]: %v", t.ID, err)
	}
	return string(p)
}

// HookTasks returns a list of hook tasks by given conditions.
func HookTasks(hookID int64, page int) ([]*HookTask, error) {
	tasks := make([]*HookTask, 0, conf.Webhook.PagingNum)
	return tasks, x.Limit(conf.Webhook.PagingNum, (page-1)*conf.Webhook.PagingNum).Where("hook_id=?", hookID).Desc("id").Find(&tasks)
}

// createHookTask creates a new hook task,
// it handles conversion from Payload to PayloadContent.
func createHookTask(e Engine, t *HookTask) error {
	data, err := t.JSONPayload()
	if err != nil {
		return err
	}
	t.UUID = uuid.New().String()
	t.PayloadContent = string(data)
	_, err = e.Insert(t)
	return err
}

var _ errx.NotFound = (*ErrHookTaskNotExist)(nil)

type ErrHookTaskNotExist struct {
	args map[string]any
}

func IsHookTaskNotExist(err error) bool {
	_, ok := err.(ErrHookTaskNotExist)
	return ok
}

func (err ErrHookTaskNotExist) Error() string {
	return fmt.Sprintf("hook task does not exist: %v", err.args)
}

func (ErrHookTaskNotExist) NotFound() bool {
	return true
}

// GetHookTaskOfWebhookByUUID returns hook task of given webhook by UUID.
func GetHookTaskOfWebhookByUUID(webhookID int64, uuid string) (*HookTask, error) {
	hookTask := &HookTask{
		HookID: webhookID,
		UUID:   uuid,
	}
	has, err := x.Get(hookTask)
	if err != nil {
		return nil, err
	} else if !has {
		return nil, ErrHookTaskNotExist{args: map[string]any{"webhookID": webhookID, "uuid": uuid}}
	}
	return hookTask, nil
}

// UpdateHookTask updates information of hook task.
func UpdateHookTask(t *HookTask) error {
	_, err := x.Id(t.ID).AllCols().Update(t)
	return err
}

// prepareHookTasks adds list of webhooks to task queue.
func prepareHookTasks(e Engine, repo *Repository, event HookEventType, p apiv1types.WebhookPayloader, webhooks []*Webhook) (err error) {
	if len(webhooks) == 0 {
		return nil
	}

	var payloader apiv1types.WebhookPayloader
	hookIDs := make([]int64, 0, len(webhooks))
	for _, w := range webhooks {
		switch event {
		case HookEventTypeCreate:
			if !w.HasCreateEvent() {
				continue
			}
		case HookEventTypeDelete:
			if !w.HasDeleteEvent() {
				continue
			}
		case HookEventTypeFork:
			if !w.HasForkEvent() {
				continue
			}
		case HookEventTypePush:
			if !w.HasPushEvent() {
				continue
			}
		case HookEventTypeIssues:
			if !w.HasIssuesEvent() {
				continue
			}
		case HookEventTypePullRequest:
			if !w.HasPullRequestEvent() {
				continue
			}
		case HookEventTypeIssueComment:
			if !w.HasIssueCommentEvent() {
				continue
			}
		case HookEventTypeRelease:
			if !w.HasReleaseEvent() {
				continue
			}
		}

		// Use separate objects so modifications won't be made on payload on non-Gogs type hooks.
		switch w.HookTaskType {
		case SLACK:
			payloader, err = GetSlackPayload(p, event, w.Meta)
			if err != nil {
				return errors.Newf("GetSlackPayload: %v", err)
			}
		case DISCORD:
			payloader, err = GetDiscordPayload(p, event, w.Meta)
			if err != nil {
				return errors.Newf("GetDiscordPayload: %v", err)
			}
		case DINGTALK:
			payloader, err = GetDingtalkPayload(p, event)
			if err != nil {
				return errors.Newf("GetDingtalkPayload: %v", err)
			}
		default:
			payloader = p
		}

		var signature string
		if len(w.Secret) > 0 {
			data, err := payloader.JSONPayload()
			if err != nil {
				log.Error("prepareWebhooks.JSONPayload: %v", err)
			}
			sig := hmac.New(sha256.New, []byte(w.Secret))
			_, _ = sig.Write(data)
			signature = hex.EncodeToString(sig.Sum(nil))
		}

		if err = createHookTask(e, &HookTask{
			RepoID:           repo.ID,
			HookID:           w.ID,
			Type:             w.HookTaskType,
			URL:              w.URL,
			Signature:        signature,
			WebhookPayloader: payloader,
			ContentType:      w.ContentType,
			EventType:        event,
			IsSSL:            w.IsSSL,
		}); err != nil {
			return errors.Newf("createHookTask: %v", err)
		}
		hookIDs = append(hookIDs, w.ID)
	}

	// Signal each webhook independently so deliveries to different webhooks
	// don't block each other. It's safe to call during hook execution because
	// no delivery queue is initialized in that short-lived process; the
	// persisted tasks are recovered on the next web process startup.
	for _, hookID := range hookIDs {
		QueueHookDelivery(hookID)
	}
	return nil
}

func prepareWebhooks(e Engine, repo *Repository, event HookEventType, p apiv1types.WebhookPayloader) error {
	webhooks, err := getActiveWebhooksByRepoID(e, repo.ID)
	if err != nil {
		return errors.Newf("getActiveWebhooksByRepoID [%d]: %v", repo.ID, err)
	}

	// check if repo belongs to org and append additional webhooks
	if repo.mustOwner(e).IsOrganization() {
		// get hooks for org
		orgws, err := getActiveWebhooksByOrgID(e, repo.OwnerID)
		if err != nil {
			return errors.Newf("getActiveWebhooksByOrgID [%d]: %v", repo.OwnerID, err)
		}
		webhooks = append(webhooks, orgws...)
	}
	return prepareHookTasks(e, repo, event, p, webhooks)
}

// PrepareWebhooks adds all active webhooks to task queue.
func PrepareWebhooks(repo *Repository, event HookEventType, p apiv1types.WebhookPayloader) error {
	// NOTE: To prevent too many cascading changes in a single refactoring PR, we
	// choose to ignore this function in tests.
	if x == nil && testx.InTest {
		return nil
	}
	return prepareWebhooks(x, repo, event, p)
}

// TestWebhook adds the test webhook matches the ID to task queue.
func TestWebhook(repo *Repository, event HookEventType, p apiv1types.WebhookPayloader, webhookID int64) error {
	webhook, err := GetWebhookOfRepoByID(repo.ID, webhookID)
	if err != nil {
		return errors.Newf("GetWebhookOfRepoByID [repo_id: %d, id: %d]: %v", repo.ID, webhookID, err)
	}
	return prepareHookTasks(x, repo, event, p, []*Webhook{webhook})
}

// deliver performs the HTTP delivery and always leaves a definite result on
// the task (delivered or failed, including invalid URL, blocked network,
// timeout, and non-2xx responses) so the task is never retried implicitly and
// the next task of the same webhook can proceed.
func (t *HookTask) deliver() {
	t.IsDelivered = true
	t.ResponseInfo = &HookResponse{
		Headers: map[string]string{},
	}

	defer func() {
		t.Delivered = time.Now().UnixNano()
		if t.IsSucceed {
			log.Trace("Hook delivered: %s", t.UUID)
		} else {
			log.Trace("Hook delivery failed: %s", t.UUID)
		}

		// Update webhook last delivery status.
		w, err := GetWebhookByID(t.HookID)
		if err != nil {
			log.Error("GetWebhookByID: %v", err)
			return
		}
		if t.IsSucceed {
			w.LastStatus = HookStatusSucceed
		} else {
			w.LastStatus = HookStatusFailed
		}
		if err = UpdateWebhook(w); err != nil {
			log.Error("UpdateWebhook: %v", err)
		}
	}()

	payloadURL, err := url.Parse(t.URL)
	if err != nil {
		t.ResponseInfo.Body = fmt.Sprintf("Cannot parse payload URL: %v", err)
		return
	}
	if netx.IsBlockedLocalHostname(payloadURL.Hostname(), conf.Security.LocalNetworkAllowlist) {
		t.ResponseInfo.Body = "Payload URL resolved to a local network address that is implicitly blocked."
		return
	}

	timeout := time.Duration(conf.Webhook.DeliverTimeout) * time.Second
	req := httplib.Post(t.URL).SetTimeout(timeout, timeout).
		Header("X-Github-Delivery", t.UUID).
		Header("X-Github-Event", string(t.EventType)).
		Header("X-Gogs-Delivery", t.UUID).
		Header("X-Gogs-Signature", t.Signature).
		Header("X-Gogs-Event", string(t.EventType)).
		SetTLSClientConfig(&tls.Config{InsecureSkipVerify: conf.Webhook.SkipTLSVerify}).
		SetCheckRedirect(func(req *http.Request, _ []*http.Request) error {
			// The webhook target is explicitly configured by the user, so any
			// redirect would silently retarget the signed payload. Refuse all
			// redirects rather than chase them.
			return errors.Newf("refusing to follow webhook redirect to %q", req.URL.Redacted())
		})

	switch t.ContentType {
	case JSON:
		req = req.Header("Content-Type", "application/json").Body(t.PayloadContent)
	case FORM:
		req.Param("payload", t.PayloadContent)
	}

	// Record delivery information.
	t.RequestInfo = &HookRequest{
		Headers: map[string]string{},
	}
	for k, vals := range req.Headers() {
		t.RequestInfo.Headers[k] = strings.Join(vals, ",")
	}

	resp, err := req.Response()
	if err != nil {
		// Includes connection and read (i.e. timeout) errors.
		t.ResponseInfo.Body = fmt.Sprintf("Delivery: %v", err)
		return
	}
	defer resp.Body.Close()

	// Status code is 20x can be seen as succeed.
	t.IsSucceed = resp.StatusCode/100 == 2
	t.ResponseInfo.Status = resp.StatusCode
	for k, vals := range resp.Header {
		t.ResponseInfo.Headers[k] = strings.Join(vals, ",")
	}

	p, err := io.ReadAll(resp.Body)
	if err != nil {
		t.ResponseInfo.Body = fmt.Sprintf("read body: %s", err)
		return
	}
	t.ResponseInfo.Body = string(p)
}

// drainPendingHookTasks delivers pending tasks of one webhook in ascending ID
// (i.e. event production) order, one at a time. Every result, including
// timeouts and failures, is persisted before the next task starts. It returns
// false when the webhook has no pending task left.
func drainPendingHookTasks(hookID int64) (keepGoing bool) {
	tasks := make([]*HookTask, 0, 10)
	if err := x.Where("hook_id = ?", hookID).And("is_delivered = ?", false).Asc("id").Find(&tasks); err != nil {
		log.Error("Find undelivered hook tasks [hook_id: %d]: %v", hookID, err)
		time.Sleep(hookDrainBackoff)
		return true
	}
	if len(tasks) == 0 {
		return false
	}
	log.Trace("Deliver %d hook tasks [hook_id: %d]", len(tasks), hookID)

	for _, t := range tasks {
		t.deliver()
		if err := updateHookTaskWithRetry(t); err != nil {
			// The result is not durable, so a later drain may redeliver the
			// task, potentially out of order at the target.
			log.Error("UpdateHookTask [%d]: %v", t.ID, err)
		}
	}
	return true
}

// hasPendingHookTask reports whether the webhook has any undelivered task.
// On a database error it returns true so the caller falls back to a full
// drain, which has its own error handling and backoff.
func hasPendingHookTask(hookID int64) bool {
	count, err := x.Where("hook_id = ?", hookID).And("is_delivered = ?", false).Count(new(HookTask))
	if err != nil {
		log.Error("Count undelivered hook tasks [hook_id: %d]: %v", hookID, err)
		return true
	}
	return count > 0
}

func updateHookTaskWithRetry(t *HookTask) error {
	var err error
	for attempt := 0; attempt < hookTaskUpdateMaxRetries; attempt++ {
		if err = UpdateHookTask(t); err == nil {
			return nil
		}
		if attempt < hookTaskUpdateMaxRetries-1 {
			time.Sleep(hookTaskUpdateRetryInterval)
		}
	}
	return err
}

// pendingHookIDs returns IDs of webhooks that have undelivered tasks matching
// the optional condition (and its bind args), ordered by webhook ID.
func pendingHookIDs(condition string, args ...any) ([]int64, error) {
	type hookIDRow struct {
		HookID int64
	}
	rows := make([]hookIDRow, 0)
	sess := x.Table(new(HookTask)).
		Select("hook_id").
		Where("is_delivered = ?", false).
		GroupBy("hook_id").
		Asc("hook_id")
	if condition != "" {
		sess = sess.And(condition, args...)
	}
	if err := sess.Find(&rows); err != nil {
		return nil, err
	}

	hookIDs := make([]int64, len(rows))
	for i, row := range rows {
		hookIDs[i] = row.HookID
	}
	return hookIDs, nil
}

// enqueuePendingHookTasks schedules every webhook that has undelivered tasks.
// It recovers deliveries interrupted by a service restart without enqueueing
// tasks that have already completed.
func enqueuePendingHookTasks(q *hookDeliveryQueue) {
	hookIDs, err := pendingHookIDs("")
	if err != nil {
		log.Error("Find undelivered hook tasks on startup: %v", err)
		return
	}
	for _, hookID := range hookIDs {
		q.add(hookID)
	}
}

// InitDeliverHooks starts the per-webhook delivery workers and re-enqueues
// tasks that were still pending when the previous process exited.
func InitDeliverHooks() {
	hookDeliverer = newHookDeliveryQueue(conf.Webhook.DeliverConcurrency, drainPendingHookTasks, hasPendingHookTask)
	enqueuePendingHookTasks(hookDeliverer)
}
