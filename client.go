package relayq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultNamespace prefixes every key relayq writes.
const DefaultNamespace = "relayq"

// Client enqueues and inspects jobs. It is safe for concurrent use and owns no
// resources beyond the Redis client it was given.
type Client struct {
	rdb       redis.UniversalClient
	namespace string
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithNamespace isolates a set of queues inside one Redis database.
func WithNamespace(namespace string) ClientOption {
	return func(c *Client) { c.namespace = namespace }
}

// NewClient wraps a go-redis client. Set ContextTimeoutEnabled on it so that
// context deadlines also bound socket reads.
func NewClient(rdb redis.UniversalClient, opts ...ClientOption) *Client {
	c := &Client{rdb: rdb, namespace: DefaultNamespace}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Redis returns the underlying client.
func (c *Client) Redis() redis.UniversalClient { return c.rdb }

// Enqueued reports the outcome of an enqueue.
type Enqueued struct {
	ID string `json:"id"`

	// Duplicate is true when an idempotency key matched and no job was created.
	// ID is then the job from the original call.
	Duplicate bool `json:"duplicate"`
}

// BatchItem is one job in EnqueueBatch.
type BatchItem struct {
	Type    string
	Payload []byte
	Options []Option
}

// Enqueue adds a job. When it returns nil the job is in Redis and will be
// delivered at least once, subject to the durability of that Redis server.
func (c *Client) Enqueue(ctx context.Context, queue, jobType string, payload []byte, opts ...Option) (Enqueued, error) {
	out, err := c.EnqueueBatch(ctx, queue, []BatchItem{{Type: jobType, Payload: payload, Options: opts}})
	if err != nil {
		return Enqueued{}, err
	}
	return out[0], nil
}

// EnqueueJSON marshals v and enqueues it.
func (c *Client) EnqueueJSON(ctx context.Context, queue, jobType string, v any, opts ...Option) (Enqueued, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return Enqueued{}, fmt.Errorf("%w: marshal payload: %v", ErrInvalid, err)
	}
	return c.Enqueue(ctx, queue, jobType, payload, opts...)
}

// EnqueueBatch adds up to MaxBatchSize jobs in one atomic script: all of them are
// stored or none are.
func (c *Client) EnqueueBatch(ctx context.Context, queue string, items []BatchItem) ([]Enqueued, error) {
	if err := validateQueue(queue); err != nil {
		return nil, err
	}
	if len(items) == 0 || len(items) > MaxBatchSize {
		return nil, fmt.Errorf("%w: batch size must be 1 to %d", ErrInvalid, MaxBatchSize)
	}

	keys := newQueueKeys(c.namespace, queue)
	args := make([]any, 0, 3+12*len(items))
	args = append(args, keys.idemPrefix, queue, len(items))
	for _, item := range items {
		if err := validateType(item.Type); err != nil {
			return nil, err
		}
		spec, err := newSpec(item.Options)
		if err != nil {
			return nil, err
		}
		id, err := newJobID()
		if err != nil {
			return nil, err
		}
		var runAt int64
		if !spec.runAt.IsZero() {
			runAt = spec.runAt.UnixMilli()
		}
		args = append(args,
			id, item.Type, item.Payload, spec.priority, spec.maxAttempts,
			spec.timeout.Milliseconds(), spec.backoffBase.Milliseconds(),
			spec.backoffMax.Milliseconds(), spec.delay.Milliseconds(), runAt,
			spec.idempotencyKey, spec.idempotencyTTL.Milliseconds())
	}

	raw, err := c.run(ctx, scriptEnqueue, queue, args...).Slice()
	if err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	out := make([]Enqueued, 0, len(raw))
	for _, entry := range raw {
		pair, ok := entry.([]any)
		if !ok || len(pair) != 2 {
			return nil, fmt.Errorf("enqueue: unexpected reply %v", entry)
		}
		id, ok := pair[0].(string)
		created, ok2 := pair[1].(int64)
		if !ok || !ok2 {
			return nil, fmt.Errorf("enqueue: unexpected reply %v", entry)
		}
		out = append(out, Enqueued{ID: id, Duplicate: created == 0})
	}
	return out, nil
}

// Job fetches one job.
func (c *Client) Job(ctx context.Context, queue, id string) (*Job, error) {
	if err := validateQueue(queue); err != nil {
		return nil, err
	}
	fields, err := c.rdb.HGetAll(ctx, newQueueKeys(c.namespace, queue).jobPrefix+id).Result()
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if len(fields) == 0 {
		return nil, ErrNotFound
	}
	return jobFromFields(fields)
}

// Jobs lists jobs in one state. Ready, delayed and active jobs come in processing
// order, dead and completed jobs newest first.
func (c *Client) Jobs(ctx context.Context, queue string, state State, offset, limit int) ([]*Job, error) {
	if err := validateQueue(queue); err != nil {
		return nil, err
	}
	if offset < 0 || limit < 1 {
		return nil, fmt.Errorf("%w: offset must be 0 or more and limit 1 or more", ErrInvalid)
	}
	keys := newQueueKeys(c.namespace, queue)
	key, err := keys.forState(state)
	if err != nil {
		return nil, err
	}

	start, stop := int64(offset), int64(offset+limit-1)
	var ids []string
	if state == StateDead || state == StateCompleted {
		ids, err = c.rdb.ZRevRange(ctx, key, start, stop).Result()
	} else {
		ids, err = c.rdb.ZRange(ctx, key, start, stop).Result()
	}
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, keys.jobPrefix+id)
	}
	if len(ids) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("list jobs: %w", err)
		}
	}

	jobs := make([]*Job, 0, len(ids))
	for _, cmd := range cmds {
		fields := cmd.Val()
		if len(fields) == 0 {
			continue // retention expired between the two reads
		}
		job, err := jobFromFields(fields)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// Queues lists every queue that has ever received a job.
func (c *Client) Queues(ctx context.Context) ([]string, error) {
	names, err := c.rdb.SMembers(ctx, newQueueKeys(c.namespace, "").queues).Result()
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	slices.Sort(names)
	return names, nil
}

// QueueStats is a point in time view of one queue. Counters are cumulative and
// shared by every process using the queue.
type QueueStats struct {
	Queue     string           `json:"queue"`
	Ready     int64            `json:"ready"`
	Delayed   int64            `json:"delayed"`
	Active    int64            `json:"active"`
	Dead      int64            `json:"dead"`
	Completed int64            `json:"completed"`
	Counters  map[string]int64 `json:"counters"`
}

// CounterNames lists the cumulative counters kept per queue.
var CounterNames = []string{
	"enqueued", "deduplicated", "delivered", "completed", "failed", "retried",
	"redelivered", "released", "dead", "requeued", "deleted",
}

// Stats reads the set sizes and counters of a queue in one round trip.
func (c *Client) Stats(ctx context.Context, queue string) (QueueStats, error) {
	if err := validateQueue(queue); err != nil {
		return QueueStats{}, err
	}
	keys := newQueueKeys(c.namespace, queue)
	pipe := c.rdb.Pipeline()
	ready := pipe.ZCard(ctx, keys.ready)
	delayed := pipe.ZCard(ctx, keys.delayed)
	active := pipe.ZCard(ctx, keys.active)
	dead := pipe.ZCard(ctx, keys.dead)
	completed := pipe.ZCard(ctx, keys.completed)
	counters := pipe.HGetAll(ctx, keys.stats)
	if _, err := pipe.Exec(ctx); err != nil {
		return QueueStats{}, fmt.Errorf("queue stats: %w", err)
	}

	stats := QueueStats{
		Queue:     queue,
		Ready:     ready.Val(),
		Delayed:   delayed.Val(),
		Active:    active.Val(),
		Dead:      dead.Val(),
		Completed: completed.Val(),
		Counters:  make(map[string]int64, len(CounterNames)),
	}
	for _, name := range CounterNames {
		stats.Counters[name] = 0
	}
	for name, raw := range counters.Val() {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return QueueStats{}, fmt.Errorf("counter %s: %w", name, err)
		}
		stats.Counters[name] = n
	}
	return stats, nil
}

// RequeueDead moves one dead job back to ready with a fresh attempt budget.
func (c *Client) RequeueDead(ctx context.Context, queue, id string) error {
	if err := validateQueue(queue); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("%w: job id is required", ErrInvalid)
	}
	moved, err := c.run(ctx, scriptRequeue, queue, id, 1).Int64()
	if err != nil {
		return fmt.Errorf("requeue: %w", err)
	}
	if moved == 0 {
		return ErrNotFound
	}
	return nil
}

// RequeueAllDead empties the dead letter queue in batches, so a large queue does
// not block Redis for the whole move. It returns the number of jobs moved.
func (c *Client) RequeueAllDead(ctx context.Context, queue string) (int64, error) {
	if err := validateQueue(queue); err != nil {
		return 0, err
	}
	var total int64
	for {
		moved, err := c.run(ctx, scriptRequeue, queue, "", maintainBatch).Int64()
		if err != nil {
			return total, fmt.Errorf("requeue: %w", err)
		}
		total += moved
		if moved < maintainBatch {
			return total, nil
		}
	}
}

// Delete removes a job in any state but active.
func (c *Client) Delete(ctx context.Context, queue, id string) error {
	if err := validateQueue(queue); err != nil {
		return err
	}
	status, err := c.run(ctx, scriptDelete, queue, id).Text()
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	switch status {
	case "ok":
		return nil
	case "active":
		return ErrJobActive
	default:
		return ErrNotFound
	}
}

// Ping checks that Redis answers.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// CheckRedis returns warnings about server settings that weaken delivery. Servers
// that disable CONFIG give no warnings rather than an error.
func (c *Client) CheckRedis(ctx context.Context) []string {
	config, err := c.rdb.ConfigGet(ctx, "*").Result()
	if err != nil {
		return nil
	}
	var warnings []string
	if policy := config["maxmemory-policy"]; policy != "" && policy != "noeviction" {
		warnings = append(warnings, fmt.Sprintf(
			"maxmemory-policy is %s: Redis may evict job data, set noeviction", policy))
	}
	if config["appendonly"] == "no" {
		warnings = append(warnings,
			"appendonly is no: jobs written since the last snapshot are lost if Redis stops")
	}
	return warnings
}

func (c *Client) run(ctx context.Context, script *redis.Script, queue string, args ...any) *redis.Cmd {
	keys := newQueueKeys(c.namespace, queue)
	full := make([]any, 0, len(args)+2)
	full = append(full, keys.jobPrefix, keys.wake)
	full = append(full, args...)
	return script.Run(ctx, c.rdb, keys.list(), full...)
}

// settle status values returned by the scripts.
const (
	statusOK    = "ok"
	statusLost  = "lost"
	statusRetry = "retry"
	statusDead  = "dead"
)

func (c *Client) dequeue(ctx context.Context, queue string, count int, lease time.Duration, worker string) ([]*Job, error) {
	raw, err := c.run(ctx, scriptDequeue, queue, count, lease.Milliseconds(), worker).Slice()
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	jobs := make([]*Job, 0, len(raw))
	for _, entry := range raw {
		flat, ok := entry.([]any)
		if !ok {
			return nil, fmt.Errorf("dequeue: unexpected reply %v", entry)
		}
		job, err := parseJob(flat)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (c *Client) complete(ctx context.Context, job *Job, retention time.Duration) (string, error) {
	return c.run(ctx, scriptComplete, job.Queue, job.ID, job.Delivery, retention.Milliseconds()).Text()
}

func (c *Client) fail(ctx context.Context, job *Job, message string, delay time.Duration, permanent bool) (string, error) {
	flag := "0"
	if permanent {
		flag = "1"
	}
	return c.run(ctx, scriptFail, job.Queue, job.ID, job.Delivery, message, delay.Milliseconds(), flag).Text()
}

func (c *Client) release(ctx context.Context, job *Job) (string, error) {
	return c.run(ctx, scriptRelease, job.Queue, job.ID, job.Delivery).Text()
}

// extend renews leases and reports, per job, whether the lease is still held.
func (c *Client) extend(ctx context.Context, queue string, jobs []*Job, lease time.Duration) ([]bool, error) {
	args := make([]any, 0, 1+2*len(jobs))
	args = append(args, lease.Milliseconds())
	for _, job := range jobs {
		args = append(args, job.ID, job.Delivery)
	}
	raw, err := c.run(ctx, scriptExtend, queue, args...).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("extend: %w", err)
	}
	if len(raw) != len(jobs) {
		return nil, errors.New("extend: reply length does not match the request")
	}
	held := make([]bool, len(raw))
	for i, v := range raw {
		held[i] = v == 1
	}
	return held, nil
}

// maintainResult counts what one pass of the periodic work moved.
type maintainResult struct {
	promoted, reclaimed, buried, trimmed int64
}

func (r maintainResult) full() bool {
	return r.promoted >= maintainBatch || r.reclaimed+r.buried >= maintainBatch
}

// maintainBatch bounds how long one maintenance script can block Redis.
const maintainBatch = 500

func (c *Client) maintain(ctx context.Context, queue string, retention time.Duration) (maintainResult, error) {
	raw, err := c.run(ctx, scriptMaintain, queue, maintainBatch, retention.Milliseconds()).Int64Slice()
	if err != nil {
		return maintainResult{}, fmt.Errorf("maintain: %w", err)
	}
	if len(raw) != 4 {
		return maintainResult{}, errors.New("maintain: unexpected reply")
	}
	return maintainResult{promoted: raw[0], reclaimed: raw[1], buried: raw[2], trimmed: raw[3]}, nil
}

func newJobID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
