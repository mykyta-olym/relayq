//go:build integration

package relayq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq/internal/redistest"
)

var server *redistest.Server

func TestMain(m *testing.M) {
	var err error
	server, err = redistest.Start(true, "--appendonly", "no", "--save", "")
	if err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	server.Remove()
	os.Exit(code)
}

// quiet keeps expected warnings out of the test output.
var quiet = slog.New(slog.DiscardHandler)

// newClient gives each test its own namespace, so tests can run in parallel on
// one server without seeing each other.
func newClient(t *testing.T) *Client {
	t.Helper()
	rdb := server.Client()
	t.Cleanup(func() { _ = rdb.Close() })
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	return NewClient(rdb, WithNamespace("t"+id[:12]))
}

func fastConfig(queue string) WorkerConfig {
	return WorkerConfig{
		Queue:               queue,
		Concurrency:         4,
		LeaseDuration:       2 * time.Second,
		MaintenanceInterval: 25 * time.Millisecond,
		PollInterval:        20 * time.Millisecond,
		Backoff:             Backoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond},
		ShutdownTimeout:     5 * time.Second,
		CancelGrace:         time.Second,
		Logger:              quiet,
	}
}

type running struct {
	w    *Worker
	stop func() error
}

// startWorker runs a worker and returns a stop function that waits for Run.
func startWorker(t *testing.T, c *Client, cfg WorkerConfig, handlers map[string]Handler) *running {
	t.Helper()
	w, err := NewWorker(c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for name, h := range handlers {
		w.Handle(name, h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			result = <-done
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return &running{w: w, stop: stop}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func mustEnqueue(t *testing.T, c *Client, queue, jobType string, opts ...Option) string {
	t.Helper()
	res, err := c.Enqueue(context.Background(), queue, jobType, []byte("{}"), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return res.ID
}

func mustStats(t *testing.T, c *Client, queue string) QueueStats {
	t.Helper()
	stats, err := c.Stats(context.Background(), queue)
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func mustJob(t *testing.T, c *Client, queue, id string) *Job {
	t.Helper()
	job, err := c.Job(context.Background(), queue, id)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func mustDequeue(t *testing.T, c *Client, queue string, n int, lease time.Duration) []*Job {
	t.Helper()
	jobs, err := c.dequeue(context.Background(), queue, n, lease, "test-worker")
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func mustMaintain(t *testing.T, c *Client, queue string) maintainResult {
	t.Helper()
	result, err := c.maintain(context.Background(), queue, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestProcessesAJob(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	got := make(chan *Job, 1)
	startWorker(t, c, fastConfig("q"), map[string]Handler{
		"greet": func(_ context.Context, job *Job) error {
			got <- job
			return nil
		},
	})
	res, err := c.EnqueueJSON(context.Background(), "q", "greet", map[string]string{"to": "world"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicate {
		t.Fatal("first enqueue reported as duplicate")
	}

	select {
	case job := <-got:
		var p struct{ To string }
		if err := job.Decode(&p); err != nil {
			t.Fatal(err)
		}
		if job.ID != res.ID || p.To != "world" || job.Attempt != 1 || job.Delivery != 1 {
			t.Fatalf("delivered job is wrong: %+v", job)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job never ran")
	}
	eventually(t, 5*time.Second, "completed state", func() bool {
		job, err := c.Job(context.Background(), "q", res.ID)
		return err == nil && job.State == StateCompleted
	})
}

func TestBinaryPayloadSurvives(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	res, err := c.Enqueue(context.Background(), "q", "bin", payload)
	if err != nil {
		t.Fatal(err)
	}
	jobs := mustDequeue(t, c, "q", 1, time.Minute)
	if len(jobs) != 1 || string(jobs[0].Payload) != string(payload) {
		t.Fatalf("payload changed in transit")
	}
	if stored := mustJob(t, c, "q", res.ID); string(stored.Payload) != string(payload) {
		t.Fatal("stored payload changed")
	}
}

func TestHigherPriorityRunsFirst(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	want := map[string]int{}
	for _, p := range []int{0, 9, 5, 3, 7} {
		want[mustEnqueue(t, c, "q", "t", Priority(p))] = p
	}
	jobs := mustDequeue(t, c, "q", 5, time.Minute)
	var got []int
	for _, job := range jobs {
		got = append(got, want[job.ID])
	}
	if !slices.Equal(got, []int{9, 7, 5, 3, 0}) {
		t.Fatalf("delivery order by priority: %v", got)
	}
}

// A score that went through Lua tostring() would collapse to 14 significant
// digits. Adjacent sequence numbers would then tie and order by id instead.
func TestFIFOWithinPriorityAtHighSequence(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	keys := newQueueKeys(c.namespace, "q")
	if err := c.rdb.Set(context.Background(), keys.seq, int64(1)<<47, 0).Err(); err != nil {
		t.Fatal(err)
	}
	var want []string
	for range 200 {
		want = append(want, mustEnqueue(t, c, "q", "t", Priority(9)))
	}
	var got []string
	for _, job := range mustDequeue(t, c, "q", 200, time.Minute) {
		got = append(got, job.ID)
	}
	if !slices.Equal(got, want) {
		t.Fatal("jobs of equal priority came out of order")
	}
}

func TestDelayedJobWaitsThenRuns(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	id := mustEnqueue(t, c, "q", "t", Delay(300*time.Millisecond))

	job := mustJob(t, c, "q", id)
	if job.State != StateDelayed {
		t.Fatalf("state %s, want delayed", job.State)
	}
	if jobs := mustDequeue(t, c, "q", 1, time.Minute); len(jobs) != 0 {
		t.Fatal("delayed job was delivered early")
	}
	if r := mustMaintain(t, c, "q"); r.promoted != 0 {
		t.Fatal("delayed job was promoted early")
	}

	eventually(t, 3*time.Second, "promotion", func() bool {
		return mustMaintain(t, c, "q").promoted == 1
	})
	jobs := mustDequeue(t, c, "q", 1, time.Minute)
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Fatal("promoted job not delivered")
	}
	if elapsed := jobs[0].StartedAt.Sub(jobs[0].CreatedAt); elapsed < 300*time.Millisecond {
		t.Fatalf("job ran after %v, before its delay", elapsed)
	}
}

func TestScheduledJobRunsNoSoonerThanRunAt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	runAt := time.Now().Add(400 * time.Millisecond)
	started := make(chan time.Time, 1)
	startWorker(t, c, fastConfig("q"), map[string]Handler{
		"t": func(context.Context, *Job) error {
			started <- time.Now()
			return nil
		},
	})
	mustEnqueue(t, c, "q", "t", RunAt(runAt))
	select {
	case at := <-started:
		// Client and server clocks differ by a little, so allow a small margin.
		if at.Before(runAt.Add(-100 * time.Millisecond)) {
			t.Fatalf("ran %v early", runAt.Sub(at))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled job never ran")
	}
}

func TestRunAtInThePastRunsNow(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	id := mustEnqueue(t, c, "q", "t", RunAt(time.Now().Add(-time.Hour)))
	if job := mustJob(t, c, "q", id); job.State != StateReady {
		t.Fatalf("state %s, want ready", job.State)
	}
}

func TestRetryBackoffGrowsBetweenAttempts(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.Backoff = Backoff{Base: 100 * time.Millisecond, Max: time.Second}

	var mu sync.Mutex
	var times []time.Time
	startWorker(t, c, cfg, map[string]Handler{
		"t": func(_ context.Context, job *Job) error {
			mu.Lock()
			times = append(times, time.Now())
			mu.Unlock()
			if job.Attempt < 4 {
				return errors.New("not yet")
			}
			return nil
		},
	})
	id := mustEnqueue(t, c, "q", "t")
	eventually(t, 10*time.Second, "completion", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateCompleted
	})

	mu.Lock()
	defer mu.Unlock()
	if len(times) != 4 {
		t.Fatalf("%d runs, want 4", len(times))
	}
	for i, ceiling := range []time.Duration{100, 200, 400} {
		gap := times[i+1].Sub(times[i])
		if gap < ceiling*time.Millisecond {
			t.Errorf("gap %d was %v, below the ceiling %vms with no jitter", i+1, gap, int(ceiling))
		}
	}
	if job := mustJob(t, c, "q", id); job.Attempt != 4 || job.LastError != "not yet" {
		t.Fatalf("job record: %+v", job)
	}
}

func TestPerJobBackoffOverridesWorker(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.Backoff = Backoff{Base: time.Hour, Max: time.Hour}
	var runs atomic.Int32
	startWorker(t, c, cfg, map[string]Handler{
		"t": func(context.Context, *Job) error {
			if runs.Add(1) == 1 {
				return errors.New("once")
			}
			return nil
		},
	})
	mustEnqueue(t, c, "q", "t", WithBackoff(10*time.Millisecond, 20*time.Millisecond))
	eventually(t, 5*time.Second, "second run", func() bool { return runs.Load() == 2 })
}

func TestExhaustedJobGoesToDeadLetterThenRequeues(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	var fixed atomic.Bool
	var runs atomic.Int32
	startWorker(t, c, fastConfig("q"), map[string]Handler{
		"t": func(context.Context, *Job) error {
			runs.Add(1)
			if !fixed.Load() {
				return errors.New("boom")
			}
			return nil
		},
	})
	id := mustEnqueue(t, c, "q", "t", MaxAttempts(3))
	eventually(t, 5*time.Second, "dead letter", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateDead
	})

	job := mustJob(t, c, "q", id)
	if runs.Load() != 3 || job.Attempt != 3 || job.LastError != "boom" || job.FailedAt == nil {
		t.Fatalf("after %d runs: %+v", runs.Load(), job)
	}
	dead, err := c.Jobs(context.Background(), "q", StateDead, 0, 10)
	if err != nil || len(dead) != 1 || dead[0].ID != id {
		t.Fatalf("dead list: %v %v", dead, err)
	}

	fixed.Store(true)
	if err := c.RequeueDead(context.Background(), "q", id); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "completion after requeue", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateCompleted
	})
	job = mustJob(t, c, "q", id)
	if job.Attempt != 1 || job.Delivery != 4 {
		t.Fatalf("attempt must restart, delivery must not: %+v", job)
	}
	stats := mustStats(t, c, "q")
	if stats.Dead != 0 || stats.Counters["requeued"] != 1 || stats.Counters["dead"] != 1 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestRequeueOnlyTouchesDeadJobs(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	id := mustEnqueue(t, c, "q", "t")
	if err := c.RequeueDead(context.Background(), "q", id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if got := mustStats(t, c, "q").Ready; got != 1 {
		t.Fatalf("ready = %d", got)
	}
}

func TestRequeueAllDeadCrossesBatchBoundary(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	const total = maintainBatch + 37
	ctx := context.Background()
	for sent := 0; sent < total; {
		n := min(MaxBatchSize, total-sent)
		items := make([]BatchItem, n)
		for i := range items {
			items[i] = BatchItem{Type: "t", Options: []Option{MaxAttempts(1)}}
		}
		if _, err := c.EnqueueBatch(ctx, "q", items); err != nil {
			t.Fatal(err)
		}
		sent += n
	}
	for _, job := range mustDequeue(t, c, "q", total, time.Minute) {
		if status, err := c.fail(ctx, job, "x", 0, false); err != nil || status != statusDead {
			t.Fatalf("fail: %v %v", status, err)
		}
	}
	moved, err := c.RequeueAllDead(ctx, "q")
	if err != nil || moved != total {
		t.Fatalf("moved %d: %v", moved, err)
	}
	stats := mustStats(t, c, "q")
	if stats.Dead != 0 || stats.Ready != total {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	var runs atomic.Int32
	startWorker(t, c, fastConfig("q"), map[string]Handler{
		"t": func(context.Context, *Job) error {
			runs.Add(1)
			return Permanent(errors.New("invalid input"))
		},
	})
	id := mustEnqueue(t, c, "q", "t", MaxAttempts(10))
	eventually(t, 5*time.Second, "dead letter", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateDead
	})
	if runs.Load() != 1 {
		t.Fatalf("%d runs, want 1", runs.Load())
	}
}

func TestPanicCountsAsFailedAttempt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	startWorker(t, c, fastConfig("q"), map[string]Handler{
		"t": func(context.Context, *Job) error { panic("handler bug") },
	})
	id := mustEnqueue(t, c, "q", "t", MaxAttempts(2))
	eventually(t, 5*time.Second, "dead letter", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateDead
	})
	if job := mustJob(t, c, "q", id); !strings.Contains(job.LastError, "handler bug") {
		t.Fatalf("panic text missing: %q", job.LastError)
	}
}

func TestUnknownJobTypeEndsInDeadLetter(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	startWorker(t, c, fastConfig("q"), nil)
	id := mustEnqueue(t, c, "q", "nobody-handles-this", MaxAttempts(2))
	eventually(t, 5*time.Second, "dead letter", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateDead
	})
}

func TestJobTimeoutCancelsAndRetries(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	causes := make(chan error, 4)
	startWorker(t, c, fastConfig("q"), map[string]Handler{
		"t": func(ctx context.Context, job *Job) error {
			if job.Attempt > 1 {
				return nil
			}
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, c, "q", "t", Timeout(50*time.Millisecond))
	eventually(t, 5*time.Second, "retry to finish", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateCompleted
	})
	if cause := <-causes; !errors.Is(cause, ErrJobTimeout) {
		t.Fatalf("cause %v, want ErrJobTimeout", cause)
	}
}

func TestIdempotencyKeyCreatesOneJobUnderConcurrency(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	const callers = 64
	results := make([]Enqueued, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			res, err := c.Enqueue(context.Background(), "q", "t", nil, IdempotencyKey("order-1"))
			if err != nil {
				t.Error(err)
			}
			results[i] = res
		})
	}
	wg.Wait()

	created := 0
	for _, res := range results {
		if !res.Duplicate {
			created++
		}
		if res.ID != results[0].ID {
			t.Fatalf("callers got different ids: %s and %s", res.ID, results[0].ID)
		}
	}
	stats := mustStats(t, c, "q")
	if created != 1 || stats.Ready != 1 {
		t.Fatalf("created %d, ready %d", created, stats.Ready)
	}
	if stats.Counters["deduplicated"] != callers-1 {
		t.Fatalf("deduplicated = %d", stats.Counters["deduplicated"])
	}
}

func TestIdempotencyKeyExpires(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	opts := []Option{IdempotencyKey("k"), IdempotencyTTL(100 * time.Millisecond)}
	first := mustEnqueue(t, c, "q", "t", opts...)
	time.Sleep(150 * time.Millisecond)
	if second := mustEnqueue(t, c, "q", "t", opts...); second == first {
		t.Fatal("expired key still deduplicated")
	}
}

func TestIdempotencyKeysAreScopedToTheQueue(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	if mustEnqueue(t, c, "a", "t", IdempotencyKey("k")) == mustEnqueue(t, c, "b", "t", IdempotencyKey("k")) {
		t.Fatal("key leaked across queues")
	}
}

func TestRepeatedEnqueueOfSameIDIsSuppressed(t *testing.T) {
	// go-redis resends a call when the connection breaks. The script sees the
	// same id twice and must not add the job again.
	t.Parallel()
	c := newClient(t)
	keys := newQueueKeys(c.namespace, "q")
	args := []any{keys.idemPrefix, "q", 1, "fixed", "t", "", 5, 5, 0, 0, 0, 0, 0, "", 1000}
	for range 2 {
		if err := c.run(context.Background(), scriptEnqueue, "q", args...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	stats := mustStats(t, c, "q")
	if stats.Ready != 1 || stats.Counters["enqueued"] != 1 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestBatchRejectsAllOnOneBadItem(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	_, err := c.EnqueueBatch(context.Background(), "q", []BatchItem{
		{Type: "ok"},
		{Type: "bad", Options: []Option{Priority(99)}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
	if got := mustStats(t, c, "q").Ready; got != 0 {
		t.Fatalf("%d jobs stored from a rejected batch", got)
	}
}

func TestExpiredLeaseIsRedelivered(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	id := mustEnqueue(t, c, "q", "t")
	first := mustDequeue(t, c, "q", 1, time.Second)[0]

	if jobs := mustDequeue(t, c, "q", 1, time.Minute); len(jobs) != 0 {
		t.Fatal("leased job visible to another worker")
	}
	if r := mustMaintain(t, c, "q"); r.reclaimed != 0 {
		t.Fatal("live lease reclaimed")
	}
	time.Sleep(1100 * time.Millisecond)
	if r := mustMaintain(t, c, "q"); r.reclaimed != 1 {
		t.Fatalf("expired lease not reclaimed: %+v", r)
	}

	second := mustDequeue(t, c, "q", 1, time.Minute)[0]
	if second.ID != id || second.Attempt != 2 || second.Delivery != first.Delivery+1 {
		t.Fatalf("redelivery: %+v", second)
	}
	if !strings.Contains(second.LastError, "lease expired") {
		t.Fatalf("last error: %q", second.LastError)
	}
	if got := mustStats(t, c, "q").Counters["redelivered"]; got != 1 {
		t.Fatalf("redelivered = %d", got)
	}
}

func TestLeaseExpiryOnLastAttemptGoesToDeadLetter(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	id := mustEnqueue(t, c, "q", "t", MaxAttempts(1))
	mustDequeue(t, c, "q", 1, 10*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	if r := mustMaintain(t, c, "q"); r.buried != 1 {
		t.Fatalf("result %+v", r)
	}
	if job := mustJob(t, c, "q", id); job.State != StateDead {
		t.Fatalf("state %s", job.State)
	}
}

// expireAndRedeliver leaves two deliveries of one job: a stale one and the
// current holder.
func expireAndRedeliver(t *testing.T, c *Client) (stale, current *Job) {
	t.Helper()
	mustEnqueue(t, c, "q", "t")
	stale = mustDequeue(t, c, "q", 1, 10*time.Millisecond)[0]
	time.Sleep(30 * time.Millisecond)
	mustMaintain(t, c, "q")
	current = mustDequeue(t, c, "q", 1, time.Minute)[0]
	return stale, current
}

func TestStaleDeliveryCannotChangeTheJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stale := map[string]func(c *Client, j *Job) (string, error){
		"complete": func(c *Client, j *Job) (string, error) { return c.complete(ctx, j, time.Hour) },
		"fail":     func(c *Client, j *Job) (string, error) { return c.fail(ctx, j, "stale", 0, false) },
		"release":  func(c *Client, j *Job) (string, error) { return c.release(ctx, j) },
	}
	for name, act := range stale {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := newClient(t)
			old, current := expireAndRedeliver(t, c)
			status, err := act(c, old)
			if err != nil || status != statusLost {
				t.Fatalf("status %q, err %v, want lost", status, err)
			}
			job := mustJob(t, c, "q", current.ID)
			if job.State != StateActive || job.LeaseOwner == "" || job.Attempt != 2 {
				t.Fatalf("stale call changed the job: %+v", job)
			}
			if status, err := c.complete(ctx, current, time.Hour); err != nil || status != statusOK {
				t.Fatalf("current holder refused: %q %v", status, err)
			}
		})
	}
}

func TestStaleDeliveryCannotExtendTheLease(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	old, current := expireAndRedeliver(t, c)
	held, err := c.extend(context.Background(), "q", []*Job{old, current}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if held[0] || !held[1] {
		t.Fatalf("held = %v, want [false true]", held)
	}
}

func TestLateAckBeforeReclaimStillCounts(t *testing.T) {
	// The lease has passed but no one reclaimed it, so nobody else has the job.
	// Accepting the ack avoids a pointless duplicate.
	t.Parallel()
	c := newClient(t)
	mustEnqueue(t, c, "q", "t")
	job := mustDequeue(t, c, "q", 1, 10*time.Millisecond)[0]
	time.Sleep(30 * time.Millisecond)
	status, err := c.complete(context.Background(), job, time.Hour)
	if err != nil || status != statusOK {
		t.Fatalf("%q %v", status, err)
	}
	if r := mustMaintain(t, c, "q"); r.reclaimed != 0 {
		t.Fatal("completed job was reclaimed")
	}
}

func TestDoubleAckIsHarmless(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	mustEnqueue(t, c, "q", "t")
	job := mustDequeue(t, c, "q", 1, time.Minute)[0]
	for range 2 {
		if status, err := c.complete(context.Background(), job, time.Hour); err != nil || status != statusOK {
			t.Fatalf("%q %v", status, err)
		}
	}
	if got := mustStats(t, c, "q").Counters["completed"]; got != 1 {
		t.Fatalf("completed = %d", got)
	}
}

func TestHeartbeatKeepsALongJobFromRedelivery(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	// One second, not less: with the whole suite running in parallel under the race
	// detector, a shorter lease can lapse for real and redeliver, which is correct
	// behaviour and not what this test is about.
	cfg.LeaseDuration = time.Second
	var runs atomic.Int32
	handler := func(ctx context.Context, _ *Job) error {
		runs.Add(1)
		select {
		case <-time.After(3500 * time.Millisecond): // several lease periods
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// A second worker is there to steal the job if the lease ever lapses.
	startWorker(t, c, cfg, map[string]Handler{"t": handler})
	startWorker(t, c, cfg, map[string]Handler{"t": handler})

	id := mustEnqueue(t, c, "q", "t")
	eventually(t, 15*time.Second, "completion", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateCompleted
	})
	if runs.Load() != 1 {
		t.Fatalf("%d runs, want 1", runs.Load())
	}
}

func TestLostLeaseCancelsTheHandler(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.LeaseDuration = 300 * time.Millisecond
	cfg.MaintenanceInterval = time.Hour // no reclaim from this worker
	started := make(chan struct{})
	causes := make(chan error, 1)
	startWorker(t, c, cfg, map[string]Handler{
		"t": func(ctx context.Context, job *Job) error {
			if job.Delivery > 1 {
				return nil
			}
			close(started)
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return errors.New("stale failure, must be refused")
		},
	})
	id := mustEnqueue(t, c, "q", "t")
	<-started

	// Simulate the reaper winning: expire the lease and reclaim it directly.
	keys := newQueueKeys(c.namespace, "q")
	if err := c.rdb.ZAdd(context.Background(), keys.active, redis.Z{Score: 1, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	mustMaintain(t, c, "q")

	select {
	case cause := <-causes:
		if !errors.Is(cause, ErrLeaseLost) {
			t.Fatalf("cause %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not told about the lost lease")
	}
	eventually(t, 5*time.Second, "second delivery to finish", func() bool {
		job, err := c.Job(context.Background(), "q", id)
		return err == nil && job.State == StateCompleted
	})
	job := mustJob(t, c, "q", id)
	if job.Delivery != 2 || strings.Contains(job.LastError, "stale failure") {
		t.Fatalf("stale result reached the job: %+v", job)
	}
	if got := mustStats(t, c, "q").Counters["failed"]; got != 0 {
		t.Fatalf("failed = %d, the stale failure was counted", got)
	}
}

// A lapsed lease can bring a job back to the worker whose old handler is still
// running. Both deliveries have to stay tracked. When the second one replaced the
// first, the old handler was never cancelled and shutdown waited for it forever.
func TestTwoDeliveriesOfOneJobInOneWorkerBothStayTracked(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.LeaseDuration = time.Second
	cfg.MaintenanceInterval = time.Hour // only this test reclaims
	cfg.ShutdownTimeout = 100 * time.Millisecond
	started := make(chan int64, 2)
	r := startWorker(t, c, cfg, map[string]Handler{
		"t": func(ctx context.Context, job *Job) error {
			started <- job.Delivery
			<-ctx.Done()
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, c, "q", "t")
	if got := <-started; got != 1 {
		t.Fatalf("first delivery %d", got)
	}

	keys := newQueueKeys(c.namespace, "q")
	if err := c.rdb.ZAdd(context.Background(), keys.active, redis.Z{Score: 1, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	mustMaintain(t, c, "q")
	select {
	case got := <-started:
		if got != 2 {
			t.Fatalf("second delivery %d", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job was not redelivered")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- r.stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown hung on a delivery it lost track of")
	}
	if job := mustJob(t, c, "q", id); job.State != StateReady || job.Delivery != 2 {
		t.Fatalf("job after shutdown: %+v", job)
	}
}

func TestShutdownFinishesInFlightJob(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	started := make(chan struct{})
	r := startWorker(t, c, fastConfig("q"), map[string]Handler{
		"t": func(ctx context.Context, _ *Job) error {
			close(started)
			select {
			case <-time.After(300 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	id := mustEnqueue(t, c, "q", "t")
	<-started
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if job := mustJob(t, c, "q", id); job.State != StateCompleted || job.Attempt != 1 {
		t.Fatalf("job after shutdown: %+v", job)
	}
}

func TestShutdownStopsFetching(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.Concurrency = 1
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	r := startWorker(t, c, cfg, map[string]Handler{
		"t": func(context.Context, *Job) error {
			started <- struct{}{}
			<-release
			return nil
		},
	})
	mustEnqueue(t, c, "q", "t")
	<-started
	waiting := mustEnqueue(t, c, "q", "t")

	stopped := make(chan error, 1)
	go func() { stopped <- r.stop() }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if job := mustJob(t, c, "q", waiting); job.State != StateReady || job.Delivery != 0 {
		t.Fatalf("queued job was touched during shutdown: %+v", job)
	}
}

func TestShutdownReleasesJobAfterTimeoutWithoutUsingAnAttempt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.ShutdownTimeout = 100 * time.Millisecond
	started := make(chan struct{})
	causes := make(chan error, 1)
	r := startWorker(t, c, cfg, map[string]Handler{
		"t": func(ctx context.Context, _ *Job) error {
			close(started)
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, c, "q", "t")
	<-started
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if cause := <-causes; !errors.Is(cause, ErrShuttingDown) {
		t.Fatalf("cause %v", cause)
	}

	job := mustJob(t, c, "q", id)
	if job.State != StateReady || job.Attempt != 0 || job.Delivery != 1 {
		t.Fatalf("released job: %+v", job)
	}
	next := mustDequeue(t, c, "q", 1, time.Minute)
	if len(next) != 1 || next[0].Attempt != 1 || next[0].Delivery != 2 {
		t.Fatalf("redelivery after release: %+v", next)
	}
}

func TestShutdownAbandonsHandlerThatIgnoresCancellation(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.ShutdownTimeout = 50 * time.Millisecond
	cfg.CancelGrace = 50 * time.Millisecond
	started := make(chan struct{})
	unblock := make(chan struct{})
	returned := make(chan struct{})
	r := startWorker(t, c, cfg, map[string]Handler{
		"t": func(context.Context, *Job) error {
			defer close(returned)
			close(started)
			<-unblock
			return nil
		},
	})
	id := mustEnqueue(t, c, "q", "t")
	<-started
	if err := r.stop(); !errors.Is(err, ErrJobsAbandoned) {
		t.Fatalf("got %v, want ErrJobsAbandoned", err)
	}
	if job := mustJob(t, c, "q", id); job.State != StateReady {
		t.Fatalf("abandoned job: %+v", job)
	}

	// The next worker takes the job, then the old handler finally returns nil.
	// Its success must not touch the job.
	current := mustDequeue(t, c, "q", 1, time.Minute)[0]
	close(unblock)
	<-returned
	time.Sleep(50 * time.Millisecond)
	job := mustJob(t, c, "q", id)
	if job.State != StateActive || job.Delivery != current.Delivery {
		t.Fatalf("late handler changed the job: %+v", job)
	}
}

func TestCompletedJobsExpireAfterRetention(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	mustEnqueue(t, c, "q", "t")
	job := mustDequeue(t, c, "q", 1, time.Minute)[0]
	if status, err := c.complete(context.Background(), job, 100*time.Millisecond); err != nil || status != statusOK {
		t.Fatalf("%q %v", status, err)
	}
	if got := mustStats(t, c, "q").Completed; got != 1 {
		t.Fatalf("completed = %d", got)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := c.Job(context.Background(), "q", job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("hash survived retention: %v", err)
	}
	if _, err := c.maintain(context.Background(), "q", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := mustStats(t, c, "q").Completed; got != 0 {
		t.Fatalf("index survived retention: %d", got)
	}
}

func TestNegativeRetentionDeletesOnCompletion(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	cfg.Retention = -1
	startWorker(t, c, cfg, map[string]Handler{"t": func(context.Context, *Job) error { return nil }})
	id := mustEnqueue(t, c, "q", "t")
	eventually(t, 5*time.Second, "deletion", func() bool {
		_, err := c.Job(context.Background(), "q", id)
		return errors.Is(err, ErrNotFound)
	})
	stats := mustStats(t, c, "q")
	if stats.Completed != 0 || stats.Counters["completed"] != 1 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestDeleteByState(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()

	if err := c.Delete(ctx, "q", mustEnqueue(t, c, "q", "t", Delay(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "q", mustEnqueue(t, c, "q", "t")); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "q", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	stats := mustStats(t, c, "q")
	if stats.Ready != 0 || stats.Delayed != 0 {
		t.Fatalf("stats: %+v", stats)
	}

	id := mustEnqueue(t, c, "q", "t")
	mustDequeue(t, c, "q", 1, time.Minute)
	if err := c.Delete(ctx, "q", id); !errors.Is(err, ErrJobActive) {
		t.Fatalf("got %v, want ErrJobActive", err)
	}
	if job := mustJob(t, c, "q", id); job.State != StateActive {
		t.Fatalf("active job changed: %+v", job)
	}
}

func TestJobsListingAndPaging(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	var ids []string
	for range 5 {
		ids = append(ids, mustEnqueue(t, c, "q", "t"))
	}
	page, err := c.Jobs(context.Background(), "q", StateReady, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].ID != ids[1] || page[1].ID != ids[2] {
		t.Fatalf("page: %+v", page)
	}
	if _, err := c.Jobs(context.Background(), "q", "nonsense", 0, 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
	empty, err := c.Jobs(context.Background(), "q", StateDead, 0, 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("%v %v", empty, err)
	}
}

func TestQueuesLists(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	mustEnqueue(t, c, "b", "t")
	mustEnqueue(t, c, "a", "t")
	got, err := c.Queues(context.Background())
	if err != nil || !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("%v %v", got, err)
	}
}

// Not parallel: it changes a server setting. Eviction is off in this suite because
// maxmemory is unset, so the policy has no effect on other tests.
func TestCheckRedisWarnsAboutRiskySettings(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	warnings := strings.Join(c.CheckRedis(ctx), "\n")
	// The suite can also run against an external server with AOF on.
	aofOff := c.rdb.ConfigGet(ctx, "appendonly").Val()["appendonly"] == "no"
	if strings.Contains(warnings, "appendonly") != aofOff {
		t.Fatalf("appendonly off is %v, warnings are %q", aofOff, warnings)
	}
	if strings.Contains(warnings, "maxmemory-policy") {
		t.Fatalf("eviction warning on a noeviction server: %q", warnings)
	}

	if err := c.rdb.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.rdb.ConfigSet(context.Background(), "maxmemory-policy", "noeviction") })
	warnings = strings.Join(c.CheckRedis(ctx), "\n")
	if !strings.Contains(warnings, "allkeys-lru") {
		t.Fatalf("no eviction warning in %q", warnings)
	}
}

func TestCollectorExportsRedisCounts(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	mustEnqueue(t, c, "q", "t")
	mustEnqueue(t, c, "q", "t")
	mustEnqueue(t, c, "q", "t", Delay(time.Hour))
	mustDequeue(t, c, "q", 1, time.Minute)

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewCollector(c))
	const want = `
# HELP relayq_redis_up 1 if the last scrape could read Redis.
# TYPE relayq_redis_up gauge
relayq_redis_up 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "relayq_redis_up"); err != nil {
		t.Fatal(err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]float64{}
	for _, family := range families {
		for _, m := range family.Metric {
			var labels []string
			for _, l := range m.Label {
				labels = append(labels, l.GetValue())
			}
			name := family.GetName() + "/" + strings.Join(labels, "/")
			if m.Gauge != nil {
				found[name] = m.Gauge.GetValue()
			} else {
				found[name] = m.Counter.GetValue()
			}
		}
	}
	for name, value := range map[string]float64{
		"relayq_queue_jobs/q/ready":       1,
		"relayq_queue_jobs/q/active":      1,
		"relayq_queue_jobs/q/delayed":     1,
		"relayq_jobs_total/enqueued/q":    3,
		"relayq_jobs_total/delivered/q":   1,
		"relayq_jobs_total/redelivered/q": 0,
	} {
		if got, ok := found[name]; !ok || got != value {
			t.Errorf("%s = %v (present %v), want %v", name, got, ok, value)
		}
	}
}

func TestCollectorReportsRedisDown(t *testing.T) {
	t.Parallel()
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewCollector(NewClient(rdb)))
	const want = `
# HELP relayq_redis_up 1 if the last scrape could read Redis.
# TYPE relayq_redis_up gauge
relayq_redis_up 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "relayq_redis_up"); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerMetricsCountOutcomes(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	cfg := fastConfig("q")
	reg := prometheus.NewPedanticRegistry()
	cfg.Registerer = reg
	startWorker(t, c, cfg, map[string]Handler{
		"good": func(context.Context, *Job) error { return nil },
		"bad":  func(context.Context, *Job) error { return Permanent(errors.New("no")) },
	})
	mustEnqueue(t, c, "q", "good")
	mustEnqueue(t, c, "q", "bad")
	eventually(t, 5*time.Second, "both jobs", func() bool {
		stats := mustStats(t, c, "q")
		return stats.Completed == 1 && stats.Dead == 1
	})
	eventually(t, 5*time.Second, "metric update", func() bool {
		n, err := testutil.GatherAndCount(reg, "relayq_worker_handler_duration_seconds")
		return err == nil && n == 2
	})
}

// Sixteen maintenance loops race on the same due jobs. Each job must be promoted
// once, or the worker would receive it twice.
func TestConcurrentMaintenancePromotesEachJobOnce(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	const total = 1000
	items := make([]BatchItem, total)
	for i := range items {
		items[i] = BatchItem{Type: "t", Options: []Option{Delay(time.Millisecond)}}
	}
	if _, err := c.EnqueueBatch(context.Background(), "q", items); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	var promoted atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 10 {
				r, err := c.maintain(context.Background(), "q", time.Hour)
				if err != nil {
					t.Error(err)
					return
				}
				promoted.Add(r.promoted)
			}
		})
	}
	wg.Wait()

	stats := mustStats(t, c, "q")
	if promoted.Load() != total || stats.Ready != total || stats.Delayed != 0 {
		t.Fatalf("promoted %d, stats %+v", promoted.Load(), stats)
	}
}

// executions records every handler run per job id.
type executions struct {
	mu   sync.Mutex
	runs map[string]int
}

func (e *executions) record(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runs == nil {
		e.runs = make(map[string]int)
	}
	e.runs[id]++
	return e.runs[id]
}

func (e *executions) snapshot() map[string]int {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]int, len(e.runs))
	for id, n := range e.runs {
		out[id] = n
	}
	return out
}

// No crashes and no failures here, so atomic dequeue must give exactly one run per
// job. A non atomic pop shows up as a job run twice.
func TestManyConcurrentWorkersRunEachJobOnce(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	const total, workers, producers = 20000, 16, 8

	var seen executions
	var doubled atomic.Int64
	handler := func(_ context.Context, job *Job) error {
		if seen.record(job.ID) > 1 {
			doubled.Add(1)
		}
		return nil
	}
	for range workers {
		cfg := fastConfig("q")
		cfg.Concurrency = 16
		cfg.LeaseDuration = 30 * time.Second
		cfg.Retention = -1
		startWorker(t, c, cfg, map[string]Handler{"t": handler})
	}

	ids := make(chan string, total)
	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range total / producers / 100 {
				items := make([]BatchItem, 100)
				for i := range items {
					items[i] = BatchItem{Type: "t", Options: []Option{Priority(rand.IntN(10))}}
				}
				out, err := c.EnqueueBatch(context.Background(), "q", items)
				if err != nil {
					t.Error(err)
					return
				}
				for _, res := range out {
					ids <- res.ID
				}
			}
		})
	}
	wg.Wait()
	close(ids)

	eventually(t, 60*time.Second, "all jobs to complete", func() bool {
		if n := doubled.Load(); n > 0 {
			t.Fatalf("%d handler runs were for a job that had already run", n)
		}
		return mustStats(t, c, "q").Counters["completed"] == total
	})
	runs := seen.snapshot()
	for id := range ids {
		if runs[id] != 1 {
			t.Fatalf("job %s ran %d times", id, runs[id])
		}
	}
	stats := mustStats(t, c, "q")
	if stats.Ready != 0 || stats.Active != 0 || stats.Delayed != 0 || stats.Dead != 0 {
		t.Fatalf("leftovers: %+v", stats)
	}
	if len(runs) != total {
		t.Fatalf("%d distinct jobs ran, want %d", len(runs), total)
	}
}

// Random failures, retries and a mix of delayed work, with every job required to
// finish. Accounting has to close exactly: nothing lost, nothing invented.
func TestRetriesUnderConcurrencyLoseNothing(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	const total, workers = 4000, 8

	var seen executions
	handler := func(_ context.Context, job *Job) error {
		seen.record(job.ID)
		if job.Attempt < 3 && rand.IntN(3) == 0 {
			return errors.New("flaky")
		}
		return nil
	}
	for range workers {
		cfg := fastConfig("q")
		cfg.Concurrency = 8
		cfg.Backoff = Backoff{Base: time.Millisecond, Max: 20 * time.Millisecond, Jitter: 1}
		startWorker(t, c, cfg, map[string]Handler{"t": handler})
	}
	want := make(map[string]bool, total)
	for sent := 0; sent < total; sent += 100 {
		items := make([]BatchItem, 100)
		for i := range items {
			var opts []Option
			if i%5 == 0 {
				opts = append(opts, Delay(time.Duration(rand.IntN(100))*time.Millisecond))
			}
			items[i] = BatchItem{Type: "t", Payload: []byte(fmt.Sprint(i)), Options: opts}
		}
		out, err := c.EnqueueBatch(context.Background(), "q", items)
		if err != nil {
			t.Fatal(err)
		}
		for _, res := range out {
			want[res.ID] = true
		}
	}

	eventually(t, 60*time.Second, "all jobs to complete", func() bool {
		return mustStats(t, c, "q").Counters["completed"] == total
	})
	runs := seen.snapshot()
	var executed int64
	for id := range want {
		if runs[id] == 0 {
			t.Fatalf("job %s never ran", id)
		}
		executed += int64(runs[id])
	}
	stats := mustStats(t, c, "q")
	if stats.Dead != 0 || stats.Ready != 0 || stats.Delayed != 0 || stats.Active != 0 {
		t.Fatalf("leftovers: %+v", stats)
	}
	if stats.Counters["delivered"] != executed {
		t.Fatalf("delivered %d, executed %d", stats.Counters["delivered"], executed)
	}
	if stats.Counters["failed"] != executed-total || stats.Counters["retried"] != executed-total {
		t.Fatalf("executed %d of %d jobs, counters %+v", executed, total, stats.Counters)
	}
}
