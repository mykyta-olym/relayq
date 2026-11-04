package relayq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Handler processes one delivery. Returning nil acknowledges the job. Returning
// an error schedules a retry, or sends the job to the dead letter queue when it
// is out of attempts or the error is Permanent. A panic counts as an error.
//
// Delivery is at least once, so a handler must tolerate running twice. The
// context is cancelled on job timeout, on lease loss and late in a shutdown. Use
// context.Cause to tell these apart.
type Handler func(ctx context.Context, job *Job) error

// WorkerConfig tunes a Worker. Zero values take the documented defaults.
type WorkerConfig struct {
	Queue string

	// Concurrency is how many handlers run at once. Default 10.
	Concurrency int

	// LeaseDuration is the visibility timeout: how long a job stays hidden from
	// other workers without a heartbeat. Default 30s.
	LeaseDuration time.Duration

	// HeartbeatInterval is how often leases are renewed. Default LeaseDuration/3.
	HeartbeatInterval time.Duration

	// MaintenanceInterval is how often this worker promotes due jobs and reclaims
	// expired leases. It bounds how late a scheduled job starts. Default 500ms.
	MaintenanceInterval time.Duration

	// PollInterval is the fallback fetch rate when no wake message arrives.
	// Default 1s.
	PollInterval time.Duration

	// JobTimeout bounds handlers for jobs that set no timeout. Default 30m.
	JobTimeout time.Duration

	// Backoff is the retry policy for jobs that set none. Default DefaultBackoff.
	Backoff Backoff

	// Retention is how long completed jobs stay inspectable. Negative deletes
	// them immediately. Default 1h.
	Retention time.Duration

	// ShutdownTimeout is how long in-flight handlers may run after shutdown
	// starts. Default 30s.
	ShutdownTimeout time.Duration

	// CancelGrace is how long handlers get to return once their context is
	// cancelled at the end of ShutdownTimeout. Default 5s.
	CancelGrace time.Duration

	// ID names this worker in lease records. Default host-pid-random.
	ID string

	Logger *slog.Logger

	// Registerer receives worker metrics when set.
	Registerer prometheus.Registerer
}

func (cfg *WorkerConfig) applyDefaults() error {
	if err := validateQueue(cfg.Queue); err != nil {
		return err
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	if cfg.HeartbeatInterval >= cfg.LeaseDuration {
		return fmt.Errorf("%w: heartbeat interval must be shorter than the lease", ErrInvalid)
	}
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = 500 * time.Millisecond
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 30 * time.Minute
	}
	if cfg.Backoff == (Backoff{}) {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Retention == 0 {
		cfg.Retention = time.Hour
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.CancelGrace <= 0 {
		cfg.CancelGrace = 5 * time.Second
	}
	if cfg.ID == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "worker"
		}
		cfg.ID = fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.Uint32N(1<<16))
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return nil
}

// Worker pulls jobs from one queue and runs handlers. Run one Worker per queue.
type Worker struct {
	client   *Client
	cfg      WorkerConfig
	log      *slog.Logger
	metrics  *workerMetrics
	handlers map[string]Handler

	slots chan struct{}
	wake  chan struct{}

	mu       sync.Mutex
	inFlight map[string]*flight
	handling sync.WaitGroup
}

// flight tracks one running delivery.
type flight struct {
	job    *Job
	ctx    context.Context
	cancel context.CancelCauseFunc

	// leaseUntil is the local view of the deadline, measured from before the
	// request was sent so it is never later than the Redis deadline. Guarded by
	// Worker.mu.
	leaseUntil time.Time

	returned atomic.Bool // the handler has returned
	claimed  atomic.Bool // the outcome of this delivery has been decided
}

// NewWorker creates a worker. Register handlers before calling Run.
func NewWorker(client *Client, cfg WorkerConfig) (*Worker, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	metrics, err := newWorkerMetrics(cfg.Registerer, cfg.Queue)
	if err != nil {
		return nil, err
	}
	return &Worker{
		client:   client,
		cfg:      cfg,
		log:      cfg.Logger.With("worker", cfg.ID, "queue", cfg.Queue),
		metrics:  metrics,
		handlers: make(map[string]Handler),
		slots:    make(chan struct{}, cfg.Concurrency),
		wake:     make(chan struct{}, 1),
		inFlight: make(map[string]*flight),
	}, nil
}

// Handle registers the handler for a job type.
func (w *Worker) Handle(jobType string, h Handler) {
	w.handlers[jobType] = h
}

// ID returns the worker id used in lease records.
func (w *Worker) ID() string { return w.cfg.ID }

// Run processes jobs until ctx is cancelled, then shuts down gracefully:
//
//  1. It stops fetching.
//  2. It lets running handlers finish for ShutdownTimeout, with heartbeats on.
//  3. It cancels the handlers that remain. They get CancelGrace to return, and a
//     job whose handler returns an error is released without using an attempt.
//  4. It releases the leases of handlers that still have not returned, and
//     returns ErrJobsAbandoned.
//
// Handler contexts are not derived from ctx, so step 2 is a real grace period.
func (w *Worker) Run(ctx context.Context) error {
	background, stopBackground := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Go(func() { w.heartbeatLoop(background) })
	loops.Go(func() { w.maintenanceLoop(background) })
	loops.Go(func() { w.subscribeWake(background) })

	w.log.Info("worker started", "concurrency", w.cfg.Concurrency, "lease", w.cfg.LeaseDuration)
	w.fetchLoop(ctx)
	err := w.drain()

	stopBackground()
	loops.Wait()
	w.log.Info("worker stopped")
	return err
}

func (w *Worker) fetchLoop(ctx context.Context) {
	retry := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return
		case w.slots <- struct{}{}:
		}
		// select picks at random when both cases are ready.
		if ctx.Err() != nil {
			w.freeSlots(1)
			return
		}
		reserved := 1 + w.reserveFreeSlots()

		// The fetch is not tied to ctx. Cancelling it midway could lose the reply
		// after Redis already leased the jobs, and they would sit idle until the
		// lease expired.
		fetchCtx, cancel := context.WithTimeout(context.Background(), w.requestTimeout())
		sent := time.Now()
		jobs, err := w.client.dequeue(fetchCtx, w.cfg.Queue, reserved, w.cfg.LeaseDuration, w.cfg.ID)
		cancel()

		w.freeSlots(reserved - len(jobs))
		if err != nil {
			retry = min(max(2*retry, 50*time.Millisecond), 2*time.Second)
			w.log.Warn("fetch failed", "error", err, "retry_in", retry)
			sleep(ctx, retry)
			continue
		}
		retry = 0

		for _, job := range jobs {
			w.start(job, sent)
		}
		if len(jobs) == 0 {
			w.waitForWork(ctx)
		}
	}
}

func (w *Worker) reserveFreeSlots() int {
	extra := 0
	for {
		select {
		case w.slots <- struct{}{}:
			extra++
		default:
			return extra
		}
	}
}

func (w *Worker) freeSlots(n int) {
	for range n {
		<-w.slots
	}
}

func (w *Worker) waitForWork(ctx context.Context) {
	timer := time.NewTimer(w.cfg.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-w.wake:
	case <-timer.C:
	}
}

func (w *Worker) start(job *Job, sent time.Time) {
	timeout := job.Timeout
	if timeout <= 0 {
		timeout = w.cfg.JobTimeout
	}
	base, cancel := context.WithCancelCause(context.Background())
	ctx, stopTimer := context.WithTimeoutCause(base, timeout, ErrJobTimeout)

	f := &flight{
		job:        job,
		ctx:        ctx,
		cancel:     cancel,
		leaseUntil: sent.Add(w.cfg.LeaseDuration),
	}
	w.mu.Lock()
	w.inFlight[job.ID] = f
	w.mu.Unlock()
	w.metrics.inFlight.Inc()

	w.handling.Go(func() {
		defer func() {
			stopTimer()
			cancel(nil)
			w.mu.Lock()
			delete(w.inFlight, job.ID)
			w.mu.Unlock()
			w.metrics.inFlight.Dec()
			<-w.slots
		}()
		w.process(f)
	})
}

func (w *Worker) process(f *flight) {
	began := time.Now()
	err := w.invoke(f)
	f.returned.Store(true)

	// The handler context has to be read before the deferred cancel in start.
	cause := context.Cause(f.ctx)
	if !f.claimed.CompareAndSwap(false, true) {
		w.log.Warn("handler returned after its lease was released", "job", f.job.ID)
		return
	}
	outcome := w.settle(f, err, cause)
	w.metrics.handled(f.job.Type, outcome, time.Since(began))
}

func (w *Worker) invoke(f *flight) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.metrics.panics.Inc()
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	handler, ok := w.handlers[f.job.Type]
	if !ok {
		return fmt.Errorf("no handler for job type %q", f.job.Type)
	}
	return handler(f.ctx, f.job)
}

// settle records the outcome of a delivery. The scripts check the delivery token,
// so settle does not need to know whether the lease survived: a stale result is
// refused, and a success after a false alarm still counts.
func (w *Worker) settle(f *flight, handlerErr, cause error) string {
	job := f.job
	var op func(context.Context) (string, error)

	switch {
	case handlerErr == nil:
		op = func(ctx context.Context) (string, error) {
			return w.client.complete(ctx, job, max(w.cfg.Retention, 0))
		}
	case errors.Is(cause, ErrShuttingDown):
		op = func(ctx context.Context) (string, error) {
			return w.client.release(ctx, job)
		}
	default:
		delay := w.backoffFor(job).next(job.Attempt)
		permanent := IsPermanent(handlerErr)
		message := handlerErr.Error()
		op = func(ctx context.Context) (string, error) {
			return w.client.fail(ctx, job, message, delay, permanent)
		}
	}

	status, err := w.settleWithRetry(job, op)
	switch {
	case err != nil:
		w.log.Error("giving up on recording the result, the lease will expire",
			"job", job.ID, "error", err)
		return "unrecorded"
	case status == statusLost:
		w.metrics.leaseLost.Inc()
		w.log.Warn("result refused, another delivery owns this job", "job", job.ID, "delivery", job.Delivery)
		return "lease_lost"
	case handlerErr == nil:
		return "completed"
	case errors.Is(cause, ErrShuttingDown):
		return "released"
	case status == statusDead:
		w.log.Warn("job moved to dead letter queue", "job", job.ID, "error", handlerErr)
		return "dead"
	case status == statusRetry:
		return "retried"
	}
	return status
}

// settleWithRetry keeps trying through a Redis outage for one lease period. The
// flight stays registered meanwhile, so heartbeats keep the lease alive as soon
// as Redis is back.
func (w *Worker) settleWithRetry(job *Job, op func(context.Context) (string, error)) (string, error) {
	deadline := time.Now().Add(w.cfg.LeaseDuration)
	wait := 25 * time.Millisecond
	for {
		ctx, cancel := context.WithTimeout(context.Background(), w.requestTimeout())
		status, err := op(ctx)
		cancel()
		if err == nil {
			return status, nil
		}
		if time.Now().After(deadline) {
			return "", err
		}
		w.log.Warn("recording result failed, retrying", "job", job.ID, "error", err)
		time.Sleep(wait)
		wait = min(2*wait, time.Second)
	}
}

func (w *Worker) backoffFor(job *Job) Backoff {
	policy := w.cfg.Backoff
	if job.BackoffBase > 0 {
		policy.Base = job.BackoffBase
		policy.Max = job.BackoffMax
	}
	return policy
}

func (w *Worker) requestTimeout() time.Duration {
	return min(w.cfg.LeaseDuration, 5*time.Second)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.beat()
		}
	}
}

// beat renews every lease this worker should still hold. A handler that blew its
// timeout and keeps running is left to expire, so the job is retried elsewhere.
func (w *Worker) beat() {
	sent := time.Now()
	var flights []*flight
	var jobs []*Job

	w.mu.Lock()
	for _, f := range w.inFlight {
		if sent.After(f.leaseUntil) {
			// No renewal reached Redis in time. Assume another worker has the job
			// and tell the handler to stop.
			w.fence(f, "lease not renewed in time, cancelling handler")
		}
		if !f.returned.Load() && errors.Is(context.Cause(f.ctx), ErrJobTimeout) {
			continue
		}
		flights = append(flights, f)
		jobs = append(jobs, f.job)
	}
	w.mu.Unlock()

	if len(jobs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.requestTimeout())
	defer cancel()
	held, err := w.client.extend(ctx, w.cfg.Queue, jobs, w.cfg.LeaseDuration)
	if err != nil {
		w.log.Warn("heartbeat failed", "error", err)
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for i, f := range flights {
		if held[i] {
			f.leaseUntil = sent.Add(w.cfg.LeaseDuration)
			continue
		}
		w.fence(f, "lease lost, cancelling handler")
	}
}

// fence stops a handler that no longer owns its job. Go cannot kill a goroutine,
// so this is a request: the handler has to observe its context.
func (w *Worker) fence(f *flight, reason string) {
	if f.returned.Load() || f.ctx.Err() != nil {
		return
	}
	f.cancel(ErrLeaseLost)
	w.metrics.leaseLost.Inc()
	w.log.Warn(reason, "job", f.job.ID, "delivery", f.job.Delivery)
}

func (w *Worker) maintenanceLoop(ctx context.Context) {
	retention := max(w.cfg.Retention, 0)
	for {
		// Spread workers out so they do not all run maintenance at the same time.
		jitter := 0.8 + 0.4*rand.Float64()
		sleep(ctx, time.Duration(float64(w.cfg.MaintenanceInterval)*jitter))
		if ctx.Err() != nil {
			return
		}
		for ctx.Err() == nil {
			callCtx, cancel := context.WithTimeout(ctx, w.requestTimeout())
			result, err := w.client.maintain(callCtx, w.cfg.Queue, retention)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					w.log.Warn("maintenance failed", "error", err)
				}
				break
			}
			if result.reclaimed+result.buried > 0 {
				w.log.Info("reclaimed expired leases", "requeued", result.reclaimed, "dead", result.buried)
			}
			if !result.full() {
				break
			}
		}
	}
}

// subscribeWake turns enqueue notifications into fetches. It only lowers latency:
// a lost message costs at most PollInterval.
func (w *Worker) subscribeWake(ctx context.Context) {
	keys := newQueueKeys(w.client.namespace, w.cfg.Queue)
	sub := w.client.rdb.Subscribe(ctx, keys.wake)
	defer func() { _ = sub.Close() }()
	messages := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-messages:
			if !ok {
				return
			}
			select {
			case w.wake <- struct{}{}:
			default:
			}
		}
	}
}

// drain implements steps 2 to 4 of the shutdown described on Run.
func (w *Worker) drain() error {
	finished := make(chan struct{})
	go func() {
		w.handling.Wait()
		close(finished)
	}()

	if count := w.inFlightCount(); count > 0 {
		w.log.Info("waiting for handlers to finish", "in_flight", count, "timeout", w.cfg.ShutdownTimeout)
	}
	if waitFor(finished, w.cfg.ShutdownTimeout) {
		return nil
	}

	w.log.Warn("shutdown timeout reached, cancelling handlers", "in_flight", w.inFlightCount())
	for _, f := range w.snapshot() {
		f.cancel(ErrShuttingDown)
	}
	if waitFor(finished, w.cfg.CancelGrace) {
		return nil
	}

	abandoned := 0
	for _, f := range w.snapshot() {
		if !f.claimed.CompareAndSwap(false, true) {
			continue // the handler returned and its result is being recorded
		}
		abandoned++
		ctx, cancel := context.WithTimeout(context.Background(), w.requestTimeout())
		if _, err := w.client.release(ctx, f.job); err != nil {
			w.log.Error("could not release job, its lease will expire", "job", f.job.ID, "error", err)
		}
		cancel()
	}
	if abandoned == 0 {
		<-finished
		return nil
	}
	w.log.Error("handlers ignored cancellation", "abandoned", abandoned)
	return fmt.Errorf("%w: %d", ErrJobsAbandoned, abandoned)
}

func (w *Worker) snapshot() []*flight {
	w.mu.Lock()
	defer w.mu.Unlock()
	flights := make([]*flight, 0, len(w.inFlight))
	for _, f := range w.inFlight {
		flights = append(flights, f)
	}
	return flights
}

func (w *Worker) inFlightCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inFlight)
}

func waitFor(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
