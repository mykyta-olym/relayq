package relayq

import (
	"fmt"
	"time"
)

// Limits and defaults for enqueued jobs.
const (
	MinPriority           = 0
	MaxPriority           = 9
	DefaultPriority       = 5
	DefaultMaxAttempts    = 5
	DefaultIdempotencyTTL = 24 * time.Hour
	MaxBatchSize          = 1000

	maxTypeLength           = 128
	maxIdempotencyKeyLength = 255
)

// Option changes how a job is enqueued.
type Option func(*enqueueSpec)

type enqueueSpec struct {
	priority       int
	maxAttempts    int
	timeout        time.Duration
	backoffBase    time.Duration
	backoffMax     time.Duration
	delay          time.Duration
	runAt          time.Time
	idempotencyKey string
	idempotencyTTL time.Duration
}

// Priority sets the job priority, 0 to 9. Higher runs first.
func Priority(p int) Option { return func(s *enqueueSpec) { s.priority = p } }

// MaxAttempts sets how many deliveries the job gets before it goes to the dead
// letter queue. 1 means no retries.
func MaxAttempts(n int) Option { return func(s *enqueueSpec) { s.maxAttempts = n } }

// Timeout bounds one handler run. Zero uses the worker default.
func Timeout(d time.Duration) Option { return func(s *enqueueSpec) { s.timeout = d } }

// WithBackoff sets the retry base and cap for this job. Zero uses the worker
// policy. Jitter always comes from the worker.
func WithBackoff(base, maxDelay time.Duration) Option {
	return func(s *enqueueSpec) { s.backoffBase, s.backoffMax = base, maxDelay }
}

// Delay runs the job no sooner than d from now, measured on the Redis clock.
func Delay(d time.Duration) Option { return func(s *enqueueSpec) { s.delay = d } }

// RunAt runs the job no sooner than t. The time comes from the caller, so a
// caller with a skewed clock schedules early or late by that skew.
func RunAt(t time.Time) Option { return func(s *enqueueSpec) { s.runAt = t } }

// IdempotencyKey makes enqueue safe to repeat. Calls with the same key on the
// same queue within the TTL create one job. It deduplicates enqueues, not
// executions: the job can still run more than once.
func IdempotencyKey(key string) Option { return func(s *enqueueSpec) { s.idempotencyKey = key } }

// IdempotencyTTL sets how long the key is remembered. The default is 24 hours.
func IdempotencyTTL(d time.Duration) Option { return func(s *enqueueSpec) { s.idempotencyTTL = d } }

func newSpec(opts []Option) (*enqueueSpec, error) {
	spec := &enqueueSpec{
		priority:       DefaultPriority,
		maxAttempts:    DefaultMaxAttempts,
		idempotencyTTL: DefaultIdempotencyTTL,
	}
	for _, opt := range opts {
		opt(spec)
	}
	return spec, spec.validate()
}

func (s *enqueueSpec) validate() error {
	switch {
	case s.priority < MinPriority || s.priority > MaxPriority:
		return fmt.Errorf("%w: priority must be %d to %d", ErrInvalid, MinPriority, MaxPriority)
	case s.maxAttempts < 1:
		return fmt.Errorf("%w: max attempts must be at least 1", ErrInvalid)
	case s.timeout < 0:
		return fmt.Errorf("%w: timeout must not be negative", ErrInvalid)
	case s.backoffBase < 0 || s.backoffMax < 0:
		return fmt.Errorf("%w: backoff must not be negative", ErrInvalid)
	case s.backoffBase > 0 && s.backoffMax < s.backoffBase:
		return fmt.Errorf("%w: backoff max must not be below base", ErrInvalid)
	case s.delay < 0:
		return fmt.Errorf("%w: delay must not be negative", ErrInvalid)
	case s.delay > 0 && !s.runAt.IsZero():
		return fmt.Errorf("%w: delay and run-at are mutually exclusive", ErrInvalid)
	case len(s.idempotencyKey) > maxIdempotencyKeyLength:
		return fmt.Errorf("%w: idempotency key longer than %d bytes", ErrInvalid, maxIdempotencyKeyLength)
	case s.idempotencyTTL < time.Millisecond:
		return fmt.Errorf("%w: idempotency TTL must be at least 1ms", ErrInvalid)
	}
	return nil
}

func validateType(jobType string) error {
	if jobType == "" || len(jobType) > maxTypeLength {
		return fmt.Errorf("%w: job type must be 1 to %d bytes", ErrInvalid, maxTypeLength)
	}
	return nil
}
