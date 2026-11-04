package relayq

import (
	"math/rand/v2"
	"time"
)

// Backoff computes the wait before a retry. The ceiling doubles from Base with
// each attempt up to Max. Jitter is the fraction of the ceiling that is
// randomised, so with Jitter 0.5 the delay lands between half the ceiling and
// the ceiling. Randomising the delay stops jobs that failed together from
// retrying together.
type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	Jitter float64
}

// DefaultBackoff is used when neither the job nor the worker sets a policy.
var DefaultBackoff = Backoff{Base: time.Second, Max: 5 * time.Minute, Jitter: 0.5}

// Ceiling returns the delay for an attempt before jitter. Attempt 1 is the first
// delivery. The loop stops at Max, so a huge attempt number cannot overflow.
func (b Backoff) Ceiling(attempt int) time.Duration {
	delay := b.Base
	for i := 1; i < attempt && delay < b.Max; i++ {
		delay *= 2
	}
	return min(delay, b.Max)
}

// Delay returns a jittered delay for an attempt. Sample must be in [0, 1).
func (b Backoff) Delay(attempt int, sample float64) time.Duration {
	ceiling := b.Ceiling(attempt)
	return ceiling - time.Duration(float64(ceiling)*b.Jitter*sample)
}

func (b Backoff) next(attempt int) time.Duration {
	return b.Delay(attempt, rand.Float64())
}
