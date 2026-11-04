package relayq

import "errors"

// Errors returned by the client and passed to handlers.
var (
	ErrNotFound     = errors.New("relayq: job not found")
	ErrJobActive    = errors.New("relayq: job is leased to a worker")
	ErrInvalid      = errors.New("relayq: invalid argument")
	ErrLeaseLost    = errors.New("relayq: lease lost")
	ErrJobTimeout   = errors.New("relayq: job timeout exceeded")
	ErrShuttingDown = errors.New("relayq: worker shutting down")

	// ErrJobsAbandoned is returned by Worker.Run when handlers ignored
	// cancellation for the whole shutdown budget. Their leases were released,
	// so those jobs can run again while the old handlers are still going.
	ErrJobsAbandoned = errors.New("relayq: handlers still running after shutdown")
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks a handler error as not worth retrying. The job goes straight
// to the dead letter queue with attempts left.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was wrapped by Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
