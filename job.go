package relayq

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// State is where a job currently sits.
type State string

// Job states. A job is in exactly one of them, and each state has one sorted
// set in Redis, so moving a job is a single atomic script.
const (
	StateReady     State = "ready"     // waiting for a worker
	StateDelayed   State = "delayed"   // scheduled, delayed or waiting out a retry backoff
	StateActive    State = "active"    // leased to a worker
	StateDead      State = "dead"      // out of attempts, kept for inspection and requeue
	StateCompleted State = "completed" // finished, kept until retention expires
)

// States lists every job state in display order.
var States = []State{StateReady, StateDelayed, StateActive, StateDead, StateCompleted}

// Job is a unit of work. Handlers receive one per delivery.
type Job struct {
	ID       string `json:"id"`
	Queue    string `json:"queue"`
	Type     string `json:"type"`
	Payload  []byte `json:"-"`
	Priority int    `json:"priority"`
	State    State  `json:"state"`

	// Attempt counts deliveries against MaxAttempts, including the current one.
	// It is refunded when a job is released at shutdown and reset on requeue.
	Attempt     int `json:"attempt"`
	MaxAttempts int `json:"max_attempts"`

	// Delivery rises by one on every delivery and is never reset or refunded.
	// It is the fencing token: a side effect stamped with it can be rejected
	// downstream when a newer delivery has already happened.
	Delivery int64 `json:"delivery"`

	Timeout        time.Duration `json:"-"`
	BackoffBase    time.Duration `json:"-"`
	BackoffMax     time.Duration `json:"-"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	RunAt       time.Time  `json:"run_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	FailedAt    *time.Time `json:"failed_at,omitempty"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
}

// Decode unmarshals a JSON payload into v.
func (j *Job) Decode(v any) error {
	if err := json.Unmarshal(j.Payload, v); err != nil {
		return fmt.Errorf("decode payload of job %s: %w", j.ID, err)
	}
	return nil
}

// parseJob builds a Job from the flat field list that HGETALL returns inside a
// script.
func parseJob(flat []any) (*Job, error) {
	fields := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		name, ok := flat[i].(string)
		value, ok2 := flat[i+1].(string)
		if !ok || !ok2 {
			return nil, fmt.Errorf("job field %d is not a string", i)
		}
		fields[name] = value
	}
	return jobFromFields(fields)
}

func jobFromFields(f map[string]string) (*Job, error) {
	if f["id"] == "" {
		return nil, fmt.Errorf("job hash has no id")
	}
	var p fieldParser
	job := &Job{
		ID:             f["id"],
		Queue:          f["queue"],
		Type:           f["type"],
		Payload:        []byte(f["payload"]),
		Priority:       int(p.integer(f, "priority")),
		State:          State(f["state"]),
		Attempt:        int(p.integer(f, "attempts")),
		MaxAttempts:    int(p.integer(f, "max_attempts")),
		Delivery:       p.integer(f, "delivery"),
		Timeout:        p.millis(f, "timeout_ms"),
		BackoffBase:    p.millis(f, "backoff_base_ms"),
		BackoffMax:     p.millis(f, "backoff_max_ms"),
		IdempotencyKey: f["idempotency_key"],
		LeaseOwner:     f["lease_owner"],
		LastError:      f["last_error"],
	}
	if t := p.timestamp(f, "created_at"); t != nil {
		job.CreatedAt = *t
	}
	if t := p.timestamp(f, "run_at"); t != nil {
		job.RunAt = *t
	}
	job.StartedAt = p.timestamp(f, "started_at")
	job.CompletedAt = p.timestamp(f, "completed_at")
	job.FailedAt = p.timestamp(f, "failed_at")
	job.LeaseUntil = p.timestamp(f, "lease_until")
	if p.err != nil {
		return nil, fmt.Errorf("job %s: %w", job.ID, p.err)
	}
	return job, nil
}

// fieldParser keeps the first error so jobFromFields can read like a table.
type fieldParser struct{ err error }

func (p *fieldParser) integer(f map[string]string, name string) int64 {
	raw, ok := f[name]
	if !ok || raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("field %s: %w", name, err)
	}
	return n
}

func (p *fieldParser) millis(f map[string]string, name string) time.Duration {
	return time.Duration(p.integer(f, name)) * time.Millisecond
}

func (p *fieldParser) timestamp(f map[string]string, name string) *time.Time {
	ms := p.integer(f, name)
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}
