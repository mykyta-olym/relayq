package relayq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestBackoffCeilingDoublesAndCaps(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second}
	want := []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, time.Second, time.Second,
	}
	for i, expected := range want {
		if got := b.Ceiling(i + 1); got != expected {
			t.Errorf("attempt %d: got %v, want %v", i+1, got, expected)
		}
	}
}

func TestBackoffDoesNotOverflowOnHugeAttempt(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Hour}
	for _, attempt := range []int{64, 1000, math.MaxInt32} {
		if got := b.Ceiling(attempt); got != time.Hour {
			t.Errorf("attempt %d: got %v, want the cap", attempt, got)
		}
	}
}

func TestBackoffMaxBelowBaseIsCapped(t *testing.T) {
	b := Backoff{Base: time.Minute, Max: time.Second}
	if got := b.Ceiling(1); got != time.Second {
		t.Errorf("got %v, want the cap", got)
	}
}

func TestBackoffJitterStaysInBounds(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute, Jitter: 0.5}
	if got := b.Delay(1, 0); got != time.Second {
		t.Errorf("sample 0: got %v, want the ceiling", got)
	}
	if got := b.Delay(1, 0.999999); got < 500*time.Millisecond || got >= 501*time.Millisecond {
		t.Errorf("sample near 1: got %v, want about half the ceiling", got)
	}
	seen := map[time.Duration]bool{}
	for range 1000 {
		d := b.next(3)
		if d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("delay %v outside [2s, 4s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 100 {
		t.Errorf("only %d distinct delays in 1000 draws, jitter looks broken", len(seen))
	}
}

func TestBackoffWithoutJitterIsExact(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	if got := b.next(2); got != 2*time.Second {
		t.Errorf("got %v", got)
	}
}

func TestSpecValidation(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
		ok   bool
	}{
		{"defaults", nil, true},
		{"lowest priority", []Option{Priority(0)}, true},
		{"highest priority", []Option{Priority(9)}, true},
		{"priority too low", []Option{Priority(-1)}, false},
		{"priority too high", []Option{Priority(10)}, false},
		{"zero attempts", []Option{MaxAttempts(0)}, false},
		{"negative delay", []Option{Delay(-time.Second)}, false},
		{"negative timeout", []Option{Timeout(-time.Second)}, false},
		{"negative backoff", []Option{WithBackoff(-1, time.Second)}, false},
		{"backoff max below base", []Option{WithBackoff(time.Minute, time.Second)}, false},
		{"delay and run at", []Option{Delay(time.Second), RunAt(time.Now())}, false},
		{"long idempotency key", []Option{IdempotencyKey(string(make([]byte, 256)))}, false},
		{"tiny idempotency ttl", []Option{IdempotencyTTL(time.Microsecond)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newSpec(tc.opts)
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestQueueNamesCannotInjectKeys(t *testing.T) {
	for _, name := range []string{"", "a:b", "a b", "a/b", "a*", "{a}", string(make([]byte, 65))} {
		if err := validateQueue(name); !errors.Is(err, ErrInvalid) {
			t.Errorf("queue %q: got %v, want ErrInvalid", name, err)
		}
	}
	for _, name := range []string{"a", "emails", "tenant-1.high_priority"} {
		if err := validateQueue(name); err != nil {
			t.Errorf("queue %q: %v", name, err)
		}
	}
}

func TestPermanentWrapping(t *testing.T) {
	base := errors.New("bad input")
	wrapped := fmt.Errorf("handler: %w", Permanent(base))
	if !IsPermanent(wrapped) {
		t.Error("wrapped permanent error not detected")
	}
	if !errors.Is(wrapped, base) {
		t.Error("Permanent hides the cause")
	}
	if IsPermanent(base) {
		t.Error("plain error reported as permanent")
	}
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must stay nil")
	}
}

func TestParseJobRoundTrip(t *testing.T) {
	job, err := parseJob([]any{
		"id", "abc", "queue", "q", "type", "t", "payload", "\x00\xff",
		"priority", "7", "attempts", "2", "max_attempts", "5", "delivery", "9",
		"timeout_ms", "1500", "state", "active", "created_at", "1700000000123",
		"lease_until", "1700000000456",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Priority != 7 || job.Attempt != 2 || job.MaxAttempts != 5 || job.Delivery != 9 {
		t.Errorf("numbers wrong: %+v", job)
	}
	if string(job.Payload) != "\x00\xff" {
		t.Errorf("payload not preserved: %q", job.Payload)
	}
	if job.Timeout != 1500*time.Millisecond {
		t.Errorf("timeout: %v", job.Timeout)
	}
	if job.CreatedAt.UnixMilli() != 1700000000123 || job.LeaseUntil.UnixMilli() != 1700000000456 {
		t.Errorf("times wrong: %+v", job)
	}
	if job.StartedAt != nil {
		t.Errorf("absent timestamp should stay nil, got %v", job.StartedAt)
	}
}

func TestParseJobRejectsCorruptData(t *testing.T) {
	if _, err := parseJob([]any{"id", "abc", "priority", "not-a-number"}); err == nil {
		t.Error("corrupt number accepted")
	}
	if _, err := parseJob([]any{"queue", "q"}); err == nil {
		t.Error("hash without id accepted")
	}
	if _, err := parseJob([]any{"id", int64(5)}); err == nil {
		t.Error("non string field accepted")
	}
}

func TestDecode(t *testing.T) {
	job := &Job{ID: "x", Payload: []byte(`{"n":3}`)}
	var v struct{ N int }
	if err := job.Decode(&v); err != nil || v.N != 3 {
		t.Fatalf("decode: %v %+v", err, v)
	}
	job.Payload = []byte("{")
	if err := job.Decode(&v); err == nil {
		t.Fatal("bad JSON accepted")
	}
}

func TestWorkerConfigDefaults(t *testing.T) {
	cfg := WorkerConfig{Queue: "q"}
	if err := cfg.applyDefaults(); err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != cfg.LeaseDuration/3 {
		t.Errorf("heartbeat %v for lease %v", cfg.HeartbeatInterval, cfg.LeaseDuration)
	}
	if cfg.Concurrency != 10 || cfg.ID == "" || cfg.Backoff != DefaultBackoff {
		t.Errorf("defaults missing: %+v", cfg)
	}
}

func TestWorkerConfigRejectsHeartbeatSlowerThanLease(t *testing.T) {
	cfg := WorkerConfig{Queue: "q", LeaseDuration: time.Second, HeartbeatInterval: time.Second}
	if err := cfg.applyDefaults(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestWorkerConfigRejectsBadQueue(t *testing.T) {
	cfg := WorkerConfig{Queue: "a:b"}
	if err := cfg.applyDefaults(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestEnqueueValidatesBeforeTouchingRedis(t *testing.T) {
	// The nil client would panic if validation let the call through.
	c := NewClient(nil)
	ctx := context.Background()
	cases := []func() error{
		func() error { _, err := c.Enqueue(ctx, "bad:queue", "t", nil); return err },
		func() error { _, err := c.Enqueue(ctx, "q", "", nil); return err },
		func() error { _, err := c.Enqueue(ctx, "q", "t", nil, Priority(99)); return err },
		func() error { _, err := c.EnqueueBatch(ctx, "q", nil); return err },
		func() error { _, err := c.EnqueueBatch(ctx, "q", make([]BatchItem, MaxBatchSize+1)); return err },
		func() error { _, err := c.EnqueueJSON(ctx, "q", "t", make(chan int)); return err },
		func() error { return c.RequeueDead(ctx, "q", "") },
	}
	for i, call := range cases {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: got %v, want ErrInvalid", i, err)
		}
	}
}

func TestLuaScriptsLoad(t *testing.T) {
	for name, script := range map[string]string{
		"enqueue": scriptEnqueue.Hash(), "dequeue": scriptDequeue.Hash(),
		"complete": scriptComplete.Hash(), "fail": scriptFail.Hash(),
		"release": scriptRelease.Hash(), "extend": scriptExtend.Hash(),
		"maintain": scriptMaintain.Hash(), "requeue": scriptRequeue.Hash(),
		"delete": scriptDelete.Hash(),
	} {
		if script == "" {
			t.Errorf("script %s has no hash", name)
		}
	}
}
