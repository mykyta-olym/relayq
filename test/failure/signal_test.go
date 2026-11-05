//go:build failure && !windows

package failure

import (
	"syscall"
	"testing"
	"time"

	"github.com/mykyta-olym/relayq"
)

// These tests send a real SIGTERM, which Windows does not have. They run on Linux
// in CI and in the container command from the README.

func TestSIGTERMFinishesTheRunningJob(t *testing.T) {
	l := newLab(t, true, "--appendonly", "no", "--save", "")
	worker := l.spawn("-lease", "2s", "-shutdown-timeout", "10s")
	id := l.enqueue("work", `{"ms":1500}`)
	l.waitFor(10*time.Second, "handler to start", func() bool { return l.count("start", id) == 1 })
	queued := l.enqueue("work", `{"ms":0}`, relayq.Priority(0))

	if err := worker.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := worker.wait(); err != nil {
		t.Fatalf("worker exit: %v\n%s", err, worker.stderr)
	}

	job := l.job(id)
	if job.State != relayq.StateCompleted || job.Delivery != 1 {
		t.Fatalf("job %+v", job)
	}
	if got := l.count("finish", id); got != 1 {
		t.Fatalf("finished %d times", got)
	}
	if next := l.job(queued); next.Delivery > 1 {
		t.Fatalf("queued job %+v", next)
	}
	t.Logf("RESULT SIGTERM: running job %s finished on its first delivery, worker exited 0", id)
}

func TestSIGTERMReleasesAJobThatCannotFinish(t *testing.T) {
	l := newLab(t, true, "--appendonly", "no", "--save", "")
	worker := l.spawn("-lease", "30s", "-shutdown-timeout", "300ms")
	id := l.enqueue("hold-first", "{}")
	l.waitFor(10*time.Second, "handler to start", func() bool { return l.count("start", id) == 1 })

	began := time.Now()
	if err := worker.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := worker.wait(); err != nil {
		t.Fatalf("worker exit: %v\n%s", err, worker.stderr)
	}

	// With a 30s lease, the job can only be back this early because the worker
	// released it.
	job := l.job(id)
	if job.State != relayq.StateReady || job.Attempt != 0 {
		t.Fatalf("job %+v", job)
	}
	if time.Since(began) > 10*time.Second {
		t.Fatalf("shutdown took %v", time.Since(began))
	}

	second := l.spawn("-lease", "30s")
	l.waitState(10*time.Second, id, relayq.StateCompleted)
	second.kill()
	if done := l.job(id); done.Attempt != 1 || done.Delivery != 2 {
		t.Fatalf("job %+v", done)
	}
	t.Logf("RESULT SIGTERM: job %s released with its attempt refunded, no wait for the 30s lease", id)
}
