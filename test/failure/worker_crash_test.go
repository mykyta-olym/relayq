//go:build failure

package failure

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/mykyta-olym/relayq"
)

// A worker is killed while its handler is running. The job must stay hidden until
// the lease runs out, then come back to another worker with the same id.
func TestKilledWorkerJobIsRedelivered(t *testing.T) {
	l := newLab(t, false, "--appendonly", "no", "--save", "")
	const lease = 2 * time.Second

	first := l.spawn("-lease", lease.String())
	id := l.enqueue("hold-first", "{}")
	l.waitFor(10*time.Second, "handler to start", func() bool { return l.count("start", id) == 1 })

	before := l.job(id)
	if before.State != relayq.StateActive || before.LeaseOwner == "" {
		t.Fatalf("job should be leased: %+v", before)
	}
	killedAt := time.Now()
	first.kill()
	t.Logf("killed worker %s while it ran job %s, lease until %s",
		before.LeaseOwner, id, before.LeaseUntil.Format(time.RFC3339Nano))

	// The dead worker cannot ack or release, so the job stays active and no one
	// else may take it yet.
	after := l.job(id)
	if after.State != relayq.StateActive || after.Delivery != 1 {
		t.Fatalf("job moved before its lease expired: %+v", after)
	}

	second := l.spawn("-lease", lease.String())
	l.waitState(20*time.Second, id, relayq.StateCompleted)
	redelivered := time.Since(killedAt)
	second.kill()

	job := l.job(id)
	if job.Attempt != 2 || job.Delivery != 2 {
		t.Fatalf("want second delivery, got %+v", job)
	}
	if !strings.Contains(job.LastError, "lease expired") {
		t.Fatalf("last error %q", job.LastError)
	}
	if got := l.count("start", id); got != 2 {
		t.Fatalf("%d handler starts, want 2", got)
	}
	if got := l.count("finish", id); got != 1 {
		t.Fatalf("%d handler finishes, want 1", got)
	}
	if redelivered < time.Until(*before.LeaseUntil) {
		t.Fatalf("redelivered before the lease could expire")
	}
	stats := l.stats()
	if stats.Counters["redelivered"] != 1 {
		t.Fatalf("stats %+v", stats.Counters)
	}
	t.Logf("RESULT job %s redelivered and completed %v after the kill, deliveries=%d, handler starts=2, finishes=1",
		id, redelivered.Round(time.Millisecond), job.Delivery)
}

// The side effect happens, then the worker dies before the ack. relayq cannot tell
// this apart from a crash before the side effect, so it must run the job again.
// This test documents the duplicate that at-least-once delivery implies.
func TestCrashBetweenSideEffectAndAckDuplicatesTheEffect(t *testing.T) {
	l := newLab(t, false, "--appendonly", "no", "--save", "")
	worker := l.spawn("-lease", "1s")
	id := l.enqueue("effect-then-crash", "{}")
	l.waitFor(10*time.Second, "worker to crash itself", func() bool { return l.count("effect", id) == 1 })
	<-waitChan(worker)

	l.spawn("-lease", "1s")
	l.waitState(20*time.Second, id, relayq.StateCompleted)
	if got := l.count("effect", id); got != 2 {
		t.Fatalf("side effect ran %d times, this test expects the documented duplicate", got)
	}
	t.Logf("RESULT side effect of job %s ran 2 times, as documented", id)
}

// A job that kills every worker that touches it must not take the fleet down
// forever. Attempts are counted at delivery, so it runs out.
func TestPoisonJobEndsInDeadLetterQueue(t *testing.T) {
	l := newLab(t, false, "--appendonly", "no", "--save", "")
	id := l.enqueue("poison", "{}", relayq.MaxAttempts(3))

	l.waitFor(60*time.Second, "poison job to reach the dead letter queue", func() bool {
		job, err := l.client.Job(l.ctx(), queue, id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == relayq.StateDead {
			return true
		}
		// Keep one worker alive, as a supervisor would.
		if p := l.liveProc(); p == nil {
			l.spawn("-lease", "500ms")
		}
		return false
	})

	if got := l.count("start", id); got != 3 {
		t.Fatalf("handler started %d times, want 3", got)
	}
	job := l.job(id)
	if job.Attempt != 3 || !strings.Contains(job.LastError, "lease expired") {
		t.Fatalf("job %+v", job)
	}
	t.Logf("RESULT poison job killed the worker on all %d deliveries, then stopped in the dead letter queue",
		job.Attempt)
}

func (l *lab) liveProc() *proc {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.procs {
		select {
		case err := <-p.done:
			p.done <- err
		default:
			return p
		}
	}
	return nil
}

// Workers are killed at random while jobs flow through. The evidence files have
// to show that every accepted job finished at least once.
func TestChaosKillingWorkersLosesNoJob(t *testing.T) {
	l := newLab(t, false, "--appendonly", "no", "--save", "")
	const total, fleet = 5000, 6
	flags := []string{"-lease", "1s", "-concurrency", "16"}

	workers := make([]*proc, fleet)
	for i := range workers {
		workers[i] = l.spawn(flags...)
	}

	ids := make(map[string]bool, total)
	for sent := 0; sent < total; sent += 100 {
		items := make([]relayq.BatchItem, 100)
		for i := range items {
			// Attempts are generous because this test is about loss. A job hit by
			// many kills must not stop at the dead letter queue and pass as lost.
			items[i] = relayq.BatchItem{
				Type:    "work",
				Payload: fmt.Appendf(nil, `{"ms":%d}`, rand.IntN(20)),
				Options: []relayq.Option{relayq.MaxAttempts(100)},
			}
		}
		out, err := l.client.EnqueueBatch(l.ctx(), queue, items)
		if err != nil {
			t.Fatal(err)
		}
		for _, res := range out {
			ids[res.ID] = true
		}
	}

	kills := 0
	for l.stats().Counters["completed"] < total*8/10 {
		time.Sleep(150 * time.Millisecond)
		victim := rand.IntN(fleet)
		workers[victim].kill()
		kills++
		workers[victim] = l.spawn(flags...)
	}
	l.waitFor(120*time.Second, "every job to complete", func() bool {
		return l.stats().Counters["completed"] == total
	})

	finished := l.finishCounts()
	duplicates := 0
	for id := range ids {
		switch n := finished[id]; {
		case n == 0:
			t.Errorf("job %s never finished", id)
		case n > 1:
			duplicates += n - 1
		}
	}
	stats := l.stats()
	if stats.Ready != 0 || stats.Active != 0 || stats.Delayed != 0 || stats.Dead != 0 {
		t.Fatalf("leftovers: %+v", stats)
	}
	if kills == 0 {
		t.Fatal("no worker was killed, so this run proved nothing")
	}
	if stats.Counters["redelivered"] == 0 {
		t.Fatal("no job was redelivered, so no kill landed mid job")
	}
	t.Logf("RESULT %d jobs, %d workers killed, %d leases expired and redelivered, 0 lost, "+
		"%d duplicate handler finishes (%.2f%%)", total, kills, stats.Counters["redelivered"],
		duplicates, 100*float64(duplicates)/total)
}
