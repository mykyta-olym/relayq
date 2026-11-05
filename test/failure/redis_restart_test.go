//go:build failure

package failure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mykyta-olym/relayq"
)

// seeded holds job ids for every state, so each state can be counted after the
// restart.
type seeded struct {
	ready, delayed, active, dead, completed []string
	idempotencyKeys                         []string
}

const (
	readyCount     = 200
	delayedCount   = 100
	activeCount    = 20
	deadCount      = 20
	completedCount = 20
)

// seed drives one queue into a mix of every state. No worker is alive when it
// returns. Active jobs hold leases from a worker that was killed. beforeReady runs
// just before the ready jobs are written, so a snapshot can be placed there.
func (l *lab) seed(beforeReady func()) seeded {
	l.t.Helper()
	var s seeded
	worker := l.spawn("-lease", "3s", "-concurrency", "32")

	for range completedCount {
		s.completed = append(s.completed, l.enqueue("work", `{"ms":0}`))
	}
	for range deadCount {
		s.dead = append(s.dead, l.enqueue("fail", "{}"))
	}
	l.waitFor(30*time.Second, "completed and dead jobs", func() bool {
		stats := l.stats()
		return stats.Completed == completedCount && stats.Dead == deadCount
	})

	for range activeCount {
		s.active = append(s.active, l.enqueue("hold-first", "{}"))
	}
	l.waitFor(30*time.Second, "jobs to be leased", func() bool { return l.stats().Active == activeCount })
	worker.kill()

	for i := range delayedCount {
		s.delayed = append(s.delayed, l.enqueue("work", `{"ms":0}`, relayq.Delay(time.Hour+time.Duration(i)*time.Second)))
	}
	for i := range 10 {
		key := fmt.Sprintf("order-%d", i)
		s.idempotencyKeys = append(s.idempotencyKeys, key)
		s.delayed = append(s.delayed, l.enqueue("work", `{"ms":0}`, relayq.Delay(time.Hour), relayq.IdempotencyKey(key)))
	}

	beforeReady()
	for range readyCount {
		s.ready = append(s.ready, l.enqueue("work", `{"ms":0}`))
	}
	return s
}

// survivors counts ids that still exist in the state they were left in.
func (l *lab) survivors(ids []string, state relayq.State) int {
	l.t.Helper()
	n := 0
	for _, id := range ids {
		job, err := l.client.Job(l.ctx(), queue, id)
		if err == nil && job.State == state {
			n++
		} else if err != nil && !errors.Is(err, relayq.ErrNotFound) {
			l.t.Fatal(err)
		}
	}
	return n
}

func (l *lab) keysStillDeduplicate(keys []string) int {
	l.t.Helper()
	n := 0
	for _, key := range keys {
		res, err := l.client.Enqueue(l.ctx(), queue, "work", []byte(`{"ms":0}`),
			relayq.Delay(time.Hour), relayq.IdempotencyKey(key))
		if err != nil {
			l.t.Fatal(err)
		}
		if res.Duplicate {
			n++
		} else if err := l.client.Delete(l.ctx(), queue, res.ID); err != nil {
			l.t.Fatal(err)
		}
	}
	return n
}

type survival struct {
	ready, delayed, active, dead, completed, idempotency int
}

func (s survival) String() string {
	return fmt.Sprintf("ready %d/%d, delayed %d/%d, active %d/%d, dead %d/%d, completed %d/%d, idempotency keys %d/10",
		s.ready, readyCount, s.delayed, delayedCount+10, s.active, activeCount,
		s.dead, deadCount, s.completed, completedCount, s.idempotency)
}

var everything = survival{
	ready: readyCount, delayed: delayedCount + 10, active: activeCount,
	dead: deadCount, completed: completedCount, idempotency: 10,
}

// Each case restarts a Redis holding jobs in every state. Cases with a fixed
// expectation fail if the result changes. "aof-everysec sigkill" only reports,
// because its loss depends on timing that Redis does not promise.
func TestWhatSurvivesARedisRestart(t *testing.T) {
	aofAlways := []string{"--appendonly", "yes", "--appendfsync", "always", "--save", ""}
	aofEverysec := []string{"--appendonly", "yes", "--appendfsync", "everysec", "--save", ""}
	rdb := []string{"--appendonly", "no", "--save", "3600", "1"}
	none := []string{"--appendonly", "no", "--save", ""}

	afterSnapshot := everything
	afterSnapshot.ready = 0

	cases := []struct {
		name     string
		args     []string
		crash    bool
		snapshot bool
		want     *survival
	}{
		{name: "aof-always sigterm", args: aofAlways, want: &everything},
		{name: "aof-always sigkill", args: aofAlways, crash: true, want: &everything},
		{name: "aof-everysec sigterm", args: aofEverysec, want: &everything},
		{name: "aof-everysec sigkill", args: aofEverysec, crash: true},
		{name: "rdb sigterm", args: rdb, want: &everything},
		{name: "rdb sigkill", args: rdb, crash: true, snapshot: true, want: &afterSnapshot},
		{name: "no persistence sigterm", args: none, want: &survival{}},
		{name: "no persistence sigkill", args: none, crash: true, want: &survival{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLab(t, false, tc.args...)
			s := l.seed(func() {
				if tc.snapshot {
					if err := l.rdb.Save(l.ctx()).Err(); err != nil {
						t.Fatal(err)
					}
				}
			})

			restart := l.redis.GracefulRestart
			if tc.crash {
				restart = l.redis.CrashRestart
			}
			began := time.Now()
			if err := restart(); err != nil {
				t.Fatal(err)
			}
			took := time.Since(began).Round(time.Millisecond)

			got := survival{
				ready:       l.survivors(s.ready, relayq.StateReady),
				delayed:     l.survivors(s.delayed, relayq.StateDelayed),
				active:      l.survivors(s.active, relayq.StateActive),
				dead:        l.survivors(s.dead, relayq.StateDead),
				completed:   l.survivors(s.completed, relayq.StateCompleted),
				idempotency: l.keysStillDeduplicate(s.idempotencyKeys),
			}
			t.Logf("RESULT %-24s back in %-8v %s", tc.name, took, got)
			if tc.want != nil && got != *tc.want {
				t.Fatalf("survival changed:\n got %s\nwant %s", got, *tc.want)
			}
			if got == (survival{}) {
				return
			}

			// Survivors have to be usable, not just present: ready jobs run, leases
			// left by the dead worker expire and redeliver, the rest stay put.
			l.spawn("-lease", "1s", "-concurrency", "32")
			l.waitFor(45*time.Second, "ready and active jobs to drain", func() bool {
				stats := l.stats()
				return stats.Ready == 0 && stats.Active == 0
			})
			stats := l.stats()
			if int(stats.Delayed) != got.delayed || int(stats.Dead) != got.dead {
				t.Fatalf("delayed or dead changed during drain: %+v", stats)
			}
			for _, id := range s.active {
				if job, err := l.client.Job(l.ctx(), queue, id); err == nil && job.State != relayq.StateCompleted {
					// A job from before the snapshot can still be mid retry.
					l.waitState(30*time.Second, id, relayq.StateCompleted)
				}
			}
		})
	}
}

// Producers and workers keep running while Redis is killed and restarted. Every
// enqueue that returned success must finish. Enqueue errors are retried with the
// same idempotency key, as a real producer would.
func TestJobsSurviveRedisCrashWhileRunning(t *testing.T) {
	l := newLab(t, false, "--appendonly", "yes", "--appendfsync", "always", "--save", "")
	const fleet = 3
	for range fleet {
		l.spawn("-lease", "2s", "-concurrency", "8")
	}

	var (
		mu       sync.Mutex
		accepted = map[string]bool{}
		failures atomic.Int64
		stop     atomic.Bool
		wg       sync.WaitGroup
	)
	for producer := range 4 {
		wg.Go(func() {
			for n := 0; !stop.Load(); n++ {
				key := fmt.Sprintf("p%d-%d", producer, n)
				for {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					res, err := l.client.Enqueue(ctx, queue, "work", []byte(`{"ms":2}`),
						relayq.IdempotencyKey(key), relayq.MaxAttempts(50))
					cancel()
					if err == nil {
						mu.Lock()
						accepted[res.ID] = true
						mu.Unlock()
						break
					}
					failures.Add(1)
					time.Sleep(50 * time.Millisecond)
				}
				time.Sleep(2 * time.Millisecond)
			}
		})
	}

	time.Sleep(2 * time.Second)
	began := time.Now()
	if err := l.redis.CrashRestart(); err != nil {
		t.Fatal(err)
	}
	outage := time.Since(began).Round(time.Millisecond)
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()

	l.waitFor(60*time.Second, "queue to drain", func() bool {
		stats := l.stats()
		return stats.Ready == 0 && stats.Active == 0 && stats.Delayed == 0
	})
	if failures.Load() == 0 {
		t.Fatal("no enqueue failed, so the outage did not touch the producers")
	}

	finished := l.finishCounts()
	duplicates := 0
	for id := range accepted {
		switch n := finished[id]; {
		case n == 0:
			t.Errorf("accepted job %s never finished", id)
		case n > 1:
			duplicates += n - 1
		}
	}
	stats := l.stats()
	if stats.Dead != 0 {
		t.Fatalf("dead jobs: %+v", stats)
	}
	t.Logf("RESULT redis down %v under load: %d jobs accepted, 0 lost, %d enqueue errors retried, "+
		"%d duplicate finishes, %d redelivered", outage, len(accepted), failures.Load(), duplicates,
		stats.Counters["redelivered"])
}

// A frozen Redis looks like a network partition to the worker: heartbeats stop
// getting through. The worker must stop trusting its lease by itself, and after
// the freeze the job must be redelivered.
func TestFrozenRedisFencesTheHandler(t *testing.T) {
	l := newLab(t, false, "--appendonly", "no", "--save", "")
	const lease = time.Second
	l.spawn("-lease", lease.String())
	id := l.enqueue("hold-first", "{}")
	l.waitFor(10*time.Second, "handler to start", func() bool { return l.count("start", id) == 1 })

	if err := l.redis.Pause(); err != nil {
		t.Fatal(err)
	}
	l.waitFor(15*time.Second, "worker to fence its own handler", func() bool {
		for _, e := range l.events() {
			if e.kind == "cancelled" && e.id == id {
				return true
			}
		}
		return false
	})
	if err := l.redis.Unpause(); err != nil {
		t.Fatal(err)
	}

	l.waitState(20*time.Second, id, relayq.StateCompleted)
	job := l.job(id)
	if job.Delivery != 2 {
		t.Fatalf("job %+v", job)
	}
	var cause string
	for _, e := range l.events() {
		if e.kind == "cancelled" {
			cause = e.detail
		}
	}
	if cause != relayq.ErrLeaseLost.Error() {
		t.Fatalf("handler cancelled with %q", cause)
	}
	t.Logf("RESULT handler cancelled with %q while Redis was frozen, job finished on delivery %d", cause, job.Delivery)
}
