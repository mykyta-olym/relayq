//go:build failure

// Package failure kills real worker processes and real Redis servers, then checks
// what happened to the jobs. Nothing here is simulated in process.
package failure

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq"
	"github.com/mykyta-olym/relayq/internal/redistest"
)

var workerBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "relayq-failure")
	if err != nil {
		log.Fatal(err)
	}
	workerBinary = filepath.Join(dir, "workerproc")
	if runtime.GOOS == "windows" {
		workerBinary += ".exe"
	}
	args := []string{"build"}
	if raceEnabled {
		args = append(args, "-race")
	}
	args = append(args, "-o", workerBinary, "./workerproc")
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		log.Fatalf("build worker: %v\n%s", err, out)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// lab is one Redis server, one namespace and a directory of worker evidence.
type lab struct {
	t        *testing.T
	redis    *redistest.Server
	rdb      *redis.Client
	client   *relayq.Client
	ns       string
	evidence string

	mu    sync.Mutex
	procs []*proc
}

const queue = "q"

// newLab starts a dedicated Redis, unless external is set and the environment
// names a server. Only tests that never touch the server may use one.
func newLab(t *testing.T, external bool, redisArgs ...string) *lab {
	t.Helper()
	server, err := redistest.Start(external, redisArgs...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Remove)
	rdb := server.Client()
	t.Cleanup(func() { _ = rdb.Close() })
	ns := fmt.Sprintf("failure%d", time.Now().UnixNano())
	l := &lab{
		t:        t,
		redis:    server,
		rdb:      rdb,
		client:   relayq.NewClient(rdb, relayq.WithNamespace(ns)),
		ns:       ns,
		evidence: t.TempDir(),
	}
	t.Cleanup(l.stopAll)
	return l
}

// proc is a running worker.
type proc struct {
	cmd    *exec.Cmd
	stderr *lockedWriter
	done   chan error
}

func (l *lab) spawn(flags ...string) *proc {
	l.t.Helper()
	args := []string{"-redis", l.redis.Addr, "-namespace", l.ns, "-queue", queue, "-evidence", l.evidence}
	args = append(args, flags...)
	cmd := exec.Command(workerBinary, args...)
	stderr := &lockedWriter{buffer: &bytes.Buffer{}}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		l.t.Fatal(err)
	}
	p := &proc{cmd: cmd, stderr: stderr, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	l.mu.Lock()
	l.procs = append(l.procs, p)
	l.mu.Unlock()

	ready := filepath.Join(l.evidence, fmt.Sprintf("ready-%d.json", cmd.Process.Pid))
	l.waitFor(30*time.Second, "worker to start", func() bool {
		_, err := os.Stat(ready)
		return err == nil
	})
	return p
}

// kill is SIGKILL on Unix and TerminateProcess on Windows. The process gets no
// chance to run any code, so no ack, no release and no goodbye.
func (p *proc) kill() {
	_ = p.cmd.Process.Kill()
	_ = p.wait()
}

// wait blocks until the process has exited.
func (p *proc) wait() error { return <-p.done }

func (l *lab) stopAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.procs {
		_ = p.cmd.Process.Kill()
		if strings.Contains(p.stderr.String(), "DATA RACE") {
			l.t.Errorf("data race in worker process:\n%s", p.stderr.String())
		}
	}
}

type lockedWriter struct {
	mu     sync.Mutex
	buffer *bytes.Buffer
}

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(b)
}

// String reads under the same lock, because the process may still be writing.
func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

func (l *lab) waitFor(timeout time.Duration, what string, cond func() bool) {
	l.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	l.t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func (l *lab) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	l.t.Cleanup(cancel)
	return ctx
}

func (l *lab) enqueue(jobType, payload string, opts ...relayq.Option) string {
	l.t.Helper()
	res, err := l.client.Enqueue(l.ctx(), queue, jobType, []byte(payload), opts...)
	if err != nil {
		l.t.Fatal(err)
	}
	return res.ID
}

func (l *lab) job(id string) *relayq.Job {
	l.t.Helper()
	job, err := l.client.Job(l.ctx(), queue, id)
	if err != nil {
		l.t.Fatalf("job %s: %v", id, err)
	}
	return job
}

func (l *lab) stats() relayq.QueueStats {
	l.t.Helper()
	stats, err := l.client.Stats(l.ctx(), queue)
	if err != nil {
		l.t.Fatal(err)
	}
	return stats
}

func (l *lab) waitState(timeout time.Duration, id string, state relayq.State) {
	l.t.Helper()
	l.waitFor(timeout, "job to reach "+string(state), func() bool {
		job, err := l.client.Job(l.ctx(), queue, id)
		return err == nil && job.State == state
	})
}

// event is one line of evidence.
type event struct {
	kind     string
	id       string
	delivery int
	attempt  int
	detail   string
}

// events reads the evidence of every process, including ones that were killed.
func (l *lab) events() []event {
	l.t.Helper()
	files, err := filepath.Glob(filepath.Join(l.evidence, "worker-*.log"))
	if err != nil {
		l.t.Fatal(err)
	}
	var all []event
	for _, name := range files {
		file, err := os.Open(name)
		if err != nil {
			l.t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), " ", 5)
			if len(parts) < 4 {
				continue // torn final line from a kill
			}
			delivery, err1 := strconv.Atoi(parts[2])
			attempt, err2 := strconv.Atoi(parts[3])
			if err1 != nil || err2 != nil {
				continue
			}
			e := event{kind: parts[0], id: parts[1], delivery: delivery, attempt: attempt}
			if len(parts) == 5 {
				e.detail = parts[4]
			}
			all = append(all, e)
		}
		_ = file.Close()
	}
	return all
}

func (l *lab) count(kind, id string) int {
	l.t.Helper()
	n := 0
	for _, e := range l.events() {
		if e.kind == kind && (id == "" || e.id == id) {
			n++
		}
	}
	return n
}

// finishCounts maps each job id to how many times its handler ran to the end.
func (l *lab) finishCounts() map[string]int {
	l.t.Helper()
	counts := map[string]int{}
	for _, e := range l.events() {
		if e.kind == "finish" {
			counts[e.id]++
		}
	}
	return counts
}
