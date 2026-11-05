// Command workerproc is the worker the failure tests start and kill. It is a real
// process so a kill is a real kill. Each handler event goes to a private file
// outside Redis, so the record of what ran survives anything done to Redis.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq"
)

// evidence is an append only log. Lines are written whole, so a process killed
// mid write leaves at most one unterminated line, which the reader drops.
type evidence struct {
	mu   sync.Mutex
	file *os.File
}

func (e *evidence) record(kind string, job *relayq.Job, detail string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	line := fmt.Sprintf("%s %s %d %d %s\n", kind, job.ID, job.Delivery, job.Attempt, detail)
	if _, err := e.file.WriteString(line); err != nil {
		panic(err)
	}
}

type work struct {
	Millis int `json:"ms"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		os.Exit(3)
	}
}

func run() error {
	addr := flag.String("redis", "", "Redis address")
	namespace := flag.String("namespace", "", "key prefix")
	queue := flag.String("queue", "q", "queue")
	dir := flag.String("evidence", "", "directory for the evidence file")
	concurrency := flag.Int("concurrency", 8, "handlers at once")
	lease := flag.Duration("lease", time.Second, "visibility timeout")
	shutdown := flag.Duration("shutdown-timeout", 5*time.Second, "graceful shutdown budget")
	flag.Parse()

	file, err := os.CreateTemp(*dir, fmt.Sprintf("worker-%d-*.log", os.Getpid()))
	if err != nil {
		return err
	}
	log := &evidence{file: file}

	rdb := redis.NewClient(&redis.Options{
		Addr:                  *addr,
		ContextTimeoutEnabled: true,
		DialTimeout:           500 * time.Millisecond,
		ReadTimeout:           500 * time.Millisecond,
		WriteTimeout:          500 * time.Millisecond,
		PoolSize:              *concurrency + 8,
	})
	defer func() { _ = rdb.Close() }()

	worker, err := relayq.NewWorker(relayq.NewClient(rdb, relayq.WithNamespace(*namespace)), relayq.WorkerConfig{
		Queue:               *queue,
		Concurrency:         *concurrency,
		LeaseDuration:       *lease,
		MaintenanceInterval: 50 * time.Millisecond,
		PollInterval:        50 * time.Millisecond,
		ShutdownTimeout:     *shutdown,
		CancelGrace:         time.Second,
		Backoff:             relayq.Backoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, Jitter: 1},
		Logger:              slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		return err
	}

	// work sleeps for the payload duration, then succeeds.
	worker.Handle("work", func(ctx context.Context, job *relayq.Job) error {
		log.record("start", job, "")
		var w work
		if err := job.Decode(&w); err != nil {
			return relayq.Permanent(err)
		}
		select {
		case <-time.After(time.Duration(w.Millis) * time.Millisecond):
			log.record("finish", job, "")
			return nil
		case <-ctx.Done():
			log.record("cancelled", job, context.Cause(ctx).Error())
			return ctx.Err()
		}
	})

	// hold-first blocks on its first delivery until it is cancelled, so a test
	// can catch it mid job. Later deliveries finish at once.
	worker.Handle("hold-first", func(ctx context.Context, job *relayq.Job) error {
		log.record("start", job, "")
		if job.Delivery > 1 {
			log.record("finish", job, "")
			return nil
		}
		<-ctx.Done()
		log.record("cancelled", job, context.Cause(ctx).Error())
		return ctx.Err()
	})

	// effect-then-crash performs its side effect, then dies before the ack could
	// be sent. This is the window that makes duplicates unavoidable.
	worker.Handle("effect-then-crash", func(_ context.Context, job *relayq.Job) error {
		log.record("start", job, "")
		log.record("effect", job, "")
		if job.Delivery == 1 {
			os.Exit(137)
		}
		log.record("finish", job, "")
		return nil
	})

	// poison kills the worker on every delivery.
	worker.Handle("poison", func(_ context.Context, job *relayq.Job) error {
		log.record("start", job, "")
		os.Exit(137)
		return nil
	})

	worker.Handle("fail", func(_ context.Context, job *relayq.Job) error {
		log.record("start", job, "")
		return relayq.Permanent(errors.New("always fails"))
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The ready line tells the test the worker is up. Tests that want a job caught
	// mid handler wait for the handler's own start event instead.
	ready, _ := json.Marshal(map[string]any{"worker": worker.ID(), "pid": os.Getpid()})
	_ = os.WriteFile(filepath.Join(*dir, fmt.Sprintf("ready-%d.json", os.Getpid())), ready, 0o600)

	return worker.Run(ctx)
}
