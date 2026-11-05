// Command demo-worker runs handlers that behave like real work: some are slow,
// some are flaky and one always fails, so retries and the dead letter queue show
// up in a demo run. It exposes worker metrics on -metrics.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq"
)

func main() {
	redisURL := flag.String("redis", "redis://127.0.0.1:6379/0", "Redis URL")
	queue := flag.String("queue", "demo", "queue")
	concurrency := flag.Int("concurrency", 8, "handlers at once")
	lease := flag.Duration("lease", 5*time.Second, "visibility timeout")
	metrics := flag.String("metrics", ":9100", "metrics listen address, empty to disable")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	options, err := redis.ParseURL(*redisURL)
	if err != nil {
		log.Error("bad Redis URL", "error", err)
		os.Exit(1)
	}
	options.ContextTimeoutEnabled = true
	rdb := redis.NewClient(options)
	defer func() { _ = rdb.Close() }()

	registry := prometheus.NewRegistry()
	worker, err := relayq.NewWorker(relayq.NewClient(rdb), relayq.WorkerConfig{
		Queue:           *queue,
		Concurrency:     *concurrency,
		LeaseDuration:   *lease,
		ShutdownTimeout: 20 * time.Second,
		Backoff:         relayq.Backoff{Base: 500 * time.Millisecond, Max: 10 * time.Second, Jitter: 0.5},
		Logger:          log,
		Registerer:      registry,
	})
	if err != nil {
		log.Error("create worker", "error", err)
		os.Exit(1)
	}

	worker.Handle("email.send", func(ctx context.Context, job *relayq.Job) error {
		log.Info("sending email", "job", job.ID, "priority", job.Priority, "attempt", job.Attempt)
		return pause(ctx, 50*time.Millisecond)
	})
	worker.Handle("image.resize", func(ctx context.Context, job *relayq.Job) error {
		log.Info("resizing image", "job", job.ID, "attempt", job.Attempt)
		return pause(ctx, time.Duration(200+rand.IntN(800))*time.Millisecond)
	})
	worker.Handle("report.flaky", func(ctx context.Context, job *relayq.Job) error {
		if rand.IntN(2) == 0 {
			log.Info("flaky job failed, it will retry", "job", job.ID, "attempt", job.Attempt)
			return errors.New("upstream timed out")
		}
		log.Info("flaky job succeeded", "job", job.ID, "attempt", job.Attempt)
		return pause(ctx, 100*time.Millisecond)
	})
	worker.Handle("invoice.broken", func(_ context.Context, job *relayq.Job) error {
		log.Info("broken job failed", "job", job.ID, "attempt", job.Attempt)
		return errors.New("invoice template is missing")
	})

	if *metrics != "" {
		server := &http.Server{
			Addr:              *metrics,
			Handler:           promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "error", err)
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for _, warning := range relayq.NewClient(rdb).CheckRedis(ctx) {
		log.Warn(warning)
	}
	if err := worker.Run(ctx); err != nil {
		log.Error("worker stopped with an error", "error", err)
		os.Exit(1)
	}
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
