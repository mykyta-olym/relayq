// Command relayq-bench measures throughput against a real Redis through the same
// code paths production uses. Every run checks that all jobs were processed
// exactly once before its number is accepted.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq"
)

const benchQueue = "bench"

type config struct {
	url         string
	scenario    string
	jobs        int
	producers   int
	batch       int
	workers     int
	concurrency int
	payload     int
	rate        int
	runs        int
}

// result is one run. Latency is empty when a scenario does not measure it.
type result struct {
	perSecond float64
	took      time.Duration
	latency   []time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.url, "redis", "redis://127.0.0.1:6379/0", "Redis URL")
	flag.StringVar(&cfg.scenario, "scenario", "e2e", "enqueue, process or e2e")
	flag.IntVar(&cfg.jobs, "jobs", 100000, "jobs per run")
	flag.IntVar(&cfg.producers, "producers", 8, "concurrent producers")
	flag.IntVar(&cfg.batch, "batch", 1, "jobs per enqueue call")
	flag.IntVar(&cfg.workers, "workers", 4, "Worker instances, each with its own connection pool")
	flag.IntVar(&cfg.concurrency, "concurrency", 32, "handlers per worker")
	flag.IntVar(&cfg.payload, "payload", 128, "payload bytes")
	flag.IntVar(&cfg.rate, "rate", 0, "total jobs per second for e2e, 0 means as fast as possible")
	flag.IntVar(&cfg.runs, "runs", 5, "runs to take the median of")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "relayq-bench:", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	if cfg.jobs%(cfg.producers*cfg.batch) != 0 {
		return errors.New("jobs must divide evenly by producers*batch")
	}
	if cfg.payload < 8 {
		return errors.New("payload must be at least 8 bytes, it carries the send time")
	}
	scenarios := map[string]func(config, string) (result, error){
		"enqueue": benchEnqueue,
		"process": benchProcess,
		"e2e":     benchEndToEnd,
	}
	scenario, ok := scenarios[cfg.scenario]
	if !ok {
		return fmt.Errorf("unknown scenario %q", cfg.scenario)
	}

	header, err := describe(cfg)
	if err != nil {
		return err
	}
	fmt.Println(header)

	var results []result
	for i := range cfg.runs {
		namespace := fmt.Sprintf("bench%d", time.Now().UnixNano())
		res, err := scenario(cfg, namespace)
		cleanupErr := cleanup(cfg, namespace)
		if err != nil {
			return err
		}
		if cleanupErr != nil {
			return cleanupErr
		}
		results = append(results, res)
		fmt.Printf("run %d: %.0f jobs/s in %v%s\n", i+1, res.perSecond, res.took.Round(time.Millisecond), latencyText(res.latency))
	}

	slices.SortFunc(results, func(a, b result) int {
		switch {
		case a.perSecond < b.perSecond:
			return -1
		case a.perSecond > b.perSecond:
			return 1
		}
		return 0
	})
	median := results[len(results)/2]
	fmt.Printf("median %.0f jobs/s (min %.0f, max %.0f, %d runs)%s\n",
		median.perSecond, results[0].perSecond, results[len(results)-1].perSecond, cfg.runs, latencyText(median.latency))
	return nil
}

func describe(cfg config) (string, error) {
	rdb, err := connect(cfg, 8)
	if err != nil {
		return "", err
	}
	defer func() { _ = rdb.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := rdb.Info(ctx, "server", "persistence").Result()
	if err != nil {
		return "", fmt.Errorf("read Redis info: %w", err)
	}
	fields := map[string]string{}
	for line := range strings.Lines(info) {
		if name, value, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			fields[name] = value
		}
	}
	return fmt.Sprintf("%s %s/%s, %d CPUs seen, Redis %s, aof_enabled=%s\n"+
		"scenario=%s jobs=%d producers=%d batch=%d workers=%d concurrency=%d payload=%dB rate=%d",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(),
		fields["redis_version"], fields["aof_enabled"],
		cfg.scenario, cfg.jobs, cfg.producers, cfg.batch, cfg.workers, cfg.concurrency, cfg.payload, cfg.rate), nil
}

func connect(cfg config, pool int) (*redis.Client, error) {
	options, err := redis.ParseURL(cfg.url)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	options.ContextTimeoutEnabled = true
	options.PoolSize = pool
	return redis.NewClient(options), nil
}

// cleanup leaves the server as it was found, apart from the script cache.
func cleanup(cfg config, namespace string) error {
	rdb, err := connect(cfg, 8)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, namespace+":*", 1000).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) == 1000 {
			if err := rdb.Unlink(ctx, keys...).Err(); err != nil {
				return err
			}
			keys = keys[:0]
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return rdb.Unlink(ctx, keys...).Err()
	}
	return nil
}

// produce runs the producers. Each send stamps the payload with the send time.
// A non zero rate spreads sends evenly, so the queue stays short and the latency
// measured is the system's and not the backlog's.
func produce(cfg config, client *relayq.Client, record func(time.Duration)) error {
	var wg sync.WaitGroup
	errs := make(chan error, cfg.producers)
	calls := cfg.jobs / cfg.producers / cfg.batch

	var interval time.Duration
	if cfg.rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(cfg.producers*cfg.batch) / float64(cfg.rate))
	}

	for range cfg.producers {
		wg.Go(func() {
			items := make([]relayq.BatchItem, cfg.batch)
			for i := range items {
				items[i] = relayq.BatchItem{Type: "noop"}
			}
			began := time.Now()
			for call := range calls {
				if interval > 0 {
					time.Sleep(time.Until(began.Add(time.Duration(call) * interval)))
				}
				now := time.Now()
				for i := range items {
					payload := make([]byte, cfg.payload)
					binary.LittleEndian.PutUint64(payload, uint64(now.UnixNano()))
					items[i].Payload = payload
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := client.EnqueueBatch(ctx, benchQueue, items)
				cancel()
				if err != nil {
					errs <- err
					return
				}
				if record != nil {
					record(time.Since(now))
				}
			}
		})
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func benchEnqueue(cfg config, namespace string) (result, error) {
	rdb, err := connect(cfg, cfg.producers+4)
	if err != nil {
		return result{}, err
	}
	defer func() { _ = rdb.Close() }()
	client := relayq.NewClient(rdb, relayq.WithNamespace(namespace))

	var mu sync.Mutex
	calls := make([]time.Duration, 0, cfg.jobs/cfg.batch)
	began := time.Now()
	err = produce(cfg, client, func(d time.Duration) {
		mu.Lock()
		calls = append(calls, d)
		mu.Unlock()
	})
	took := time.Since(began)
	if err != nil {
		return result{}, err
	}

	stats, err := client.Stats(context.Background(), benchQueue)
	if err != nil {
		return result{}, err
	}
	if stats.Ready != int64(cfg.jobs) {
		return result{}, fmt.Errorf("stored %d jobs, sent %d", stats.Ready, cfg.jobs)
	}
	return result{perSecond: float64(cfg.jobs) / took.Seconds(), took: took, latency: calls}, nil
}

// fleet runs Worker instances with no-op handlers and a separate pool each.
type fleet struct {
	cancel context.CancelFunc
	done   sync.WaitGroup
	pools  []*redis.Client
}

func startFleet(cfg config, namespace string, handler relayq.Handler) (*fleet, error) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fleet{cancel: cancel}
	for range cfg.workers {
		rdb, err := connect(cfg, cfg.concurrency+8)
		if err != nil {
			cancel()
			return nil, err
		}
		f.pools = append(f.pools, rdb)
		worker, err := relayq.NewWorker(relayq.NewClient(rdb, relayq.WithNamespace(namespace)), relayq.WorkerConfig{
			Queue:       benchQueue,
			Concurrency: cfg.concurrency,
			Retention:   -1,
			Logger:      slog.New(slog.DiscardHandler),
		})
		if err != nil {
			cancel()
			return nil, err
		}
		worker.Handle("noop", handler)
		f.done.Go(func() {
			if err := worker.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "worker:", err)
			}
		})
	}
	return f, nil
}

func (f *fleet) stop() {
	f.cancel()
	f.done.Wait()
	for _, rdb := range f.pools {
		_ = rdb.Close()
	}
}

// waitForAll polls Redis, not the handlers, so a job only counts once its ack has
// landed. It then checks the books: exactly one delivery per job.
func waitForAll(client *relayq.Client, jobs int) error {
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		stats, err := client.Stats(ctx, benchQueue)
		if err != nil {
			return err
		}
		if stats.Counters["completed"] >= int64(jobs) {
			if stats.Counters["completed"] != int64(jobs) || stats.Counters["delivered"] != int64(jobs) ||
				stats.Ready != 0 || stats.Active != 0 || stats.Dead != 0 {
				return fmt.Errorf("books do not balance: %+v", stats)
			}
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errors.New("timed out waiting for jobs")
}

func benchProcess(cfg config, namespace string) (result, error) {
	rdb, err := connect(cfg, cfg.producers+4)
	if err != nil {
		return result{}, err
	}
	defer func() { _ = rdb.Close() }()
	client := relayq.NewClient(rdb, relayq.WithNamespace(namespace))

	fill := cfg
	fill.batch = 100
	fill.rate = 0
	if err := produce(fill, client, nil); err != nil {
		return result{}, err
	}

	var handled atomic.Int64
	began := time.Now()
	f, err := startFleet(cfg, namespace, func(context.Context, *relayq.Job) error {
		handled.Add(1)
		return nil
	})
	if err != nil {
		return result{}, err
	}
	err = waitForAll(client, cfg.jobs)
	took := time.Since(began)
	f.stop()
	if err != nil {
		return result{}, err
	}
	if handled.Load() != int64(cfg.jobs) {
		return result{}, fmt.Errorf("handlers ran %d times for %d jobs", handled.Load(), cfg.jobs)
	}
	return result{perSecond: float64(cfg.jobs) / took.Seconds(), took: took}, nil
}

func benchEndToEnd(cfg config, namespace string) (result, error) {
	rdb, err := connect(cfg, cfg.producers+4)
	if err != nil {
		return result{}, err
	}
	defer func() { _ = rdb.Close() }()
	client := relayq.NewClient(rdb, relayq.WithNamespace(namespace))

	samples := make([]time.Duration, cfg.jobs)
	var next atomic.Int64
	f, err := startFleet(cfg, namespace, func(_ context.Context, job *relayq.Job) error {
		sent := int64(binary.LittleEndian.Uint64(job.Payload))
		samples[next.Add(1)-1] = time.Since(time.Unix(0, sent))
		return nil
	})
	if err != nil {
		return result{}, err
	}

	began := time.Now()
	if err := produce(cfg, client, nil); err != nil {
		f.stop()
		return result{}, err
	}
	err = waitForAll(client, cfg.jobs)
	took := time.Since(began)
	f.stop()
	if err != nil {
		return result{}, err
	}
	if next.Load() != int64(cfg.jobs) {
		return result{}, fmt.Errorf("handlers ran %d times for %d jobs", next.Load(), cfg.jobs)
	}
	return result{perSecond: float64(cfg.jobs) / took.Seconds(), took: took, latency: samples}, nil
}

func latencyText(samples []time.Duration) string {
	if len(samples) == 0 {
		return ""
	}
	sorted := slices.Sorted(slices.Values(samples))
	at := func(p float64) time.Duration {
		return sorted[int(p*float64(len(sorted)-1))].Round(10 * time.Microsecond)
	}
	return fmt.Sprintf(", latency p50 %v p95 %v p99 %v", at(.50), at(.95), at(.99))
}
