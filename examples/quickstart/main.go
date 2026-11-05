// Command quickstart enqueues one job and runs a worker until it has finished.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", ContextTimeoutEnabled: true})
	defer func() { _ = rdb.Close() }()
	client := relayq.NewClient(rdb)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	worker, err := relayq.NewWorker(client, relayq.WorkerConfig{Queue: "emails"})
	if err != nil {
		return err
	}
	worker.Handle("welcome", func(_ context.Context, job *relayq.Job) error {
		var p struct{ To string }
		if err := job.Decode(&p); err != nil {
			return relayq.Permanent(err) // retrying cannot fix a bad payload
		}
		// Delivery is at least once, so this must be safe to run twice.
		fmt.Printf("welcome %s (job %s, attempt %d)\n", p.To, job.ID, job.Attempt)
		stop()
		return nil
	})

	_, err = client.EnqueueJSON(ctx, "emails", "welcome", map[string]string{"to": "ada"},
		relayq.Priority(9),
		relayq.MaxAttempts(3),
		relayq.IdempotencyKey(fmt.Sprintf("welcome-%d", time.Now().Unix())),
	)
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}
