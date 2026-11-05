// Command demo-producer sends a steady mix of jobs through the HTTP API. Every
// tenth job is sent twice with the same idempotency key, so deduplication shows up
// in the counters.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type job struct {
	Type           string         `json:"type"`
	Payload        map[string]any `json:"payload"`
	Priority       int            `json:"priority"`
	MaxAttempts    int            `json:"max_attempts,omitempty"`
	Delay          string         `json:"delay,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

func next(n int) job {
	j := job{Payload: map[string]any{"n": n}, Priority: 5}
	switch roll := rand.IntN(100); {
	case roll < 50:
		j.Type = "email.send"
		if roll < 10 {
			j.Priority = 9
		}
	case roll < 75:
		j.Type = "image.resize"
		j.Priority = 3
	case roll < 90:
		j.Type = "report.flaky"
		j.MaxAttempts = 8
	case roll < 95:
		j.Type = "email.send"
		j.Delay = "10s"
	default:
		j.Type = "invoice.broken"
		j.MaxAttempts = 3
	}
	if n%10 == 0 {
		j.IdempotencyKey = fmt.Sprintf("demo-%d-%d", os.Getpid(), n)
	}
	return j
}

func main() {
	server := flag.String("server", "http://127.0.0.1:8080", "API base URL")
	queue := flag.String("queue", "demo", "queue")
	rate := flag.Int("rate", 10, "jobs per second")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{Timeout: 5 * time.Second}
	url := *server + "/v1/queues/" + *queue + "/jobs"
	send := func(j job) (int, error) {
		body, err := json.Marshal(j)
		if err != nil {
			return 0, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return 0, err
		}
		res, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer func() { _ = res.Body.Close() }()
		_, _ = io.Copy(io.Discard, res.Body)
		return res.StatusCode, nil
	}

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		j := next(n)
		status, err := send(j)
		if err != nil {
			log.Warn("enqueue failed", "error", err)
			continue
		}
		if j.IdempotencyKey != "" {
			if replay, err := send(j); err == nil {
				log.Info("sent the same key twice", "first_status", status, "second_status", replay)
			}
		}
		if n%50 == 0 {
			log.Info("progress", "sent", n)
		}
	}
}
