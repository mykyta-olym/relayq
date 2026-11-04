//go:build integration

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mykyta-olym/relayq"
	"github.com/mykyta-olym/relayq/internal/redistest"
)

var server *redistest.Server

func TestMain(m *testing.M) {
	var err error
	server, err = redistest.Start(true, "--appendonly", "no", "--save", "")
	if err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	server.Remove()
	os.Exit(code)
}

func call(t *testing.T, method, url, body string, out any) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: %v in %q", method, url, err, raw)
		}
	}
	return res.StatusCode, res.Header
}

// The whole API path against real Redis and a real worker: enqueue, deduplicate,
// inspect, fail into the dead letter queue, requeue and scrape metrics.
func TestAPIEndToEnd(t *testing.T) {
	rdb := server.Client()
	t.Cleanup(func() { _ = rdb.Close() })
	client := relayq.NewClient(rdb, relayq.WithNamespace(fmt.Sprintf("api%d", time.Now().UnixNano())))

	registry := prometheus.NewRegistry()
	registry.MustRegister(relayq.NewCollector(client))
	api := httptest.NewServer(New(client, Config{Gatherer: registry, Logger: quiet}))
	t.Cleanup(api.Close)

	var fixed bool
	fixedCh := make(chan struct{})
	worker, err := relayq.NewWorker(client, relayq.WorkerConfig{
		Queue:               "mail",
		LeaseDuration:       2 * time.Second,
		MaintenanceInterval: 20 * time.Millisecond,
		PollInterval:        20 * time.Millisecond,
		Backoff:             relayq.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond},
		Logger:              quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.Handle("send", func(_ context.Context, job *relayq.Job) error {
		var p struct{ Fail bool }
		if err := job.Decode(&p); err != nil {
			return err
		}
		select {
		case <-fixedCh:
			fixed = true
		default:
		}
		if p.Fail && !fixed {
			return errors.New("smtp refused")
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	var first relayq.Enqueued
	status, header := call(t, "POST", api.URL+"/v1/queues/mail/jobs",
		`{"type":"send","payload":{"to":"a"},"idempotency_key":"welcome-1"}`, &first)
	if status != http.StatusCreated || first.ID == "" {
		t.Fatalf("enqueue: %d %+v", status, first)
	}
	if header.Get("Location") == "" {
		t.Fatal("no Location header")
	}

	var second relayq.Enqueued
	status, _ = call(t, "POST", api.URL+"/v1/queues/mail/jobs",
		`{"type":"send","payload":{"to":"a"},"idempotency_key":"welcome-1"}`, &second)
	if status != http.StatusOK || second.ID != first.ID || !second.Duplicate {
		t.Fatalf("replayed enqueue: %d %+v", status, second)
	}

	waitFor := func(id, state string) map[string]any {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		var job map[string]any
		for time.Now().Before(deadline) {
			job = nil
			call(t, "GET", api.URL+"/v1/queues/mail/jobs/"+id, "", &job)
			if job["state"] == state {
				return job
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("job %s never reached %s: %v", id, state, job)
		return nil
	}
	job := waitFor(first.ID, "completed")
	if job["attempt"] != 1.0 {
		t.Fatalf("job: %v", job)
	}

	var failing relayq.Enqueued
	call(t, "POST", api.URL+"/v1/queues/mail/jobs",
		`{"type":"send","payload":{"fail":true},"max_attempts":2}`, &failing)
	job = waitFor(failing.ID, "dead")
	if job["last_error"] != "smtp refused" {
		t.Fatalf("dead job: %v", job)
	}

	var dead struct{ Jobs []map[string]any }
	call(t, "GET", api.URL+"/v1/queues/mail/jobs?state=dead", "", &dead)
	if len(dead.Jobs) != 1 || dead.Jobs[0]["id"] != failing.ID {
		t.Fatalf("dead list: %+v", dead)
	}

	close(fixedCh)
	var requeued map[string]int
	status, _ = call(t, "POST", api.URL+"/v1/queues/mail/dead/"+failing.ID+"/requeue", "", &requeued)
	if status != http.StatusOK || requeued["requeued"] != 1 {
		t.Fatalf("requeue: %d %v", status, requeued)
	}
	waitFor(failing.ID, "completed")

	if status, _ := call(t, "POST", api.URL+"/v1/queues/mail/dead/"+failing.ID+"/requeue", "", nil); status != http.StatusNotFound {
		t.Fatalf("second requeue: %d", status)
	}

	var delayed relayq.Enqueued
	call(t, "POST", api.URL+"/v1/queues/mail/jobs", `{"type":"send","delay":"1h","priority":9}`, &delayed)
	waitFor(delayed.ID, "delayed")
	if status, _ := call(t, "DELETE", api.URL+"/v1/queues/mail/jobs/"+delayed.ID, "", nil); status != http.StatusNoContent {
		t.Fatalf("delete: %d", status)
	}
	if status, _ := call(t, "GET", api.URL+"/v1/queues/mail/jobs/"+delayed.ID, "", nil); status != http.StatusNotFound {
		t.Fatalf("deleted job still there: %d", status)
	}

	var queues struct {
		Queues []relayq.QueueStats
	}
	call(t, "GET", api.URL+"/v1/queues", "", &queues)
	if len(queues.Queues) != 1 || queues.Queues[0].Counters["completed"] != 2 {
		t.Fatalf("queues: %+v", queues)
	}

	res, err := http.Get(api.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	text := string(raw)
	for _, want := range []string{
		`relayq_jobs_total{event="completed",queue="mail"} 2`,
		`relayq_jobs_total{event="deduplicated",queue="mail"} 1`,
		`relayq_jobs_total{event="requeued",queue="mail"} 1`,
		`relayq_queue_jobs{queue="mail",state="dead"} 0`,
		`relayq_redis_up 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics missing %q in:\n%s", want, text)
		}
	}
}
