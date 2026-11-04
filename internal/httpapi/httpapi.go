// Package httpapi serves the relayq HTTP API.
package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mykyta-olym/relayq"
)

// DefaultMaxBody caps a request body. Jobs are references to work, not the work.
const DefaultMaxBody = 1 << 20

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

// Service is the part of relayq.Client that the API uses.
type Service interface {
	Enqueue(ctx context.Context, queue, jobType string, payload []byte, opts ...relayq.Option) (relayq.Enqueued, error)
	Job(ctx context.Context, queue, id string) (*relayq.Job, error)
	Jobs(ctx context.Context, queue string, state relayq.State, offset, limit int) ([]*relayq.Job, error)
	Queues(ctx context.Context) ([]string, error)
	Stats(ctx context.Context, queue string) (relayq.QueueStats, error)
	RequeueDead(ctx context.Context, queue, id string) error
	RequeueAllDead(ctx context.Context, queue string) (int64, error)
	Delete(ctx context.Context, queue, id string) error
	Ping(ctx context.Context) error
}

// Config configures the handler.
type Config struct {
	// Token, when set, is required as a bearer token on every /v1 route.
	Token string

	// MaxBody caps request bodies. Default DefaultMaxBody.
	MaxBody int64

	// Gatherer is served at /metrics. Nil disables the route.
	Gatherer prometheus.Gatherer

	Logger *slog.Logger
}

type api struct {
	svc     Service
	cfg     Config
	log     *slog.Logger
	maxBody int64
}

// New builds the handler.
func New(svc Service, cfg Config) http.Handler {
	a := &api{svc: svc, cfg: cfg, log: cfg.Logger, maxBody: cfg.MaxBody}
	if a.log == nil {
		a.log = slog.Default()
	}
	if a.maxBody <= 0 {
		a.maxBody = DefaultMaxBody
	}

	v1 := http.NewServeMux()
	v1.HandleFunc("GET /v1/queues", a.listQueues)
	v1.HandleFunc("GET /v1/queues/{queue}", a.queueStats)
	v1.HandleFunc("POST /v1/queues/{queue}/jobs", a.enqueue)
	v1.HandleFunc("GET /v1/queues/{queue}/jobs", a.listJobs)
	v1.HandleFunc("GET /v1/queues/{queue}/jobs/{id}", a.getJob)
	v1.HandleFunc("DELETE /v1/queues/{queue}/jobs/{id}", a.deleteJob)
	v1.HandleFunc("POST /v1/queues/{queue}/dead/requeue", a.requeueAll)
	v1.HandleFunc("POST /v1/queues/{queue}/dead/{id}/requeue", a.requeueOne)

	root := http.NewServeMux()
	root.Handle("/v1/", a.authenticate(v1))
	root.HandleFunc("GET /healthz", a.health)
	if cfg.Gatherer != nil {
		root.Handle("GET /metrics", promhttp.HandlerFor(cfg.Gatherer, promhttp.HandlerOpts{}))
	}
	return root
}

func (a *api) authenticate(next http.Handler) http.Handler {
	if a.cfg.Token == "" {
		return next
	}
	want := []byte("Bearer " + a.cfg.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			a.fail(w, http.StatusUnauthorized, "unauthorized", "missing or wrong bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.svc.Ping(ctx); err != nil {
		a.fail(w, http.StatusServiceUnavailable, "redis_unavailable", "redis did not answer")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *api) listQueues(w http.ResponseWriter, r *http.Request) {
	names, err := a.svc.Queues(r.Context())
	if err != nil {
		a.failErr(w, err)
		return
	}
	queues := make([]relayq.QueueStats, 0, len(names))
	for _, name := range names {
		stats, err := a.svc.Stats(r.Context(), name)
		if err != nil {
			a.failErr(w, err)
			return
		}
		queues = append(queues, stats)
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": queues})
}

func (a *api) queueStats(w http.ResponseWriter, r *http.Request) {
	stats, err := a.svc.Stats(r.Context(), r.PathValue("queue"))
	if err != nil {
		a.failErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// enqueueRequest is the POST body. Durations are strings that time.ParseDuration
// accepts, so "90s" and "1h30m" both work.
type enqueueRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority"`
	MaxAttempts    *int            `json:"max_attempts"`
	Delay          string          `json:"delay"`
	RunAt          *time.Time      `json:"run_at"`
	Timeout        string          `json:"timeout"`
	BackoffBase    string          `json:"backoff_base"`
	BackoffMax     string          `json:"backoff_max"`
	IdempotencyKey string          `json:"idempotency_key"`
	IdempotencyTTL string          `json:"idempotency_ttl"`
}

func (req *enqueueRequest) options(headerKey string) ([]relayq.Option, error) {
	var opts []relayq.Option
	if req.Priority != nil {
		opts = append(opts, relayq.Priority(*req.Priority))
	}
	if req.MaxAttempts != nil {
		opts = append(opts, relayq.MaxAttempts(*req.MaxAttempts))
	}
	if req.RunAt != nil {
		opts = append(opts, relayq.RunAt(*req.RunAt))
	}

	var delay, timeout, base, maxDelay, ttl time.Duration
	for _, field := range []struct {
		name, raw string
		target    *time.Duration
	}{
		{"delay", req.Delay, &delay},
		{"timeout", req.Timeout, &timeout},
		{"backoff_base", req.BackoffBase, &base},
		{"backoff_max", req.BackoffMax, &maxDelay},
		{"idempotency_ttl", req.IdempotencyTTL, &ttl},
	} {
		if field.raw == "" {
			continue
		}
		parsed, err := time.ParseDuration(field.raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", relayq.ErrInvalid, field.name, err)
		}
		*field.target = parsed
	}
	if delay != 0 {
		opts = append(opts, relayq.Delay(delay))
	}
	if timeout != 0 {
		opts = append(opts, relayq.Timeout(timeout))
	}
	if base != 0 || maxDelay != 0 {
		opts = append(opts, relayq.WithBackoff(base, maxDelay))
	}
	if ttl != 0 {
		opts = append(opts, relayq.IdempotencyTTL(ttl))
	}

	key := req.IdempotencyKey
	if headerKey != "" {
		if key != "" && key != headerKey {
			return nil, fmt.Errorf("%w: idempotency key in header and body differ", relayq.ErrInvalid)
		}
		key = headerKey
	}
	if key != "" {
		opts = append(opts, relayq.IdempotencyKey(key))
	}
	return opts, nil
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, a.maxBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req enqueueRequest
	if err := decoder.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			a.fail(w, http.StatusRequestEntityTooLarge, "too_large",
				fmt.Sprintf("body exceeds %d bytes", a.maxBody))
			return
		}
		a.fail(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		a.fail(w, http.StatusBadRequest, "invalid_json", "body must hold one JSON object")
		return
	}

	opts, err := req.options(r.Header.Get("Idempotency-Key"))
	if err != nil {
		a.failErr(w, err)
		return
	}
	queue := r.PathValue("queue")
	res, err := a.svc.Enqueue(r.Context(), queue, req.Type, req.Payload, opts...)
	if err != nil {
		a.failErr(w, err)
		return
	}

	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/v1/queues/"+queue+"/jobs/"+res.ID)
	writeJSON(w, status, res)
}

// jobView adds fields that do not map directly onto relayq.Job. JSON payloads are
// shown as they are. Anything else is base64, so a binary payload enqueued from
// the Go library still round trips.
type jobView struct {
	*relayq.Job
	Payload       json.RawMessage `json:"payload,omitempty"`
	PayloadBase64 string          `json:"payload_base64,omitempty"`
	Timeout       string          `json:"timeout,omitempty"`
}

func newJobView(job *relayq.Job) jobView {
	view := jobView{Job: job}
	switch {
	case len(job.Payload) == 0:
	case json.Valid(job.Payload):
		view.Payload = job.Payload
	default:
		view.PayloadBase64 = base64.StdEncoding.EncodeToString(job.Payload)
	}
	if job.Timeout > 0 {
		view.Timeout = job.Timeout.String()
	}
	return view
}

func (a *api) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.svc.Job(r.Context(), r.PathValue("queue"), r.PathValue("id"))
	if err != nil {
		a.failErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newJobView(job))
}

func (a *api) listJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	state := relayq.State(query.Get("state"))
	if state == "" {
		a.fail(w, http.StatusBadRequest, "invalid", "state is required")
		return
	}
	offset, err := intParam(query.Get("offset"), 0)
	if err != nil {
		a.fail(w, http.StatusBadRequest, "invalid", "offset must be a number")
		return
	}
	limit, err := intParam(query.Get("limit"), defaultListLimit)
	if err != nil {
		a.fail(w, http.StatusBadRequest, "invalid", "limit must be a number")
		return
	}
	limit = min(limit, maxListLimit)

	jobs, err := a.svc.Jobs(r.Context(), r.PathValue("queue"), state, offset, limit)
	if err != nil {
		a.failErr(w, err)
		return
	}
	views := make([]jobView, len(jobs))
	for i, job := range jobs {
		views[i] = newJobView(job)
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": views, "offset": offset, "limit": limit})
}

func (a *api) deleteJob(w http.ResponseWriter, r *http.Request) {
	if err := a.svc.Delete(r.Context(), r.PathValue("queue"), r.PathValue("id")); err != nil {
		a.failErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) requeueOne(w http.ResponseWriter, r *http.Request) {
	if err := a.svc.RequeueDead(r.Context(), r.PathValue("queue"), r.PathValue("id")); err != nil {
		a.failErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"requeued": 1})
}

func (a *api) requeueAll(w http.ResponseWriter, r *http.Request) {
	moved, err := a.svc.RequeueAllDead(r.Context(), r.PathValue("queue"))
	if err != nil {
		a.failErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"requeued": moved})
}

func (a *api) failErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, relayq.ErrInvalid):
		a.fail(w, http.StatusBadRequest, "invalid", strings.TrimPrefix(err.Error(), "relayq: invalid argument: "))
	case errors.Is(err, relayq.ErrNotFound):
		a.fail(w, http.StatusNotFound, "not_found", "no such job")
	case errors.Is(err, relayq.ErrJobActive):
		a.fail(w, http.StatusConflict, "job_active", "job is leased to a worker")
	default:
		// The cause goes to the log. It can contain addresses the caller should
		// not see.
		a.log.Error("request failed", "error", err)
		a.fail(w, http.StatusServiceUnavailable, "unavailable", "the job store is unavailable")
	}
}

func (a *api) fail(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func intParam(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	return strconv.Atoi(raw)
}
