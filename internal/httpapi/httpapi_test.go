package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mykyta-olym/relayq"
)

// fakeService records the enqueue call and returns canned results.
type fakeService struct {
	enqueued  []byte
	enqueue   relayq.Enqueued
	err       error
	jobs      []*relayq.Job
	stats     relayq.QueueStats
	lastLimit int
}

func (f *fakeService) Enqueue(_ context.Context, _, _ string, payload []byte, _ ...relayq.Option) (relayq.Enqueued, error) {
	f.enqueued = payload
	return f.enqueue, f.err
}

func (f *fakeService) Job(context.Context, string, string) (*relayq.Job, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.jobs[0], nil
}

func (f *fakeService) Jobs(_ context.Context, _ string, _ relayq.State, _, limit int) ([]*relayq.Job, error) {
	f.lastLimit = limit
	return f.jobs, f.err
}

func (f *fakeService) Queues(context.Context) ([]string, error) { return []string{"q"}, f.err }

func (f *fakeService) Stats(context.Context, string) (relayq.QueueStats, error) {
	return f.stats, f.err
}

func (f *fakeService) RequeueDead(context.Context, string, string) error { return f.err }

func (f *fakeService) RequeueAllDead(context.Context, string) (int64, error) { return 3, f.err }

func (f *fakeService) Delete(context.Context, string, string) error { return f.err }

func (f *fakeService) Ping(context.Context) error { return f.err }

var quiet = slog.New(slog.DiscardHandler)

func do(t *testing.T, h http.Handler, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct{ Code string }
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("not JSON: %q", rec.Body.String())
	}
	return body.Error.Code
}

func TestEnqueueReturnsCreated(t *testing.T) {
	svc := &fakeService{enqueue: relayq.Enqueued{ID: "abc"}}
	rec := do(t, New(svc, Config{Logger: quiet}), "POST", "/v1/queues/q/jobs",
		`{"type":"t","payload":{"n":1}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Location"); got != "/v1/queues/q/jobs/abc" {
		t.Errorf("location %q", got)
	}
	if string(svc.enqueued) != `{"n":1}` {
		t.Errorf("payload %q", svc.enqueued)
	}
}

func TestDuplicateEnqueueReturnsOK(t *testing.T) {
	svc := &fakeService{enqueue: relayq.Enqueued{ID: "abc", Duplicate: true}}
	rec := do(t, New(svc, Config{Logger: quiet}), "POST", "/v1/queues/q/jobs",
		`{"type":"t"}`, "Idempotency-Key", "k")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestEnqueueRejectsBadBodies(t *testing.T) {
	cases := map[string]string{
		"not json":          `{`,
		"unknown field":     `{"type":"t","prioritty":3}`,
		"two objects":       `{"type":"t"} {"type":"u"}`,
		"bad duration":      `{"type":"t","delay":"soon"}`,
		"bad timeout":       `{"type":"t","timeout":"x"}`,
		"wrong field type":  `{"type":"t","priority":"high"}`,
		"bad run_at format": `{"type":"t","run_at":"tomorrow"}`,
	}
	h := New(&fakeService{}, Config{Logger: quiet})
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if rec := do(t, h, "POST", "/v1/queues/q/jobs", body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestHeaderAndBodyIdempotencyKeysMustAgree(t *testing.T) {
	h := New(&fakeService{}, Config{Logger: quiet})
	rec := do(t, h, "POST", "/v1/queues/q/jobs", `{"type":"t","idempotency_key":"a"}`, "Idempotency-Key", "b")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestBodyLimit(t *testing.T) {
	h := New(&fakeService{}, Config{MaxBody: 64, Logger: quiet})
	body := `{"type":"t","payload":"` + strings.Repeat("x", 100) + `"}`
	if rec := do(t, h, "POST", "/v1/queues/q/jobs", body); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{relayq.ErrNotFound, http.StatusNotFound, "not_found"},
		{relayq.ErrJobActive, http.StatusConflict, "job_active"},
		{relayq.ErrInvalid, http.StatusBadRequest, "invalid"},
		{errors.New("dial tcp 10.0.0.5:6379: refused"), http.StatusServiceUnavailable, "unavailable"},
	}
	for _, tc := range cases {
		h := New(&fakeService{err: tc.err}, Config{Logger: quiet})
		rec := do(t, h, "DELETE", "/v1/queues/q/jobs/abc", "")
		if rec.Code != tc.status || errorCode(t, rec) != tc.code {
			t.Errorf("%v: status %d, body %s", tc.err, rec.Code, rec.Body)
		}
	}
}

func TestInternalErrorsDoNotLeakToClients(t *testing.T) {
	h := New(&fakeService{err: errors.New("dial tcp 10.0.0.5:6379: refused")}, Config{Logger: quiet})
	rec := do(t, h, "GET", "/v1/queues", "")
	if strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Fatalf("address leaked: %s", rec.Body)
	}
}

func TestBearerToken(t *testing.T) {
	h := New(&fakeService{}, Config{Token: "secret", Logger: quiet})
	if rec := do(t, h, "GET", "/v1/queues", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/v1/queues", "", "Authorization", "Bearer wrong"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/v1/queues", "", "Authorization", "Bearer secret"); rec.Code != http.StatusOK {
		t.Fatalf("right token: %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("health must stay open: %d", rec.Code)
	}
}

func TestHealthReportsRedisDown(t *testing.T) {
	h := New(&fakeService{err: errors.New("down")}, Config{Logger: quiet})
	if rec := do(t, h, "GET", "/healthz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestListJobsValidatesAndCapsParameters(t *testing.T) {
	svc := &fakeService{}
	h := New(svc, Config{Logger: quiet})
	if rec := do(t, h, "GET", "/v1/queues/q/jobs", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing state: %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/v1/queues/q/jobs?state=dead&limit=x", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit: %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/v1/queues/q/jobs?state=dead&limit=100000", ""); rec.Code != http.StatusOK {
		t.Fatalf("large limit: %d", rec.Code)
	}
	if svc.lastLimit != maxListLimit {
		t.Fatalf("limit not capped: %d", svc.lastLimit)
	}
}

func TestPayloadIsJSONOrBase64(t *testing.T) {
	now := time.Now()
	cases := []struct {
		payload []byte
		want    string
	}{
		{[]byte(`{"a":1}`), `"payload":{"a":1}`},
		{[]byte{0xff, 0x00}, `"payload_base64":"/wA="`},
	}
	for _, tc := range cases {
		svc := &fakeService{jobs: []*relayq.Job{{ID: "a", Payload: tc.payload, CreatedAt: now}}}
		rec := do(t, New(svc, Config{Logger: quiet}), "GET", "/v1/queues/q/jobs/a", "")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), tc.want) {
			t.Errorf("status %d, body %s, want %s", rec.Code, rec.Body, tc.want)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := New(&fakeService{}, Config{Logger: quiet})
	rec := do(t, h, "PUT", "/v1/queues/q/jobs", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", rec.Code)
	}
	_, _ = io.Copy(io.Discard, rec.Body)
}
