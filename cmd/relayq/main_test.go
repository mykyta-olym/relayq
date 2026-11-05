package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// stubAPI records requests and replies with a fixed body.
type stubAPI struct {
	status int
	reply  string
	method string
	path   string
	body   map[string]any
	auth   string
}

func (s *stubAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.method, s.path, s.auth = r.Method, r.URL.RequestURI(), r.Header.Get("Authorization")
	raw, _ := io.ReadAll(r.Body)
	s.body = nil
	_ = json.Unmarshal(raw, &s.body)
	if s.status != 0 {
		w.WriteHeader(s.status)
	}
	_, _ = io.WriteString(w, s.reply)
}

func runCLI(t *testing.T, stub *stubAPI, args ...string) (string, error) {
	t.Helper()
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	t.Setenv("RELAYQ_SERVER", server.URL)
	var stdout, stderr bytes.Buffer
	err := run(args, &stdout, &stderr)
	return stdout.String(), err
}

func TestEnqueueSendsOptions(t *testing.T) {
	stub := &stubAPI{status: 201, reply: `{"id":"abc","duplicate":false}`}
	out, err := runCLI(t, stub, "enqueue", "-queue", "mail", "-type", "send",
		"-payload", `{"to":"a"}`, "-priority", "9", "-max-attempts", "3",
		"-delay", "90s", "-idempotency-key", "k1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "abc" {
		t.Errorf("output %q", out)
	}
	if stub.method != "POST" || stub.path != "/v1/queues/mail/jobs" {
		t.Errorf("request %s %s", stub.method, stub.path)
	}
	for key, want := range map[string]any{
		"type": "send", "priority": 9.0, "max_attempts": 3.0, "delay": "1m30s", "idempotency_key": "k1",
	} {
		if stub.body[key] != want {
			t.Errorf("%s = %v, want %v", key, stub.body[key], want)
		}
	}
}

func TestEnqueueZeroPriorityIsSent(t *testing.T) {
	stub := &stubAPI{status: 201, reply: `{"id":"abc"}`}
	if _, err := runCLI(t, stub, "enqueue", "-queue", "q", "-type", "t", "-priority", "0"); err != nil {
		t.Fatal(err)
	}
	if got, ok := stub.body["priority"]; !ok || got != 0.0 {
		t.Fatalf("priority %v present %v", got, ok)
	}
}

func TestEnqueueReportsDuplicate(t *testing.T) {
	stub := &stubAPI{reply: `{"id":"abc","duplicate":true}`}
	out, err := runCLI(t, stub, "enqueue", "-queue", "q", "-type", "t")
	if err != nil || !strings.Contains(out, "duplicate") {
		t.Fatalf("%q %v", out, err)
	}
}

func TestEnqueueRejectsBadInputBeforeCallingTheServer(t *testing.T) {
	cases := [][]string{
		{"enqueue", "-type", "t"},
		{"enqueue", "-queue", "q"},
		{"enqueue", "-queue", "q", "-type", "t", "-payload", "{"},
	}
	for _, args := range cases {
		stub := &stubAPI{}
		if _, err := runCLI(t, stub, args...); err == nil {
			t.Errorf("%v accepted", args)
		}
		if stub.method != "" {
			t.Errorf("%v reached the server", args)
		}
	}
}

func TestServerErrorIsShown(t *testing.T) {
	stub := &stubAPI{status: 404, reply: `{"error":{"code":"not_found","message":"no such job"}}`}
	_, err := runCLI(t, stub, "job", "-queue", "q", "abc")
	if err == nil || !strings.Contains(err.Error(), "no such job") {
		t.Fatalf("got %v", err)
	}
}

func TestNonJSONServerErrorIsShown(t *testing.T) {
	stub := &stubAPI{status: 502, reply: "bad gateway"}
	_, err := runCLI(t, stub, "stats")
	if err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("got %v", err)
	}
}

func TestFlagsAfterPositionalArguments(t *testing.T) {
	stub := &stubAPI{reply: `{"id":"abc"}`}
	if _, err := runCLI(t, stub, "job", "abc", "-queue", "q"); err != nil {
		t.Fatal(err)
	}
	if stub.path != "/v1/queues/q/jobs/abc" {
		t.Fatalf("path %s", stub.path)
	}
}

func TestPathsAreEscaped(t *testing.T) {
	stub := &stubAPI{status: 404, reply: `{"error":{"code":"not_found","message":"x"}}`}
	_, _ = runCLI(t, stub, "job", "-queue", "q", "a/../b")
	if strings.Contains(stub.path, "../") {
		t.Fatalf("id not escaped: %s", stub.path)
	}
}

func TestStatsTable(t *testing.T) {
	stub := &stubAPI{reply: `{"queues":[{"queue":"mail","ready":3,"counters":{"completed":7,"redelivered":2}}]}`}
	out, err := runCLI(t, stub, "stats")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"mail", "COMPLETED", "7", "2"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %q", want, out)
		}
	}
}

func TestDeadListShowsFirstErrorLine(t *testing.T) {
	stub := &stubAPI{reply: `{"jobs":[{"id":"a","type":"t","state":"dead","attempt":5,"max_attempts":5,"last_error":"boom\nstack line"}]}`}
	out, err := runCLI(t, stub, "dead", "-queue", "q")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "boom") || strings.Contains(out, "stack line") {
		t.Fatalf("output %q", out)
	}
}

func TestRequeueOneAndAll(t *testing.T) {
	stub := &stubAPI{reply: `{"requeued":1}`}
	if _, err := runCLI(t, stub, "requeue", "-queue", "q", "abc"); err != nil {
		t.Fatal(err)
	}
	if stub.path != "/v1/queues/q/dead/abc/requeue" {
		t.Fatalf("path %s", stub.path)
	}
	if _, err := runCLI(t, stub, "requeue", "-queue", "q", "-all"); err != nil {
		t.Fatal(err)
	}
	if stub.path != "/v1/queues/q/dead/requeue" {
		t.Fatalf("path %s", stub.path)
	}
	if _, err := runCLI(t, stub, "requeue", "-queue", "q"); err == nil {
		t.Fatal("requeue with no id and no -all accepted")
	}
}

func TestTokenIsSent(t *testing.T) {
	t.Setenv("RELAYQ_API_TOKEN", "secret")
	stub := &stubAPI{reply: `{"queues":[]}`}
	if _, err := runCLI(t, stub, "stats"); err != nil {
		t.Fatal(err)
	}
	if stub.auth != "Bearer secret" {
		t.Fatalf("auth %q", stub.auth)
	}
}

func TestUnknownCommand(t *testing.T) {
	if err := run([]string{"nope"}, io.Discard, io.Discard); err == nil {
		t.Fatal("unknown command accepted")
	}
}

func TestNoArgumentsPrintsUsage(t *testing.T) {
	var stderr bytes.Buffer
	err := run(nil, io.Discard, &stderr)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(stderr.String(), "Usage") {
		t.Fatalf("err %v, stderr %q", err, stderr.String())
	}
}

func TestHelpGoesToStdout(t *testing.T) {
	var stdout bytes.Buffer
	if err := run([]string{"help"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Usage") {
		t.Fatal("no usage text")
	}
}
