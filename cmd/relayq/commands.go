package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mykyta-olym/relayq"
)

const requestTimeout = 30 * time.Second

// apiClient is the HTTP side of the CLI.
type apiClient struct {
	base  string
	token string
	http  *http.Client
}

// clientFlags adds the flags every client command shares. The returned function
// builds the client after parsing.
func clientFlags(flags *flag.FlagSet) func() *apiClient {
	server := flags.String("server", env("RELAYQ_SERVER", "http://127.0.0.1:8080"), "API base URL")
	return func() *apiClient {
		return &apiClient{
			base:  strings.TrimRight(*server, "/"),
			token: os.Getenv("RELAYQ_API_TOKEN"),
			http:  &http.Client{Timeout: requestTimeout},
		}
	}
}

// do sends a request and decodes a JSON reply into out.
func (c *apiClient) do(method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		var failure struct {
			Error struct{ Code, Message string }
		}
		if json.Unmarshal(raw, &failure) == nil && failure.Error.Message != "" {
			return fmt.Errorf("%s: %s", failure.Error.Code, failure.Error.Message)
		}
		return fmt.Errorf("server returned %s", res.Status)
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// parse handles interspersed flags, so "relayq job ID -queue q" works as well as
// "relayq job -queue q ID".
func parse(flags *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		if flags.NArg() == 0 {
			return positional, nil
		}
		positional = append(positional, flags.Arg(0))
		args = flags.Args()[1:]
	}
}

func oneID(positional []string) (string, error) {
	if len(positional) != 1 {
		return "", errors.New("expected exactly one job id")
	}
	return positional[0], nil
}

func requireQueue(queue string) error {
	if queue == "" {
		return errors.New("-queue is required")
	}
	return nil
}

func enqueueCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "queue name")
	jobType := flags.String("type", "", "job type")
	payload := flags.String("payload", "", "JSON payload")
	priority := flags.Int("priority", -1, "priority 0 to 9, higher runs first (default 5)")
	attempts := flags.Int("max-attempts", 0, "deliveries before the dead letter queue (default 5)")
	delay := flags.Duration("delay", 0, "run no sooner than this from now")
	runAt := flags.String("run-at", "", "run no sooner than this RFC 3339 time")
	timeout := flags.Duration("timeout", 0, "handler timeout (default: the worker's)")
	key := flags.String("idempotency-key", "", "deduplication key")
	if _, err := parse(flags, args); err != nil {
		return err
	}
	if err := requireQueue(*queue); err != nil {
		return err
	}
	if *jobType == "" {
		return errors.New("-type is required")
	}

	body := map[string]any{"type": *jobType}
	if *payload != "" {
		if !json.Valid([]byte(*payload)) {
			return errors.New("-payload is not valid JSON")
		}
		body["payload"] = json.RawMessage(*payload)
	}
	if *priority >= 0 {
		body["priority"] = *priority
	}
	if *attempts > 0 {
		body["max_attempts"] = *attempts
	}
	if *delay != 0 {
		body["delay"] = delay.String()
	}
	if *runAt != "" {
		body["run_at"] = *runAt
	}
	if *timeout != 0 {
		body["timeout"] = timeout.String()
	}
	if *key != "" {
		body["idempotency_key"] = *key
	}

	var res relayq.Enqueued
	if err := client().do("POST", "/v1/queues/"+url.PathEscape(*queue)+"/jobs", body, &res); err != nil {
		return err
	}
	if res.Duplicate {
		_, err := fmt.Fprintln(stdout, res.ID, "(duplicate, no new job)")
		return err
	}
	_, err := fmt.Fprintln(stdout, res.ID)
	return err
}

func jobCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("job", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "queue name")
	positional, err := parse(flags, args)
	if err != nil {
		return err
	}
	if err := requireQueue(*queue); err != nil {
		return err
	}
	id, err := oneID(positional)
	if err != nil {
		return err
	}

	var job json.RawMessage
	path := "/v1/queues/" + url.PathEscape(*queue) + "/jobs/" + url.PathEscape(id)
	if err := client().do("GET", path, nil, &job); err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, job, "", "  "); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, pretty.String())
	return err
}

type jobRow struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	State       string `json:"state"`
	Priority    int    `json:"priority"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"max_attempts"`
	LastError   string `json:"last_error"`
}

func printJobs(client *apiClient, stdout io.Writer, queue, state string, offset, limit int) error {
	values := url.Values{
		"state":  {state},
		"offset": {strconv.Itoa(offset)},
		"limit":  {strconv.Itoa(limit)},
	}
	var res struct{ Jobs []jobRow }
	path := "/v1/queues/" + url.PathEscape(queue) + "/jobs?" + values.Encode()
	if err := client.do("GET", path, nil, &res); err != nil {
		return err
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "ID\tTYPE\tSTATE\tPRIORITY\tATTEMPT\tLAST ERROR")
	for _, job := range res.Jobs {
		message, _, _ := strings.Cut(job.LastError, "\n")
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%d/%d\t%s\n",
			job.ID, job.Type, job.State, job.Priority, job.Attempt, job.MaxAttempts, message)
	}
	return table.Flush()
}

func jobsCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "queue name")
	state := flags.String("state", "ready", "ready, delayed, active, dead or completed")
	offset := flags.Int("offset", 0, "rows to skip")
	limit := flags.Int("limit", 50, "rows to show")
	if _, err := parse(flags, args); err != nil {
		return err
	}
	if err := requireQueue(*queue); err != nil {
		return err
	}
	return printJobs(client(), stdout, *queue, *state, *offset, *limit)
}

func deadCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("dead", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "queue name")
	limit := flags.Int("limit", 50, "rows to show")
	if _, err := parse(flags, args); err != nil {
		return err
	}
	if err := requireQueue(*queue); err != nil {
		return err
	}
	return printJobs(client(), stdout, *queue, "dead", 0, *limit)
}

func statsCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("stats", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "show one queue with all counters")
	if _, err := parse(flags, args); err != nil {
		return err
	}

	if *queue != "" {
		var stats relayq.QueueStats
		if err := client().do("GET", "/v1/queues/"+url.PathEscape(*queue), nil, &stats); err != nil {
			return err
		}
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		for _, row := range []struct {
			name  string
			value int64
		}{
			{"ready", stats.Ready}, {"delayed", stats.Delayed}, {"active", stats.Active},
			{"dead", stats.Dead}, {"completed", stats.Completed},
		} {
			_, _ = fmt.Fprintf(table, "%s\t%d\n", row.name, row.value)
		}
		_, _ = fmt.Fprintln(table, "\t")
		names := make([]string, 0, len(stats.Counters))
		for name := range stats.Counters {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			_, _ = fmt.Fprintf(table, "%s total\t%d\n", name, stats.Counters[name])
		}
		return table.Flush()
	}

	var res struct{ Queues []relayq.QueueStats }
	if err := client().do("GET", "/v1/queues", nil, &res); err != nil {
		return err
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "QUEUE\tREADY\tDELAYED\tACTIVE\tDEAD\tCOMPLETED\tREDELIVERED")
	for _, q := range res.Queues {
		_, _ = fmt.Fprintf(table, "%s\t%d\t%d\t%d\t%d\t%d\t%d\n",
			q.Queue, q.Ready, q.Delayed, q.Active, q.Dead, q.Counters["completed"], q.Counters["redelivered"])
	}
	return table.Flush()
}

func requeueCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("requeue", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "queue name")
	all := flags.Bool("all", false, "requeue every dead job")
	positional, err := parse(flags, args)
	if err != nil {
		return err
	}
	if err := requireQueue(*queue); err != nil {
		return err
	}

	path := "/v1/queues/" + url.PathEscape(*queue) + "/dead/requeue"
	if !*all {
		id, err := oneID(positional)
		if err != nil {
			return errors.New("give one job id, or -all")
		}
		path = "/v1/queues/" + url.PathEscape(*queue) + "/dead/" + url.PathEscape(id) + "/requeue"
	}
	var res struct{ Requeued int64 }
	if err := client().do("POST", path, nil, &res); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "requeued", res.Requeued)
	return err
}

func deleteCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("delete", flag.ContinueOnError)
	flags.SetOutput(stderr)
	client := clientFlags(flags)
	queue := flags.String("queue", "", "queue name")
	positional, err := parse(flags, args)
	if err != nil {
		return err
	}
	if err := requireQueue(*queue); err != nil {
		return err
	}
	id, err := oneID(positional)
	if err != nil {
		return err
	}
	path := "/v1/queues/" + url.PathEscape(*queue) + "/jobs/" + url.PathEscape(id)
	if err := client().do("DELETE", path, nil, nil); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "deleted", id)
	return err
}
