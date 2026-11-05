//go:build ignore

// Mutation check: break one guarantee at a time and require a named test to fail.
// A guarantee whose mutant survives is a guarantee nothing protects.
//
//	go run test/mutation/main.go
//
// Each mutant edits a source file, runs the tests that should notice, then puts
// the original bytes back. It needs Docker, like the integration tests.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type edit struct{ file, from, to string }

type mutant struct {
	name  string
	edits []edit
	tags  string
	pkg   string
	test  string
}

var mutants = []mutant{
	{
		name: "delivery token is not checked",
		edits: []edit{{"scripts/common.lua",
			"if not values[1] or values[2] ~= token then", "if not values[1] then"}},
		tags: "integration", pkg: ".", test: "TestStaleDelivery",
	},
	{
		name: "scores go through tostring",
		edits: []edit{{"scripts/common.lua",
			"return string.format('%.0f', n)", "return tostring(n)"}},
		tags: "integration", pkg: ".", test: "TestFIFOWithinPriority",
	},
	{
		name: "attempts are not counted at delivery",
		edits: []edit{{"scripts/dequeue.lua",
			"redis.call('HINCRBY', key, 'attempts', 1)", ""}},
		tags: "failure", pkg: "./test/failure/", test: "TestPoisonJob",
	},
	{
		name: "expired leases are never reclaimed",
		edits: []edit{{"scripts/maintain.lua",
			"local expired = redis.call('ZRANGEBYSCORE', K_ACTIVE, '-inf', num(now), 'LIMIT', 0, limit)",
			"local expired = {}"}},
		tags: "failure", pkg: "./test/failure/", test: "TestKilledWorker",
	},
	{
		name: "idempotency key is ignored",
		edits: []edit{{"scripts/enqueue.lua",
			"existing = redis.call('GET', IDEM_PREFIX .. idem)", "existing = false"}},
		tags: "integration", pkg: ".", test: "TestIdempotencyKeyCreatesOneJob",
	},
	{
		name: "release does not refund the attempt",
		edits: []edit{{"scripts/release.lua",
			"redis.call('HINCRBY', key, 'attempts', -1)", ""}},
		tags: "integration", pkg: ".", test: "TestShutdownReleasesJob",
	},
	{
		name: "heartbeats do not renew the lease",
		edits: []edit{{"scripts/extend.lua",
			"redis.call('ZADD', K_ACTIVE, 'XX', num(deadline), id)", ""}},
		tags: "integration", pkg: ".", test: "TestHeartbeatKeepsALongJob",
	},
	{
		name: "exhausted jobs retry forever",
		edits: []edit{{"scripts/fail.lua",
			"if permanent or tonumber(values[1]) >= tonumber(values[2]) then", "if permanent then"}},
		tags: "integration", pkg: ".", test: "TestExhaustedJobGoesToDeadLetter",
	},
	{
		name: "shutdown cancels handlers at once",
		edits: []edit{{"worker.go",
			"if waitFor(finished, w.cfg.ShutdownTimeout) {", "if waitFor(finished, 0) {"}},
		tags: "integration", pkg: ".", test: "TestShutdownFinishesInFlightJob",
	},
	{
		// The window this opens is the whole reason dequeue is one Lua script: two
		// workers read the same id before either removes it.
		name: "dequeue reads then removes in two steps",
		edits: []edit{
			{"scripts/dequeue.lua",
				"local popped = redis.call('ZPOPMIN', K_READY, count)",
				"local popped = {}\nfor i = 6, #ARGV do popped[#popped + 1] = ARGV[i]; popped[#popped + 1] = '0' end"},
			{"client.go",
				"raw, err := c.run(ctx, scriptDequeue, queue, count, lease.Milliseconds(), worker).Slice()",
				`ready := newQueueKeys(c.namespace, queue).ready
	ids, err := c.rdb.ZRange(ctx, ready, 0, int64(count)-1).Result()
	if err != nil {
		return nil, err
	}
	dequeueArgs := []any{count, lease.Milliseconds(), worker}
	for _, id := range ids {
		c.rdb.ZRem(ctx, ready, id)
		dequeueArgs = append(dequeueArgs, id)
	}
	raw, err := c.run(ctx, scriptDequeue, queue, dequeueArgs...).Slice()`},
		},
		tags: "integration", pkg: ".", test: "TestManyConcurrentWorkersRunEachJobOnce",
	},
}

func apply(edits []edit) (restore func(), err error) {
	originals := map[string][]byte{}
	restore = func() {
		for file, content := range originals {
			if err := os.WriteFile(file, content, 0o644); err != nil {
				panic(err)
			}
		}
	}
	for _, e := range edits {
		content, err := os.ReadFile(e.file)
		if err != nil {
			return restore, err
		}
		if _, seen := originals[e.file]; !seen {
			originals[e.file] = content
		}
		if !strings.Contains(string(content), e.from) {
			return restore, fmt.Errorf("%s no longer contains %q", e.file, e.from)
		}
		updated := strings.Replace(string(content), e.from, e.to, 1)
		if err := os.WriteFile(e.file, []byte(updated), 0o644); err != nil {
			return restore, err
		}
	}
	return restore, nil
}

func main() {
	survived := 0
	for _, m := range mutants {
		restore, err := apply(m.edits)
		if err != nil {
			restore()
			fmt.Println("error:", err)
			os.Exit(2)
		}
		cmd := exec.Command("go", "test", "-count=1", "-tags", m.tags, "-timeout", "2m", "-run", m.test, m.pkg)
		out, err := cmd.CombinedOutput()
		restore()

		switch {
		case err == nil:
			survived++
			fmt.Printf("SURVIVED  %-42s %s\n", m.name, m.test)
		case strings.Contains(string(out), "[build failed]"):
			fmt.Printf("BROKEN    %s\n%s\n", m.name, out)
			os.Exit(2)
		default:
			fmt.Printf("caught    %-42s %s\n", m.name, m.test)
		}
	}
	if survived > 0 {
		fmt.Printf("%d mutants survived\n", survived)
		os.Exit(1)
	}
	fmt.Printf("all %d mutants caught\n", len(mutants))
}
