package relayq

import (
	"embed"
	"fmt"
	"regexp"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptFiles embed.FS

// Each script is one atomic state transition. common.lua is prepended instead of
// loaded as a Redis function so plain EVALSHA works on any supported server.
var (
	scriptEnqueue  = loadScript("enqueue")
	scriptDequeue  = loadScript("dequeue")
	scriptComplete = loadScript("complete")
	scriptFail     = loadScript("fail")
	scriptRelease  = loadScript("release")
	scriptExtend   = loadScript("extend")
	scriptMaintain = loadScript("maintain")
	scriptRequeue  = loadScript("requeue")
	scriptDelete   = loadScript("delete")
)

func loadScript(name string) *redis.Script {
	common, err := scriptFiles.ReadFile("scripts/common.lua")
	if err != nil {
		panic(err)
	}
	body, err := scriptFiles.ReadFile("scripts/" + name + ".lua")
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(common) + "\n" + string(body))
}

// queueKeys names the Redis keys of one queue.
type queueKeys struct {
	ready, delayed, active, dead, completed string
	seq, stats, queues                      string
	jobPrefix, idemPrefix, wake             string
}

func newQueueKeys(namespace, queue string) queueKeys {
	base := namespace + ":q:" + queue + ":"
	return queueKeys{
		ready:      base + "ready",
		delayed:    base + "delayed",
		active:     base + "active",
		dead:       base + "dead",
		completed:  base + "completed",
		seq:        base + "seq",
		stats:      base + "stats",
		queues:     namespace + ":queues",
		jobPrefix:  base + "job:",
		idemPrefix: base + "idem:",
		wake:       base + "wake",
	}
}

// list returns the keys in the order common.lua expects.
func (k queueKeys) list() []string {
	return []string{k.ready, k.delayed, k.active, k.dead, k.completed, k.seq, k.stats, k.queues}
}

func (k queueKeys) forState(s State) (string, error) {
	switch s {
	case StateReady:
		return k.ready, nil
	case StateDelayed:
		return k.delayed, nil
	case StateActive:
		return k.active, nil
	case StateDead:
		return k.dead, nil
	case StateCompleted:
		return k.completed, nil
	}
	return "", fmt.Errorf("%w: unknown state %q", ErrInvalid, s)
}

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// validateQueue keeps queue names safe to splice into Redis keys.
func validateQueue(queue string) error {
	if !queueNamePattern.MatchString(queue) {
		return fmt.Errorf("%w: queue name must match %s", ErrInvalid, queueNamePattern)
	}
	return nil
}
