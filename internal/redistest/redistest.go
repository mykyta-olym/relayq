// Package redistest runs real Redis servers in Docker for tests. Failure tests
// need to stop and kill the server, so a fake or an in-process Redis would not do.
package redistest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultImage is the Redis image the tests run against. Set RELAYQ_TEST_IMAGE to
// run the same suite against another server.
const DefaultImage = "redis:7.4-alpine"

// Label marks everything these tests start, for cleanup after a crashed run:
//
//	docker ps -aq --filter label=relayq-test | xargs docker rm -f
const Label = "relayq-test=1"

// Server is one Redis container with a named volume for /data.
type Server struct {
	Addr      string
	Image     string
	name      string
	volume    string
	args      []string
	container bool
}

// Start launches a container and waits until it answers PING. Args are passed to
// redis-server. If RELAYQ_TEST_REDIS_ADDR is set and allowExternal is true, that
// server is used instead and no container starts.
func Start(allowExternal bool, args ...string) (*Server, error) {
	if addr := os.Getenv("RELAYQ_TEST_REDIS_ADDR"); addr != "" && allowExternal {
		s := &Server{Addr: addr}
		return s, s.waitReady(30 * time.Second)
	}

	port, err := freePort()
	if err != nil {
		return nil, err
	}
	id := randomID()
	image := os.Getenv("RELAYQ_TEST_IMAGE")
	if image == "" {
		image = DefaultImage
	}
	s := &Server{
		Addr:      fmt.Sprintf("127.0.0.1:%d", port),
		Image:     image,
		name:      "relayq-test-" + id,
		volume:    "relayq-test-" + id,
		args:      args,
		container: true,
	}
	run := []string{
		"run", "-d", "--name", s.name, "--label", Label,
		"-p", s.Addr + ":6379", "-v", s.volume + ":/data",
		image, serverBinary(image),
	}
	run = append(run, args...)
	if _, err := docker(run...); err != nil {
		return nil, err
	}
	if err := s.waitReady(60 * time.Second); err != nil {
		s.Remove()
		return nil, err
	}
	return s, nil
}

// serverBinary lets the same suite run against the Valkey fork.
func serverBinary(image string) string {
	if strings.Contains(image, "valkey") {
		return "valkey-server"
	}
	return "redis-server"
}

// Client returns a new connection pool to the server.
func (s *Server) Client() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                  s.Addr,
		ContextTimeoutEnabled: true,
		PoolSize:              64,
	})
}

// GracefulRestart stops the server with SIGTERM, which lets it flush, then starts
// it again.
func (s *Server) GracefulRestart() error {
	if _, err := docker("stop", "-t", "30", s.name); err != nil {
		return err
	}
	return s.startAgain()
}

// CrashRestart sends SIGKILL to the server, then starts it again. Nothing gets a
// chance to flush. What survives is what had already reached the kernel.
func (s *Server) CrashRestart() error {
	if err := s.Kill(); err != nil {
		return err
	}
	return s.startAgain()
}

// Kill sends SIGKILL and leaves the server down.
func (s *Server) Kill() error {
	_, err := docker("kill", "--signal", "KILL", s.name)
	return err
}

// Pause freezes the server. Connections stay open and nothing answers, which is
// what a worker sees during a network partition.
func (s *Server) Pause() error {
	_, err := docker("pause", s.name)
	return err
}

// Unpause resumes a frozen server.
func (s *Server) Unpause() error {
	_, err := docker("unpause", s.name)
	return err
}

// StartAgain brings a killed server back.
func (s *Server) StartAgain() error { return s.startAgain() }

func (s *Server) startAgain() error {
	if _, err := docker("start", s.name); err != nil {
		return err
	}
	return s.waitReady(60 * time.Second)
}

// Remove deletes the container and its volume. It is safe to call twice.
func (s *Server) Remove() {
	if !s.container {
		return
	}
	_, _ = docker("rm", "-f", "-v", s.name)
	_, _ = docker("volume", "rm", "-f", s.volume)
}

// Version returns the server version string.
func (s *Server) Version() (string, error) {
	client := s.Client()
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(info) {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			return value, nil
		}
	}
	return "", errors.New("no redis_version in INFO")
}

func (s *Server) waitReady(timeout time.Duration) error {
	client := s.Client()
	defer func() { _ = client.Close() }()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		last = client.Ping(ctx).Err()
		cancel()
		if last == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("redis at %s not ready: %w", s.Addr, last)
}

func docker(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func randomID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
