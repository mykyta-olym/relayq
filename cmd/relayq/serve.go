package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/mykyta-olym/relayq"
	"github.com/mykyta-olym/relayq/internal/httpapi"
)

func serveCommand(args []string, _, stderr io.Writer) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	addr := flags.String("addr", env("RELAYQ_ADDR", "127.0.0.1:8080"), "listen address")
	redisURL := flags.String("redis", env("RELAYQ_REDIS_URL", "redis://127.0.0.1:6379/0"), "Redis URL")
	namespace := flags.String("namespace", env("RELAYQ_NAMESPACE", relayq.DefaultNamespace), "Redis key prefix")
	if err := flags.Parse(args); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	options, err := redis.ParseURL(*redisURL)
	if err != nil {
		return fmt.Errorf("parse Redis URL: %w", err)
	}
	options.ContextTimeoutEnabled = true
	rdb := redis.NewClient(options)
	defer func() { _ = rdb.Close() }()
	client := relayq.NewClient(rdb, relayq.WithNamespace(*namespace))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(startup); err != nil {
		logger.Warn("Redis not reachable yet, requests will fail until it is", "error", err)
	}
	for _, warning := range client.CheckRedis(startup) {
		logger.Warn(warning)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(relayq.NewCollector(client), collectors.NewGoCollector())

	server := &http.Server{
		Addr: *addr,
		Handler: httpapi.New(client, httpapi.Config{
			Token:    os.Getenv("RELAYQ_API_TOKEN"),
			Gatherer: registry,
			Logger:   logger,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	failed := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}()

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	return server.Shutdown(shutdown)
}
