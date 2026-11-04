// Command relayq runs the HTTP API and is a small client for it.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const usage = `relayq is a job queue on Redis.

Usage:
  relayq serve                       run the HTTP API
  relayq enqueue -queue q -type t    add a job
  relayq job -queue q ID             show one job
  relayq jobs -queue q -state s      list jobs in a state
  relayq stats [-queue q]            show queue depths and counters
  relayq dead -queue q               list dead jobs
  relayq requeue -queue q ID         move a dead job back to ready
  relayq requeue -queue q -all       move every dead job back to ready
  relayq delete -queue q ID          delete a job that no worker holds

Client commands talk to the API at -server, or $RELAYQ_SERVER, and send
$RELAYQ_API_TOKEN as a bearer token when it is set.
Run "relayq COMMAND -h" for the flags of a command.
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "relayq:", err)
		}
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return flag.ErrHelp
	}
	commands := map[string]func([]string, io.Writer, io.Writer) error{
		"serve":   serveCommand,
		"enqueue": enqueueCommand,
		"job":     jobCommand,
		"jobs":    jobsCommand,
		"stats":   statsCommand,
		"dead":    deadCommand,
		"requeue": requeueCommand,
		"delete":  deleteCommand,
	}
	command, ok := commands[args[0]]
	if !ok {
		if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
			_, _ = fmt.Fprint(stdout, usage)
			return nil
		}
		return fmt.Errorf("unknown command %q, run relayq help", args[0])
	}
	return command(args[1:], stdout, stderr)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
