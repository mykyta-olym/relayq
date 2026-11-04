// Package relayq is a background job queue backed by Redis.
//
// A Client enqueues and inspects jobs. A Worker leases jobs, runs handlers and
// records results. Every change of job state is one Lua script, so it is atomic
// on the Redis server, and a lease is checked against a delivery token, so a
// stalled worker cannot change a job that has moved on.
//
// Delivery is at least once. The README lists exactly when a job can run twice.
package relayq
