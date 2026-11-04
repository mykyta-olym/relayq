package relayq

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector exports queue depth and lifetime counters from Redis at scrape time.
// Register it in any process that serves /metrics. The numbers come from Redis,
// so they cover all workers, and a worker restart does not reset them.
type Collector struct {
	client  *Client
	timeout time.Duration
	depth   *prometheus.Desc
	events  *prometheus.Desc
	up      *prometheus.Desc
}

// NewCollector creates a Collector.
func NewCollector(client *Client) *Collector {
	return &Collector{
		client:  client,
		timeout: 3 * time.Second,
		depth: prometheus.NewDesc("relayq_queue_jobs",
			"Jobs currently in each state.", []string{"queue", "state"}, nil),
		events: prometheus.NewDesc("relayq_jobs_total",
			"Lifetime job events, stored in Redis.", []string{"queue", "event"}, nil),
		up: prometheus.NewDesc("relayq_redis_up",
			"1 if the last scrape could read Redis.", nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.events
	ch <- c.up
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	up := 1.0
	queues, err := c.client.Queues(ctx)
	if err != nil {
		up = 0
	}
	for _, queue := range queues {
		stats, err := c.client.Stats(ctx, queue)
		if err != nil {
			up = 0
			continue
		}
		for state, n := range map[State]int64{
			StateReady:     stats.Ready,
			StateDelayed:   stats.Delayed,
			StateActive:    stats.Active,
			StateDead:      stats.Dead,
			StateCompleted: stats.Completed,
		} {
			ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(n), queue, string(state))
		}
		for event, n := range stats.Counters {
			ch <- prometheus.MustNewConstMetric(c.events, prometheus.CounterValue, float64(n), queue, event)
		}
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, up)
}

// workerMetrics are per process. Unregistered collectors still work, so the
// worker code has no nil checks.
type workerMetrics struct {
	duration  *prometheus.HistogramVec
	inFlight  prometheus.Gauge
	leaseLost prometheus.Counter
	panics    prometheus.Counter
}

func newWorkerMetrics(reg prometheus.Registerer, queue string) (*workerMetrics, error) {
	labels := prometheus.Labels{"queue": queue}
	m := &workerMetrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "relayq_worker_handler_duration_seconds",
			Help:        "Handler run time by job type and outcome.",
			ConstLabels: labels,
			Buckets:     []float64{.001, .005, .01, .05, .1, .5, 1, 5, 30, 120, 600},
		}, []string{"type", "outcome"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "relayq_worker_jobs_in_flight",
			Help:        "Handlers running in this process.",
			ConstLabels: labels,
		}),
		leaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "relayq_worker_leases_lost_total",
			Help:        "Deliveries this process lost to lease expiry.",
			ConstLabels: labels,
		}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "relayq_worker_handler_panics_total",
			Help:        "Handler panics recovered in this process.",
			ConstLabels: labels,
		}),
	}
	if reg == nil {
		return m, nil
	}
	for _, c := range []prometheus.Collector{m.duration, m.inFlight, m.leaseLost, m.panics} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *workerMetrics) handled(jobType, outcome string, took time.Duration) {
	m.duration.WithLabelValues(jobType, outcome).Observe(took.Seconds())
}
