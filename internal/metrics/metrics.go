// Package metrics is the Prometheus surface. Every method is safe to call on a
// nil *Metrics, so a test can pass nil rather than build a registry it will not
// read.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	transfers      *prometheus.CounterVec
	relayBytes     prometheus.Counter
	throughput     prometheus.Histogram
	busPublish     prometheus.Histogram
	rendezvousWait prometheus.Histogram
}

// Options wires the gauges that read live state instead of being incremented.
type Options struct {
	// Streams is the number of live SSE Streams. A GaugeFunc rather than a
	// counter pair, because the registry already knows and two numbers that can
	// disagree are worse than one.
	Streams func() float64
	// Transfers is the number of Transfers this instance holds.
	Transfers func() float64
	// Parked is the number of Recipients waiting for a Sender.
	Parked func() float64
}

func New(o Options) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		transfers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hp_transfers_total",
			Help: "Transfers that reached each state.",
		}, []string{"state"}),

		relayBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hp_relay_bytes_total",
			Help: "Payload bytes relayed. Flat at zero means no bytes went through this process.",
		}),

		throughput: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "hp_relay_throughput_bytes",
			Help: "Bytes per second achieved by a completed relay.",
			// A megabyte a second to a gigabyte a second, which is the range
			// between a phone on hotel wifi and a loopback test.
			Buckets: prometheus.ExponentialBuckets(1<<20, 2, 11),
		}),

		busPublish: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "hp_bus_publish_seconds",
			Help:    "Time spent in Bus.Publish.",
			Buckets: prometheus.ExponentialBuckets(50e-6, 3, 10),
		}),

		rendezvousWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "hp_rendezvous_wait_seconds",
			Help:    "How long a parked Recipient waited for its Sender.",
			Buckets: prometheus.ExponentialBuckets(0.001, 3, 11),
		}),
	}

	m.registry.MustRegister(
		m.transfers, m.relayBytes, m.throughput, m.busPublish, m.rendezvousWait,
		// The Go collector is where goroutines, heap and GC pause come from,
		// which is most of what a load test wants to know.
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	if o.Streams != nil {
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hp_streams_active",
			Help: "Live SSE Streams held by this instance.",
		}, o.Streams))
	}
	if o.Transfers != nil {
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hp_transfers_active",
			Help: "Transfers this instance owns, including recently finished ones.",
		}, o.Transfers))
	}
	if o.Parked != nil {
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hp_rendezvous_parked",
			Help: "Recipients currently waiting for a Sender.",
		}, o.Parked))
	}
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) TransferReached(state string) {
	if m == nil {
		return
	}
	m.transfers.WithLabelValues(state).Inc()
}

func (m *Metrics) RelayedBytes(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.relayBytes.Add(float64(n))
}

func (m *Metrics) Throughput(bytesPerSecond float64) {
	if m == nil || bytesPerSecond <= 0 {
		return
	}
	m.throughput.Observe(bytesPerSecond)
}

func (m *Metrics) BusPublish(seconds float64) {
	if m == nil {
		return
	}
	m.busPublish.Observe(seconds)
}

func (m *Metrics) RendezvousWait(seconds float64) {
	if m == nil {
		return
	}
	m.rendezvousWait.Observe(seconds)
}
