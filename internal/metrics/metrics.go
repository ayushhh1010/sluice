// Package metrics defines Sluice's Prometheus metrics.
package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

type Metrics struct {
	Reg *prometheus.Registry

	ProduceLatency *prometheus.HistogramVec // stage: buffer, put, commit, total
	FetchLatency   prometheus.Histogram
	FlushBytes     prometheus.Histogram
	ObjRequests    *prometheus.CounterVec // op, class, status
	ObjLatency     *prometheus.HistogramVec
	CacheHits      *prometheus.CounterVec // cache, result
	Epoch          prometheus.Gauge
	MetalogSeq     prometheus.Gauge
	AppendLatency  prometheus.Histogram
	Rebalances     prometheus.Counter
	CostMonth      prometheus.Gauge
	Connections    *prometheus.GaugeVec   // listener
	KafkaRequests  *prometheus.CounterVec // api, listener
	Throttled      *prometheus.CounterVec // direction
	Role           *prometheus.GaugeVec   // role
}

func New(process string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	cl := prometheus.Labels{"process": process}
	m := &Metrics{Reg: reg}
	m.ProduceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "sluice_produce_latency_seconds", Help: "Produce latency by stage.", Buckets: latencyBuckets, ConstLabels: cl}, []string{"stage"})
	m.FetchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "sluice_fetch_latency_seconds", Help: "Fetch latency including long-poll waits.", Buckets: latencyBuckets, ConstLabels: cl})
	m.FlushBytes = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "sluice_flush_bytes", Help: "Segment object size.", Buckets: prometheus.ExponentialBuckets(1024, 4, 9), ConstLabels: cl})
	m.ObjRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sluice_objstore_requests_total", Help: "Object store requests.", ConstLabels: cl}, []string{"op", "class", "status"})
	m.ObjLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "sluice_objstore_latency_seconds", Help: "Object store latency.", Buckets: latencyBuckets, ConstLabels: cl}, []string{"op"})
	m.CacheHits = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sluice_cache_hits_total", Help: "Cache lookups.", ConstLabels: cl}, []string{"cache", "result"})
	m.Epoch = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sluice_sequencer_epoch", Help: "Sequencer epoch.", ConstLabels: cl})
	m.MetalogSeq = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sluice_metalog_seq", Help: "Last applied metadata log entry.", ConstLabels: cl})
	m.AppendLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "sluice_metalog_append_seconds", Help: "Metadata log append latency.", Buckets: latencyBuckets, ConstLabels: cl})
	m.Rebalances = prometheus.NewCounter(prometheus.CounterOpts{Name: "sluice_group_rebalances_total", Help: "Consumer group rebalances.", ConstLabels: cl})
	m.CostMonth = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sluice_estimated_cost_usd_month", Help: "Object store request cost at the current rate, above the R2 free tier.", ConstLabels: cl})
	m.Connections = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "sluice_connections", Help: "Open Kafka connections.", ConstLabels: cl}, []string{"listener"})
	m.KafkaRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sluice_kafka_requests_total", Help: "Kafka requests by API.", ConstLabels: cl}, []string{"api", "listener"})
	m.Throttled = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sluice_quota_throttle_seconds_total", Help: "Time clients were throttled by quotas.", ConstLabels: cl}, []string{"direction"})
	m.Role = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "sluice_sequencer_role", Help: "1 for the current role.", ConstLabels: cl}, []string{"role"})
	for _, c := range []prometheus.Collector{m.ProduceLatency, m.FetchLatency, m.FlushBytes, m.ObjRequests, m.ObjLatency, m.CacheHits,
		m.Epoch, m.MetalogSeq, m.AppendLatency, m.Rebalances, m.CostMonth, m.Connections, m.KafkaRequests, m.Throttled, m.Role} {
		f(c)
	}
	return m
}

// Observe maps a core proc.Observe sample (milliseconds or bytes) to a metric.
func (m *Metrics) Observe(name string, v float64) {
	switch name {
	case "produce_buffer_ms":
		m.ProduceLatency.WithLabelValues("buffer").Observe(v / 1000)
	case "produce_put_ms":
		m.ProduceLatency.WithLabelValues("put").Observe(v / 1000)
	case "produce_commit_ms":
		m.ProduceLatency.WithLabelValues("commit").Observe(v / 1000)
	case "produce_total_ms":
		m.ProduceLatency.WithLabelValues("total").Observe(v / 1000)
	case "fetch_ms":
		m.FetchLatency.Observe(v / 1000)
	case "flush_bytes":
		m.FlushBytes.Observe(v)
	case "append_ms":
		m.AppendLatency.Observe(v / 1000)
	case "group_rebalance":
		m.Rebalances.Add(v)
	default:
		if strings.HasPrefix(name, "cache:") { // cache:<name>:<hit|miss>
			if parts := strings.Split(name, ":"); len(parts) == 3 {
				m.CacheHits.WithLabelValues(parts[1], parts[2]).Add(v)
			}
		}
	}
}

// Gauge maps a core proc.Gauge to a metric.
func (m *Metrics) Gauge(name string, v float64) {
	switch name {
	case "epoch":
		m.Epoch.Set(v)
	case "metalog_seq":
		m.MetalogSeq.Set(v)
	}
}
