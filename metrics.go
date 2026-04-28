package atropos

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var atroposRegistry = prometheus.NewRegistry()

var (
	httpServerRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_request_duration_seconds",
		Help:    "Duration of HTTP server requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status_code", "service"})

	httpServerRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_server_requests_total",
		Help: "Total HTTP server requests.",
	}, []string{"method", "status_code", "service"})

	httpClientRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_client_request_duration_seconds",
		Help:    "Duration of HTTP client requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status_code", "target"})

	httpClientRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_client_requests_total",
		Help: "Total HTTP client requests.",
	}, []string{"method", "status_code", "target"})

	cacheBoxHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_hits_total",
		Help: "Total cache-box hits (replay served from cache).",
	})

	cacheBoxMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_misses_total",
		Help: "Total cache-box misses (fell back to passthrough).",
	})

	cacheBoxRecordsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_records_total",
		Help: "Total cache-box records (passthrough responses captured).",
	})
)

func init() {
	atroposRegistry.MustRegister(
		httpServerRequestDuration,
		httpServerRequestsTotal,
		httpClientRequestDuration,
		httpClientRequestsTotal,
		cacheBoxHitsTotal,
		cacheBoxMissesTotal,
		cacheBoxRecordsTotal,
	)
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics.
// It merges atropos-specific metrics with the default Go runtime/process metrics.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{atroposRegistry, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{},
	)
}
