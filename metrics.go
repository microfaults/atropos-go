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

	// faultInjectionsTotal counts every fault the interceptor actually starts,
	// labelled by fault type and injection point. This is the observable signal
	// that an armed fault (admin POST or manteion active_fault) is firing on
	// live traffic — request-duration histograms only show the side effect.
	faultInjectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_fault_injections_total",
		Help: "Total faults started by the interceptor, by type and injection point.",
	}, []string{"fault_type", "injection_point"})
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
		faultInjectionsTotal,
	)
}

// recordFaultInjection bumps the injection counter. Wired into the default
// interceptor as its injection hook (see init.go / Configure).
func recordFaultInjection(faultType, injectionPoint string) {
	faultInjectionsTotal.WithLabelValues(faultType, injectionPoint).Inc()
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics.
// It merges atropos-specific metrics with the default Go runtime/process metrics.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{atroposRegistry, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{},
	)
}
