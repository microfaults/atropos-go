// Package metrics owns the atropos Prometheus registry and the series the
// HTTP middleware, the egress transport, the interceptor injection hook and
// (from M1) the daemon's proxy listeners record into. It is a leaf package so
// every host of the instrument core shares one set of series definitions.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds the atropos-specific series. Handler merges it with the
// default Go runtime/process gatherer.
var Registry = prometheus.NewRegistry()

var (
	HTTPServerRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_request_duration_seconds",
		Help:    "Duration of HTTP server requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status_code", "service"})

	HTTPServerRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_server_requests_total",
		Help: "Total HTTP server requests.",
	}, []string{"method", "status_code", "service"})

	HTTPClientRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_client_request_duration_seconds",
		Help:    "Duration of HTTP client requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status_code", "target"})

	HTTPClientRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_client_requests_total",
		Help: "Total HTTP client requests.",
	}, []string{"method", "status_code", "target"})

	CacheBoxHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_hits_total",
		Help: "Total cache-box hits (replay served from cache).",
	})

	CacheBoxMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_misses_total",
		Help: "Total cache-box misses (fell back to passthrough).",
	})

	CacheBoxRecordsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_records_total",
		Help: "Total cache-box records (passthrough responses captured).",
	})

	// FaultInjectionsTotal counts every fault the interceptor actually
	// starts, labelled by fault type and injection point. This is the
	// observable signal that an armed fault (admin POST or manteion
	// active_fault) is firing on live traffic — request-duration histograms
	// only show the side effect.
	FaultInjectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_fault_injections_total",
		Help: "Total faults started by the interceptor, by type and injection point.",
	}, []string{"fault_type", "injection_point"})
)

func init() {
	Registry.MustRegister(
		HTTPServerRequestDuration,
		HTTPServerRequestsTotal,
		HTTPClientRequestDuration,
		HTTPClientRequestsTotal,
		CacheBoxHitsTotal,
		CacheBoxMissesTotal,
		CacheBoxRecordsTotal,
		FaultInjectionsTotal,
	)
}

// RecordFaultInjection is the interceptor.InjectionHook every host installs.
func RecordFaultInjection(faultType, injectionPoint string) {
	FaultInjectionsTotal.WithLabelValues(faultType, injectionPoint).Inc()
}

// Handler serves Prometheus metrics: the atropos series plus the default Go
// runtime/process series.
func Handler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{Registry, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{},
	)
}
