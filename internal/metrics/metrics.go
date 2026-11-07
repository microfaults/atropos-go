// Package metrics owns the atropos Prometheus registry and the series the
// HTTP middleware, the egress transport, the interceptor injection hook and
// (from M1) the daemon's proxy listeners record into. It is a leaf package so
// every host of the instrument core shares one set of series definitions.
//
// The atropos series carry a service label (the host's service name), and
// the request-duration histograms and the fault and cache-box counters carry
// trace_id/span_id exemplars of the sampled span they were recorded under.
// Handler negotiates OpenMetrics, the exposition format that shows them.
package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	oteltrace "go.opentelemetry.io/otel/trace"
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

	// The cache-box counters are labelled with the service whose egress
	// call was classified (the interceptor's service).
	CacheBoxHitsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_cachebox_hits_total",
		Help: "Total cache-box hits (replay served from cache).",
	}, []string{"service"})

	CacheBoxMissesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_cachebox_misses_total",
		Help: "Total cache-box misses (fell back to passthrough).",
	}, []string{"service"})

	CacheBoxRecordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_cachebox_records_total",
		Help: "Total cache-box records (passthrough responses captured).",
	}, []string{"service"})

	// FaultInjectionsTotal counts every fault the interceptor actually
	// starts, labelled by fault type, injection point and service. This is the
	// observable signal that an armed fault (admin POST or manteion
	// active_fault) is firing on live traffic — request-duration histograms
	// only show the side effect.
	FaultInjectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_fault_injections_total",
		Help: "Total faults started by the interceptor, by type, injection point and service.",
	}, []string{"fault_type", "injection_point", "service"})

	// ProxyOverhead is the time a request spends inside the atroposd proxy
	// excluding the upstream round trip: evaluation, keying, buffering and
	// copying. The G-proxy validity gate (design doc §5.13) reads its p99.
	ProxyOverhead = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "atropos_proxy_overhead_seconds",
		Help:    "Time spent inside the atroposd proxy, excluding the upstream round trip.",
		Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25},
	}, []string{"point"})

	// ProxyInflight is the number of requests currently inside the proxy.
	ProxyInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "atropos_proxy_inflight",
		Help: "Requests currently inside the atroposd proxy.",
	}, []string{"point"})

	// RulesUnsupportedTotal counts rules a host skipped because it cannot
	// execute their category or rule kind: a fault category outside its
	// capabilities, a fault it lacks the means to run within a category it
	// supports (a resource quota in-process; a network host=inline fault
	// without an inline toxic host, host=proxy without a resolver), or a
	// composition. The category label carries that category or kind (design
	// doc §5.3, D7: no silent drops).
	RulesUnsupportedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_rules_unsupported_total",
		Help: "Rules skipped because this host cannot execute their category or rule kind.",
	}, []string{"category", "service"})

	// ReplaySetState is the replay-set pull state of this instance: 1 on the
	// state it is in (fetching, committed or failed, the wire.ReplaySet*
	// values) and 0 on the other two. All three read 0 when no replay set is
	// desired. Set it through SetReplaySetState.
	ReplaySetState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "atropos_replay_set_state",
		Help: "Replay-set pull state of this instance (1 for the current state).",
	}, []string{"state", "service"})

	// SyncGeneration is the generation of the desired state this instance
	// holds: the one its last successful apply installed, or the one a 304
	// confirmed unchanged. manteion's convergence barrier waits on the same
	// number (observed_generation), so the two can be compared directly.
	SyncGeneration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "atropos_sync_generation",
		Help: "Generation of the desired state this instance holds.",
	}, []string{"service"})
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
		ProxyOverhead,
		ProxyInflight,
		RulesUnsupportedTotal,
		ReplaySetState,
		SyncGeneration,
	)
}

// FaultInjectionHook returns the interceptor.InjectionHook a host installs:
// it counts each started fault under service, with the exemplar of the span
// in ctx (the request the fault was started for) when it is sampled.
func FaultInjectionHook(service string) func(ctx context.Context, faultType, injectionPoint string) {
	return func(ctx context.Context, faultType, injectionPoint string) {
		IncWithExemplar(FaultInjectionsTotal.WithLabelValues(faultType, injectionPoint, service), Exemplar(ctx))
	}
}

// Exemplar returns the trace_id/span_id exemplar labels of the span in ctx
// when its span context is valid and sampled, nil otherwise: an unsampled
// span's trace is never stored, so an exemplar would point at nothing. The
// two labels total 63 runes, under prometheus.ExemplarMaxRunes.
func Exemplar(ctx context.Context) prometheus.Labels {
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() || !sc.IsSampled() {
		return nil
	}
	return prometheus.Labels{"trace_id": sc.TraceID().String(), "span_id": sc.SpanID().String()}
}

// ObserveWithExemplar observes v on o with the exemplar ex when o supports
// exemplars, and plainly otherwise. A nil ex leaves the exemplar already
// held in place (the client_golang contract).
func ObserveWithExemplar(o prometheus.Observer, v float64, ex prometheus.Labels) {
	if eo, ok := o.(prometheus.ExemplarObserver); ok {
		eo.ObserveWithExemplar(v, ex)
		return
	}
	o.Observe(v)
}

// IncWithExemplar adds one to c with the exemplar ex when c supports
// exemplars, and plainly otherwise. A nil ex leaves the exemplar already
// held in place.
func IncWithExemplar(c prometheus.Counter, ex prometheus.Labels) {
	if ea, ok := c.(prometheus.ExemplarAdder); ok {
		ea.AddWithExemplar(1, ex)
		return
	}
	c.Inc()
}

// replaySetStates are the values of ReplaySetState's state label, spelled as
// the wire.ReplaySet* constants (this package stays a leaf).
var replaySetStates = [...]string{"fetching", "committed", "failed"}

// SetReplaySetState makes state the current replay-set pull state of
// service: fetching, committed and failed go to 0, then state, when
// non-empty, to 1. "" leaves all three at 0: no replay set is desired.
func SetReplaySetState(service, state string) {
	for _, s := range replaySetStates {
		ReplaySetState.WithLabelValues(s, service).Set(0)
	}
	if state != "" {
		ReplaySetState.WithLabelValues(state, service).Set(1)
	}
}

// Handler serves Prometheus metrics: the atropos series plus the default Go
// runtime/process series. It negotiates OpenMetrics (a scrape that sends
// Accept: application/openmetrics-text), the format that exposes exemplars;
// other scrapes get the classic text format, without them.
func Handler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{Registry, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{EnableOpenMetrics: true},
	)
}
