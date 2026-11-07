package instrument

import (
	"net/http"
	"strconv"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

// IngressMiddleware is the ingress composition every host wraps business
// traffic in: the otelhttp server span (outer), Prometheus request metrics
// (middle, inside the span, so the duration exemplar names the server span),
// and the interceptor's fault check (inner, so the fault span is a child of
// the request span; it also evaluates the Transient point once the response
// is complete, so a transient fault span is a child of the request span
// too). Control paths are mounted beside it, never inside it.
func IngressMiddleware(next http.Handler, serviceName string, i *interceptor.Interceptor) http.Handler {
	faulted := i.IngressMiddleware(next)
	measured := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := interceptor.NewStatusRecorder(w)
		start := time.Now()

		faulted.ServeHTTP(rec, r)

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(rec.Status())
		ex := metrics.Exemplar(r.Context())
		metrics.ObserveWithExemplar(metrics.HTTPServerRequestDuration.WithLabelValues(r.Method, status, serviceName), duration, ex)
		metrics.HTTPServerRequestsTotal.WithLabelValues(r.Method, status, serviceName).Inc()
	})
	// The server span takes otelhttp's semconv name, "METHOD" or "METHOD
	// /route" when a ServeMux in front of it set the route pattern.
	return otelhttp.NewHandler(measured, serviceName)
}

// EgressTransport is the egress composition every host sends a service's
// outbound calls through: the otelhttp client span (outer), Prometheus client
// metrics plus cache-box hit/miss/record classification (middle, inside the
// span, so the exemplars name the client span), and the interceptor's fault /
// cache-box dispatch resolved per round-trip (inner). A nil base uses
// http.DefaultTransport.
//
// resolve is called per round-trip so a transport built before a host has
// installed its interceptor still picks it up afterwards.
func EgressTransport(base http.RoundTripper, resolve func() *interceptor.Interceptor) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	measured := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		ic := resolve()
		start := time.Now()
		resp, err := ic.EgressTransport(base).RoundTrip(r)
		duration := time.Since(start).Seconds()

		status := "error"
		if resp != nil {
			status = strconv.Itoa(resp.StatusCode)
		}
		target := r.URL.Host
		if target == "" {
			target = r.Host
		}
		ex := metrics.Exemplar(r.Context())
		metrics.ObserveWithExemplar(metrics.HTTPClientRequestDuration.WithLabelValues(r.Method, status, target), duration, ex)
		metrics.HTTPClientRequestsTotal.WithLabelValues(r.Method, status, target).Inc()

		// Classification order matters: a fail-closed miss carries BOTH the
		// miss marker and the mode header, so the miss check must run first
		// or every counted-503 miss inflates the hit counter.
		if resp != nil {
			switch {
			case resp.Header.Get(interceptor.HeaderCacheMiss) != "":
				metrics.IncWithExemplar(metrics.CacheBoxMissesTotal.WithLabelValues(ic.Service()), ex)
			case resp.Header.Get(interceptor.HeaderCacheMode) != "":
				metrics.IncWithExemplar(metrics.CacheBoxHitsTotal.WithLabelValues(ic.Service()), ex)
			case resp.Header.Get(interceptor.HeaderCacheKey) != "":
				metrics.IncWithExemplar(metrics.CacheBoxRecordsTotal.WithLabelValues(ic.Service()), ex)
			}
		}

		return resp, err
	})
	// Pin the client span to the global provider. otelhttp otherwise takes
	// the provider of whatever span is in the context, and a context that
	// only carries an extracted (remote) span — the sidecar's egress
	// listener, or a caller that propagated headers without starting a
	// span — resolves to the no-op provider, which mints no child span and
	// re-emits the caller's span id unchanged. The global delegate is safe
	// to capture before InitTelemetry runs: it forwards once set.
	return otelhttp.NewTransport(measured, otelhttp.WithTracerProvider(otel.GetTracerProvider()))
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
