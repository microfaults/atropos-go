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
// traffic in: Prometheus request metrics (outer), the otelhttp server span
// (middle), and the interceptor's fault check (inner, so the fault span is
// a child of the request span; it also evaluates the Transient point once
// the response is complete, so a transient fault span is a child of the
// request span too). Control paths are mounted beside it, never inside it.
func IngressMiddleware(next http.Handler, serviceName string, i *interceptor.Interceptor) http.Handler {
	faulted := i.IngressMiddleware(next)
	traced := otelhttp.NewHandler(faulted, serviceName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := interceptor.NewStatusRecorder(w)
		start := time.Now()

		traced.ServeHTTP(rec, r)

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(rec.Status())
		metrics.HTTPServerRequestDuration.WithLabelValues(r.Method, status, serviceName).Observe(duration)
		metrics.HTTPServerRequestsTotal.WithLabelValues(r.Method, status, serviceName).Inc()
	})
}

// EgressTransport is the egress composition every host sends a service's
// outbound calls through: the otelhttp client span (middle), the
// interceptor's fault / cache-box dispatch resolved per round-trip (inner),
// and Prometheus client metrics plus cache-box hit/miss/record
// classification (outer). A nil base uses http.DefaultTransport.
//
// resolve is called per round-trip so a transport built before a host has
// installed its interceptor still picks it up afterwards.
func EgressTransport(base http.RoundTripper, resolve func() *interceptor.Interceptor) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	faulted := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return resolve().EgressTransport(base).RoundTrip(r)
	})
	// Pin the client span to the global provider. otelhttp otherwise takes
	// the provider of whatever span is in the context, and a context that
	// only carries an extracted (remote) span — the sidecar's egress
	// listener, or a caller that propagated headers without starting a
	// span — resolves to the no-op provider, which mints no child span and
	// re-emits the caller's span id unchanged. The global delegate is safe
	// to capture before InitTelemetry runs: it forwards once set.
	traced := otelhttp.NewTransport(faulted, otelhttp.WithTracerProvider(otel.GetTracerProvider()))
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		start := time.Now()
		resp, err := traced.RoundTrip(r)
		duration := time.Since(start).Seconds()

		status := "error"
		if resp != nil {
			status = strconv.Itoa(resp.StatusCode)
		}
		target := r.URL.Host
		if target == "" {
			target = r.Host
		}
		metrics.HTTPClientRequestDuration.WithLabelValues(r.Method, status, target).Observe(duration)
		metrics.HTTPClientRequestsTotal.WithLabelValues(r.Method, status, target).Inc()

		// Classification order matters: a fail-closed miss carries BOTH the
		// miss marker and the mode header, so the miss check must run first
		// or every counted-503 miss inflates the hit counter.
		if resp != nil {
			switch {
			case resp.Header.Get(interceptor.HeaderCacheMiss) != "":
				metrics.CacheBoxMissesTotal.Inc()
			case resp.Header.Get(interceptor.HeaderCacheMode) != "":
				metrics.CacheBoxHitsTotal.Inc()
			case resp.Header.Get(interceptor.HeaderCacheKey) != "":
				metrics.CacheBoxRecordsTotal.Inc()
			}
		}

		return resp, err
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
