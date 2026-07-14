package atropos

import (
	"net/http"
	"strconv"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// ingressMiddleware composes otelhttp request spans with fault injection and
// records Prometheus metrics (request count, duration histogram). Serve
// wraps the business handler with it; control paths are mounted beside it.
func ingressMiddleware(next http.Handler, serviceName string, i *interceptor.Interceptor) http.Handler {
	// Inner: fault injection check (creates fault span as child).
	faulted := i.IngressMiddleware(next)
	// Middle: otelhttp request span (becomes parent of fault span).
	traced := otelhttp.NewHandler(faulted, serviceName)
	// Outer: metrics recording.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		start := time.Now()

		traced.ServeHTTP(rec, r)

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(rec.statusCode)
		httpServerRequestDuration.WithLabelValues(r.Method, status, serviceName).Observe(duration)
		httpServerRequestsTotal.WithLabelValues(r.Method, status, serviceName).Inc()
	})
}

// EgressTransport composes otelhttp client spans with fault injection and
// cache-box dispatch for a service's own outbound calls:
//
//	client := &http.Client{Transport: atropos.EgressTransport(nil)}
//
// A nil base uses http.DefaultTransport. The interceptor is resolved per
// round-trip, so a client built before Serve runs still picks up the
// evaluator and cache-box Serve installs.
func EgressTransport(base http.RoundTripper) http.RoundTripper {
	return egressTransport(base, currentInterceptor)
}

// egressTransport is EgressTransport with an explicit interceptor resolver —
// the seam tests use to pin a private interceptor.
func egressTransport(base http.RoundTripper, resolve func() *interceptor.Interceptor) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	// Inner: fault injection check, resolved per call (creates fault span
	// as child of the otelhttp client span).
	faulted := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return resolve().EgressTransport(base).RoundTrip(r)
	})
	// Middle: otelhttp client span (becomes parent of fault span).
	traced := otelhttp.NewTransport(faulted)
	// Outer: metrics recording.
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
		httpClientRequestDuration.WithLabelValues(r.Method, status, target).Observe(duration)
		httpClientRequestsTotal.WithLabelValues(r.Method, status, target).Inc()

		// Classification order matters: a fail-closed miss carries BOTH the
		// miss marker and the mode header, so the miss check must run first
		// or every counted-503 miss inflates the hit counter.
		if resp != nil {
			switch {
			case resp.Header.Get(interceptor.HeaderCacheMiss) != "":
				cacheBoxMissesTotal.Inc()
			case resp.Header.Get(interceptor.HeaderCacheMode) != "":
				cacheBoxHitsTotal.Inc()
			case resp.Header.Get(interceptor.HeaderCacheKey) != "":
				cacheBoxRecordsTotal.Inc()
			}
		}

		return resp, err
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.written {
		r.statusCode = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.statusCode = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
