package atropos

import (
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// MiddlewareOption configures IngressMiddleware or EgressTransport.
type MiddlewareOption interface {
	applyMiddleware(*middlewareConfig)
}

type middlewareConfig struct {
	interceptor *Interceptor
}

func defaultMiddlewareConfig() middlewareConfig {
	return middlewareConfig{
		interceptor: defaultInterceptor,
	}
}

type middlewareOptionFunc func(*middlewareConfig)

func (f middlewareOptionFunc) applyMiddleware(c *middlewareConfig) { f(c) }

// WithInterceptor overrides the default package-level interceptor
// for this middleware instance.
func WithInterceptor(i *Interceptor) MiddlewareOption {
	return middlewareOptionFunc(func(c *middlewareConfig) { c.interceptor = i })
}

// IngressMiddleware composes otelhttp request spans with fault injection and
// records Prometheus metrics (request count, duration histogram).
func IngressMiddleware(next http.Handler, serviceName string, opts ...MiddlewareOption) http.Handler {
	cfg := defaultMiddlewareConfig()
	for _, o := range opts {
		o.applyMiddleware(&cfg)
	}

	// Inner: fault injection check (creates fault span as child).
	faulted := cfg.interceptor.IngressMiddleware(next)
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

// EgressTransport composes otelhttp client spans with fault injection.
func EgressTransport(base http.RoundTripper, opts ...MiddlewareOption) http.RoundTripper {
	cfg := defaultMiddlewareConfig()
	for _, o := range opts {
		o.applyMiddleware(&cfg)
	}

	if base == nil {
		base = http.DefaultTransport
	}

	// Inner: fault injection check (creates fault span as child).
	faulted := cfg.interceptor.EgressTransport(base)
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

		if resp != nil {
			if mode := resp.Header.Get("X-Atropos-Cache-Mode"); mode != "" {
				cacheBoxHitsTotal.Inc()
			} else if resp.Header.Get("X-Atropos-Cache-Key") != "" {
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
