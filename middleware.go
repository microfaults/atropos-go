package atropos

import (
	"net/http"

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

// IngressMiddleware composes otelhttp request spans with fault injection.
func IngressMiddleware(next http.Handler, serviceName string, opts ...MiddlewareOption) http.Handler {
	cfg := defaultMiddlewareConfig()
	for _, o := range opts {
		o.applyMiddleware(&cfg)
	}

	// Inner: fault injection check (creates fault span as child).
	faulted := cfg.interceptor.IngressMiddleware(next)
	// Outer: otelhttp request span (becomes parent of fault span).
	return otelhttp.NewHandler(faulted, serviceName)
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
	// Outer: otelhttp client span (becomes parent of fault span).
	return otelhttp.NewTransport(faulted)
}
