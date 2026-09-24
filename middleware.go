package atropos

import (
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
)

// ingressMiddleware is the shared ingress composition (metrics → otelhttp
// server span → fault check) Serve wraps the business handler in; control
// paths are mounted beside it.
func ingressMiddleware(next http.Handler, serviceName string, i *interceptor.Interceptor) http.Handler {
	return instrument.IngressMiddleware(next, serviceName, i)
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
	return instrument.EgressTransport(base, resolve)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
