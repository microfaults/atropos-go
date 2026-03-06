package interceptor

import (
	"log"
	"net/http"

	"atropos-go/internal/evaluator"
)

// IngressMiddleware returns HTTP middleware that checks for faults on
// inbound requests. If the evaluator returns an Inline fault, the
// middleware blocks until the fault completes before calling next.
// Background faults run independently.
func (i *Interceptor) IngressMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := evaluator.Request{
			Point:  evaluator.Ingress,
			Labels: extractHTTPLabels(r),
		}

		cr, err := i.Check(r.Context(), req)
		if err != nil {
			log.Printf("atropos: ingress check error: %v", err)
		}

		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			<-cr.Handle.Done()
		}

		next.ServeHTTP(w, r)
	})
}

// EgressTransport wraps an http.RoundTripper to check for faults on
// outbound requests. If the evaluator returns an Inline fault, the
// transport blocks before making the real request.
func (i *Interceptor) EgressTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		req := evaluator.Request{
			Point:  evaluator.Egress,
			Labels: extractHTTPLabels(r),
		}

		cr, err := i.Check(r.Context(), req)
		if err != nil {
			log.Printf("atropos: egress check error: %v", err)
		}

		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			<-cr.Handle.Done()
		}

		return base.RoundTrip(r)
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func extractHTTPLabels(r *http.Request) map[string]string {
	labels := map[string]string{
		"http.method": r.Method,
		"http.path":   r.URL.Path,
		"http.host":   r.Host,
	}
	if ua := r.UserAgent(); ua != "" {
		labels["http.user_agent"] = ua
	}
	return labels
}
