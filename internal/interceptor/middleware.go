package interceptor

import (
	"log"
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/baggage"
)

// workflowBaggageKey is the W3C baggage entry name used to carry per-request
// workflow identity across service boundaries. Cache-box rules can match on
// this to scope freezing to specific workflows (e.g. "browse" vs "checkout").
const workflowBaggageKey = "atropos.workflow"

// IngressMiddleware checks for faults on inbound HTTP requests.
// Cache-box is not dispatched at ingress in Stage 1 -- see
// internal/interceptor/cachebox.go for the egress dispatcher.
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

// EgressTransport checks for faults and cache-box operations on outbound
// HTTP requests. When a cache-box decision matches AND the interceptor has
// a CacheBox coordinator configured, the request is dispatched through
// handleCacheBox (see cachebox.go). Otherwise the fault path runs and
// base.RoundTrip is called to forward the request normally.
func (i *Interceptor) EgressTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		req := evaluator.Request{
			Point:  evaluator.Egress,
			Labels: extractHTTPLabels(r),
		}

		decision := i.Evaluate(r.Context(), req)

		// Cache-box dispatch. Requires both a matching decision and an
		// attached cache-box coordinator. If either is missing the request
		// falls through to the fault path.
		if decision != nil && decision.CacheBox != evaluator.CacheBoxNone && i.cacheBox != nil {
			return i.handleCacheBox(r, base, req, decision)
		}

		// Fault path (unchanged from previous behavior, but using the split
		// StartFault method rather than the combined Check).
		if decision != nil && decision.Fault != nil {
			cr, err := i.StartFault(r.Context(), req, decision)
			if err != nil {
				log.Printf("atropos: egress check error: %v", err)
			}
			if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
				<-cr.Handle.Done()
			}
		}

		return base.RoundTrip(r)
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// extractHTTPLabels populates the evaluator Request.Labels map with HTTP
// metadata and any workflow identity from W3C baggage. These labels are
// also threaded into spans as attributes downstream.
func extractHTTPLabels(r *http.Request) map[string]string {
	labels := map[string]string{
		trace.AttrHTTPMethod: r.Method,
		trace.AttrHTTPPath:   r.URL.Path,
		trace.AttrHTTPHost:   r.Host,
	}
	if r.URL.RawQuery != "" {
		labels[trace.AttrHTTPQuery] = r.URL.RawQuery
	}
	if ua := r.UserAgent(); ua != "" {
		labels[trace.AttrHTTPUserAgent] = ua
	}
	// Workflow label from W3C Baggage. The baggage key is "atropos.workflow";
	// production deployments propagate it via meta-trace-id wiring upstream.
	if b := baggage.FromContext(r.Context()); b.Len() > 0 {
		if m := b.Member(workflowBaggageKey); m.Key() != "" {
			labels[trace.AttrCacheBoxWorkflow] = m.Value()
		}
	}
	return labels
}
