package interceptor

import (
	"context"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	inlinefault "git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// HeaderFault marks a response the interceptor synthesized for an inline
// error fault (ingress: the handler never ran; egress: the upstream was
// never called), so callers and tests can tell an injected error from a
// real one.
const HeaderFault = "X-Atropos-Fault"

// IngressMiddleware checks for faults on inbound HTTP requests, then -- once
// the response is complete, whichever path produced it -- evaluates the
// Transient point with the same labels plus the response status. A matching
// transient fault is started off the request goroutine and never delays the
// response.
// Cache-box is not dispatched at ingress in Stage 1 -- see
// internal/interceptor/cachebox.go for the egress dispatcher.
func (i *Interceptor) IngressMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labels := extractHTTPLabels(r)
		cr, err := i.Check(r.Context(), evaluator.Request{Point: evaluator.Ingress, Labels: labels})
		if err != nil {
			i.logger.ErrorContext(r.Context(), "atropos: ingress check error", "error", err)
		}

		rec := NewStatusRecorder(w)
		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			res := <-cr.Handle.Done()
			// An inline error fault is the response: the handler never runs.
			if d := inlinefault.IsErrorResult(res); d != nil {
				rec.Header().Set(HeaderFault, "error")
				http.Error(rec, d.StatusText(), d.StatusCode)
				i.transient(r, labels, rec.Status())
				return
			}
		}

		next.ServeHTTP(rec, r)
		i.transient(r, labels, rec.Status())
	})
}

// transient evaluates the Transient point for a completed response: the
// ingress labels plus atropos.http.status_code. The evaluation is
// synchronous (a rule match); the start is not. A fault's Start can block --
// the sidecar's agent fault waits on a unix-socket round trip, the
// in-process I/O stress (internal/fault/resource/io) writes all its files
// first -- and the registry runs it under its lock, so a matching fault is
// started on its own goroutine (StartTransient) and the response is never
// delayed by it. The goroutine gets the request's context snapshotted here
// and detached from the request's cancellation, keeping the trace parent;
// it never touches r, so the request may finish while the fault starts. A
// start failure is only logged, and so is a panic in the start (the fault,
// the tracer or the injection hook): the goroutine is beyond net/http's
// per-connection recover, so without its own an escaped panic would take
// the host process down. Cache-box decisions have no meaning here and are
// ignored.
func (i *Interceptor) transient(r *http.Request, labels map[string]string, status int) {
	tl := maps.Clone(labels)
	tl[trace.AttrHTTPStatusCode] = strconv.Itoa(status)
	req := evaluator.Request{Point: evaluator.Transient, Labels: tl}
	ctx := context.WithoutCancel(r.Context())
	d := i.Evaluate(ctx, req)
	if d == nil || d.CacheBox != evaluator.CacheBoxNone {
		return
	}
	go func() {
		defer func() {
			if p := recover(); p != nil {
				i.logger.ErrorContext(ctx, "atropos: transient start panic", "panic", p)
			}
		}()
		if _, err := i.StartTransient(ctx, req, d); err != nil {
			i.logger.ErrorContext(ctx, "atropos: transient check error", "error", err)
		}
	}()
}

// EgressTransport checks for faults and cache-box operations on outbound
// HTTP requests. When a cache-box decision matches AND the interceptor has
// a CacheBox coordinator configured, the request is dispatched through
// handleCacheBox (see cachebox.go), carrying the decision's experiment as
// baggage (withExperimentBaggage). Otherwise the fault path runs and
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
			return i.handleCacheBox(withExperimentBaggage(r, decision), base, req, decision)
		}

		// Fault path (unchanged from previous behavior, but using the split
		// StartFault method rather than the combined Check).
		if decision != nil && decision.Fault != nil {
			cr, err := i.StartFault(r.Context(), req, decision)
			if err != nil {
				i.logger.ErrorContext(r.Context(), "atropos: egress check error", "error", err)
			}
			if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
				res := <-cr.Handle.Done()
				// An inline error fault is the response: the upstream is
				// never called.
				if d := inlinefault.IsErrorResult(res); d != nil {
					return syntheticErrorResponse(r, d), nil
				}
			}
		}

		return base.RoundTrip(r)
	})
}

// withExperimentBaggage returns r carrying the experiment of the cache-box
// decision applied to it: when the decision's CacheBoxContext names an
// experiment, a clone of r (a RoundTripper never mutates the caller's
// request) whose context has the trace.BaggageExperimentID and
// trace.BaggagePhaseID members set, and whose headers are re-injected from
// that context through the global propagator. The otelhttp transport one
// layer up already injected traceparent and baggage from the pre-decision
// context; re-injecting rewrites baggage with the two members added (every
// other member kept: the SDK never drops foreign baggage) and traceparent
// with the same client span. Otherwise r itself.
//
// The members ride on the call whatever its destination, a third party's
// API included: they are opaque ids, and an egress gateway is the place to
// strip them where that matters.
func withExperimentBaggage(r *http.Request, decision *evaluator.Decision) *http.Request {
	c := decision.CacheBoxContext
	if c == nil || c.ExperimentID == "" {
		return r
	}
	ctx := trace.WithExperimentBaggage(r.Context(), c.ExperimentID, c.PhaseID)
	r = r.Clone(ctx)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(r.Header))
	return r
}

// syntheticErrorResponse builds the response an inline error fault stands in
// for at egress: the configured status, the fault's message as a text/plain
// body, and HeaderFault so the caller can tell it from a real upstream error.
func syntheticErrorResponse(r *http.Request, d *inlinefault.ErrorDetail) *http.Response {
	body := d.StatusText()
	header := http.Header{}
	header.Set("Content-Type", "text/plain; charset=utf-8")
	header.Set(HeaderFault, "error")
	return &http.Response{
		Status:        http.StatusText(d.StatusCode),
		StatusCode:    d.StatusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       r,
	}
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
	// Workflow label from W3C Baggage (trace.BaggageWorkflow), which carries
	// per-request workflow identity across service boundaries: cache-box
	// rules match on it to scope freezing to specific workflows (e.g.
	// "browse" vs "checkout"). Production deployments propagate it via
	// meta-trace-id wiring upstream.
	if b := baggage.FromContext(r.Context()); b.Len() > 0 {
		if m := b.Member(trace.BaggageWorkflow); m.Key() != "" {
			labels[trace.AttrCacheBoxWorkflow] = m.Value()
		}
	}
	return labels
}
