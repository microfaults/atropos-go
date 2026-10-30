package instrument

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

func errorRule(point evaluator.InjectionPoint, status int, msg string) evaluator.StaticRule {
	return evaluator.StaticRule{
		Name: "err", Point: point,
		Decision: evaluator.Decision{Name: "err", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Error{FaultConfig: fault.FaultConfig{Duration: 1}, StatusCode: status, Message: msg}},
	}
}

// TestIngressMiddleware_ErrorFaultShortCircuits pins the documented inline
// error semantics: the configured status and message ARE the response and
// the business handler never runs.
func TestIngressMiddleware_ErrorFaultShortCircuits(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(errorRule(evaluator.Ingress, 503, "injected fault")), nil)
	var reached atomic.Bool
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }), "svc", ic)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "injected fault") {
		t.Fatalf("code=%d body=%q, want 503 + message", rec.Code, rec.Body.String())
	}
	if rec.Header().Get(interceptor.HeaderFault) != "error" {
		t.Fatal("synthetic error response must be marked X-Atropos-Fault: error")
	}
	if reached.Load() {
		t.Fatal("handler must not run when an inline error fault fires")
	}
}

// TestEgressTransport_ErrorFaultShortCircuits: at egress the synthetic
// response stands in for the upstream, which is never called.
func TestEgressTransport_ErrorFaultShortCircuits(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(errorRule(evaluator.Egress, 502, "boom")), nil)
	var live atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		live.Add(1)
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	tr := EgressTransport(base, func() *interceptor.Interceptor { return ic })

	resp, err := tr.RoundTrip(httptest.NewRequest("GET", "http://dep/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 502 || !strings.Contains(string(body), "boom") || live.Load() != 0 {
		t.Fatalf("status=%d body=%q live=%d", resp.StatusCode, body, live.Load())
	}
	if resp.Header.Get(interceptor.HeaderFault) != "error" {
		t.Fatal("synthetic error response must be marked X-Atropos-Fault: error")
	}
}

// TestEgressTransport_LatencyFaultStillForwards guards the other inline
// faults: a latency fault delays, then the upstream is called normally.
func TestEgressTransport_LatencyFaultStillForwards(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "lat", Point: evaluator.Egress,
		Decision: evaluator.Decision{Name: "lat", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: 20e6}, Delay: 20e6}},
	}), nil)
	var live atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		live.Add(1)
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	resp, err := EgressTransport(base, func() *interceptor.Interceptor { return ic }).RoundTrip(httptest.NewRequest("GET", "http://dep/x", nil))
	if err != nil || resp.StatusCode != 200 || live.Load() != 1 {
		t.Fatalf("err=%v status=%v live=%d", err, resp, live.Load())
	}
}

// TestIngressMiddleware_TransientThroughFullComposition: metrics + otelhttp +
// fault middleware; the transient rule sees the status the handler wrote and
// the injection hook is called with the transient point.
func TestIngressMiddleware_TransientThroughFullComposition(t *testing.T) {
	reg := interceptor.NewFaultRegistry()
	t.Cleanup(reg.Close)
	points := make(chan string, 4)
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "after-404", Point: evaluator.Transient, Labels: map[string]string{trace.AttrHTTPStatusCode: "404"},
		Decision: evaluator.Decision{Name: "after-404", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}},
	}), nil, interceptor.WithRegistry(reg), interceptor.WithInjectionHook(func(_, p string) { points <- p }))
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }), "svc", ic)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/missing", nil))
	if rec.Code != 404 {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	// The transient start runs on its own goroutine: wait for the hook.
	select {
	case p := <-points:
		if p != "transient" {
			t.Fatalf("injection point = %q, want transient", p)
		}
	case <-time.After(time.Second):
		t.Fatal("transient fault did not start")
	}
	select {
	case p := <-points:
		t.Fatalf("unexpected second injection point %q, want exactly [transient]", p)
	case <-time.After(50 * time.Millisecond):
	}
}
