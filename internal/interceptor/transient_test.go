package interceptor

import (
	"context"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

func inlineLatencyDecision(name string, d time.Duration) *evaluator.Decision {
	return &evaluator.Decision{Name: name, Reason: "test", Mode: evaluator.Inline,
		Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: d}, Delay: d}}
}

// TestStartTransient_ForcesBackgroundThroughRegistry: at Transient the
// response is already complete, so inline mode is meaningless -- every
// fault, inline types included, runs in Background mode through the
// registry: stoppable by rule name, deduplicated by StartPolicy, reported
// to the injection hook under the "transient" point.
func TestStartTransient_ForcesBackgroundThroughRegistry(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	var points []string
	i := New(nil, trace.Noop(), WithRegistry(reg), WithInjectionHook(func(_, point string) { points = append(points, point) }))
	req := evaluator.Request{Point: evaluator.Transient}

	cr, err := i.StartTransient(context.Background(), req, inlineLatencyDecision("after", time.Second))
	if err != nil || cr.Handle == nil {
		t.Fatalf("StartTransient: handle=%v err=%v", cr.Handle, err)
	}
	if cr.Decision.Mode != evaluator.Background {
		t.Fatalf("mode = %v, want Background", cr.Decision.Mode)
	}
	if len(points) != 1 || points[0] != "transient" {
		t.Fatalf("injection hook points = %v, want [transient]", points)
	}
	// Registry path: a second start under the same rule joins instead of starting.
	again, err := i.StartTransient(context.Background(), req, inlineLatencyDecision("after", time.Second))
	if err != nil || again.Handle != nil {
		t.Fatalf("second start: handle=%v err=%v, want a deduplicated join", again.Handle, err)
	}
	// Stoppable by rule name, as rule removal requires.
	reg.Stop("after")
	select {
	case <-cr.Handle.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("transient fault did not stop through the registry")
	}
}

// TestStartTransient_OutlivesRequestAndKeepsTraceContext: the fault must not
// die with the request, but its span must still be parented on the request's
// context (values survive context.WithoutCancel; cancellation does not).
// Both the registry and the direct path must behave this way.
func TestStartTransient_OutlivesRequestAndKeepsTraceContext(t *testing.T) {
	type key struct{}
	for _, withRegistry := range []bool{true, false} {
		t.Run(map[bool]string{true: "registry", false: "direct"}[withRegistry], func(t *testing.T) {
			rec := &recordingTracer{}
			var opts []Option
			if withRegistry {
				reg := NewFaultRegistry()
				t.Cleanup(reg.Close)
				opts = append(opts, WithRegistry(reg))
			}
			i := New(nil, rec, opts...)
			reqCtx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace-parent"))

			cr, err := i.StartTransient(reqCtx, evaluator.Request{Point: evaluator.Transient}, inlineLatencyDecision("t", 50*time.Millisecond))
			if err != nil || cr.Handle == nil {
				t.Fatalf("StartTransient: handle=%v err=%v", cr.Handle, err)
			}
			cancel() // the request is gone
			if res := <-cr.Handle.Done(); res.Err != nil {
				t.Fatalf("fault died with the request: %v", res.Err)
			}
			if got := rec.startCtx.Value(key{}); got != "trace-parent" {
				t.Fatalf("span context lost the request's values: got %v", got)
			}
			if rec.startCtx.Done() != nil {
				t.Fatal("span context must be detached from request cancellation")
			}
		})
	}
}
