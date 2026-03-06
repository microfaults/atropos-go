package interceptor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"atropos-go/internal/evaluator"
	fault "atropos-go/internal/fault"
	"atropos-go/internal/fault/inline"
	"atropos-go/internal/trace"
)

// mockEvaluator returns a fixed decision for all requests.
type mockEvaluator struct {
	decision *evaluator.Decision
}

func (m *mockEvaluator) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return m.decision
}

func TestCheck_NoFault(t *testing.T) {
	i := New(&mockEvaluator{decision: nil}, trace.Noop())

	cr, err := i.Check(context.Background(), evaluator.Request{
		Point: evaluator.Ingress,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cr.Handle != nil {
		t.Fatal("expected nil handle when no fault selected")
	}
	if cr.Decision != nil {
		t.Fatal("expected nil decision")
	}
}

func TestCheck_WithInlineLatency(t *testing.T) {
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 100 * time.Millisecond},
			Reason: "test: always inject latency",
			Mode:   evaluator.Inline,
		},
	}
	i := New(eval, trace.Noop())

	start := time.Now()
	cr, err := i.Check(context.Background(), evaluator.Request{
		Point:  evaluator.Ingress,
		Labels: map[string]string{"http.path": "/test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cr.Handle == nil {
		t.Fatal("expected handle")
	}
	if cr.Decision == nil {
		t.Fatal("expected decision")
	}

	// For inline faults, caller waits.
	result := <-cr.Handle.Done()
	elapsed := time.Since(start)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected >= 80ms, got %s", elapsed)
	}
	t.Logf("inline latency via interceptor: %s", elapsed)
}

func TestCheck_InvalidFault(t *testing.T) {
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 0, Jitter: 0}, // invalid
			Reason: "test",
			Mode:   evaluator.Inline,
		},
	}
	i := New(eval, trace.Noop())

	_, err := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	if err == nil {
		t.Fatal("expected error for invalid fault")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestCheck_OnResultCallback(t *testing.T) {
	// Verify the interceptor's OnResult callback fires (OTel span end).
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 50 * time.Millisecond},
			Reason: "test",
			Mode:   evaluator.Inline,
		},
	}

	// Use a recording tracer to verify span lifecycle.
	recorder := &recordingTracer{}
	i := New(eval, recorder)

	cr, err := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	if err != nil {
		t.Fatal(err)
	}

	<-cr.Handle.Done()

	if !recorder.started {
		t.Fatal("tracer.Start was not called")
	}
	if !recorder.span.resultRecorded {
		t.Fatal("span.RecordResult was not called")
	}
	t.Logf("OTel span lifecycle verified: started=%v, result_recorded=%v",
		recorder.started, recorder.span.resultRecorded)
}

func TestIngressMiddleware(t *testing.T) {
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 50 * time.Millisecond},
			Reason: "test: middleware latency",
			Mode:   evaluator.Inline,
		},
	}
	i := New(eval, trace.Noop())

	handler := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected >= 40ms from inline latency, got %s", elapsed)
	}
	t.Logf("ingress middleware with 50ms latency: %s", elapsed)
}

func TestHook(t *testing.T) {
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 50 * time.Millisecond},
			Reason: "test: custom hook",
			Mode:   evaluator.Inline,
		},
	}
	i := New(eval, trace.Noop())

	cr, err := i.Hook(context.Background(), "process-payment", map[string]string{"amount": "100"})
	if err != nil {
		t.Fatal(err)
	}
	if cr.Handle == nil {
		t.Fatal("expected handle from hook")
	}

	result := <-cr.Handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	t.Logf("hook: %s", result.ActualDuration)
}

// recordingTracer tracks whether Start was called and span was ended.
type recordingTracer struct {
	started bool
	span    recordingSpan
}

type recordingSpan struct {
	resultRecorded bool
	errorRecorded  bool
	ended          bool
}

func (t *recordingTracer) Start(ctx context.Context, _, _, _ string) (context.Context, trace.Span) {
	t.started = true
	return ctx, &t.span
}

func (s *recordingSpan) RecordResult(_ fault.Result) { s.resultRecorded = true }
func (s *recordingSpan) EndWithError(_ error)        { s.errorRecorded = true; s.ended = true }
func (s *recordingSpan) End()                        { s.ended = true }
