package interceptor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/microfaults/atropos-go/internal/evaluator"
	fault "github.com/microfaults/atropos-go/internal/fault"
	"github.com/microfaults/atropos-go/internal/fault/inline"
	"github.com/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
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

func TestCheck_NilEvaluator(t *testing.T) {
	// A nil evaluator should use the noop fallback and not panic.
	i := New(nil, trace.Noop())

	cr, err := i.Check(context.Background(), evaluator.Request{
		Point: evaluator.Ingress,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cr.Handle != nil {
		t.Fatal("expected nil handle with nil evaluator")
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

func TestCheck_LabelsThreadedToSpan(t *testing.T) {
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 10 * time.Millisecond},
			Reason: "test",
			Mode:   evaluator.Inline,
		},
	}

	recorder := &recordingTracer{}
	i := New(eval, recorder)

	labels := map[string]string{
		"tenant":      "acme",
		"queue_depth": "42",
	}

	cr, err := i.Check(context.Background(), evaluator.Request{
		Point:  evaluator.Ingress,
		Labels: labels,
	})
	if err != nil {
		t.Fatal(err)
	}
	<-cr.Handle.Done()

	// Verify labels were set on the span as attributes.
	found := make(map[string]string)
	for _, a := range recorder.span.attrs {
		found[string(a.Key)] = a.Value.AsString()
	}

	for k, v := range labels {
		got, ok := found[k]
		if !ok {
			t.Errorf("label %q not found in span attributes", k)
		} else if got != v {
			t.Errorf("label %q: got %q, want %q", k, got, v)
		}
	}
	t.Logf("labels threaded to span: %v", found)
}

func TestCheck_SpanName(t *testing.T) {
	eval := &mockEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 10 * time.Millisecond},
			Reason: "test",
			Mode:   evaluator.Inline,
		},
	}

	recorder := &recordingTracer{}
	i := New(eval, recorder)

	cr, err := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	if err != nil {
		t.Fatal(err)
	}
	<-cr.Handle.Done()

	if recorder.spanName != trace.SpanFaultInject {
		t.Fatalf("expected span name %q, got %q", trace.SpanFaultInject, recorder.spanName)
	}
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
	recorder := &recordingTracer{}
	i := New(eval, recorder)

	ctx, hookSpan, cr, err := i.Hook(context.Background(), "process-payment", map[string]string{"amount": "100"})
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx
	if cr.Handle == nil {
		t.Fatal("expected handle from hook")
	}

	result := <-cr.Handle.Done()
	hookSpan.End()

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	// Verify the hook span was created (the first Start call is the hook span).
	if !recorder.started {
		t.Fatal("hook span was not created")
	}

	// Verify injected event was recorded on the hook span.
	foundInjected := false
	for _, ev := range recorder.span.events {
		if ev == trace.EventFaultInjected {
			foundInjected = true
		}
	}
	if !foundInjected {
		t.Fatal("expected fault.injected event on hook span")
	}

	t.Logf("hook: %s", result.ActualDuration)
}

func TestHook_AlwaysCreatesSpan(t *testing.T) {
	// Even with nil evaluator (no faults), Hook must create a span.
	recorder := &recordingTracer{}
	i := New(nil, recorder)

	ctx, hookSpan, cr, err := i.Hook(context.Background(), "drain-queue", map[string]string{
		"queue_depth": "7",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx

	// No fault should fire.
	if cr.Handle != nil {
		t.Fatal("expected nil handle when no evaluator")
	}

	hookSpan.End()

	if !recorder.started {
		t.Fatal("hook span was not created")
	}

	// Verify span name includes hook name.
	expectedName := trace.SpanHookPrefix + "drain-queue"
	if recorder.spanName != expectedName {
		t.Fatalf("expected span name %q, got %q", expectedName, recorder.spanName)
	}

	// Verify labels were set on the hook span.
	found := make(map[string]string)
	for _, a := range recorder.span.attrs {
		found[string(a.Key)] = a.Value.AsString()
	}
	if found["queue_depth"] != "7" {
		t.Errorf("expected queue_depth=7, got %q", found["queue_depth"])
	}

	// Verify fault.skipped event.
	foundSkipped := false
	for _, ev := range recorder.span.events {
		if ev == trace.EventFaultSkipped {
			foundSkipped = true
		}
	}
	if !foundSkipped {
		t.Fatal("expected fault.skipped event on hook span")
	}

	if !recorder.span.ended {
		t.Fatal("hook span was not ended")
	}
}

// recordingTracer tracks Start calls and span lifecycle for test assertions.
type recordingTracer struct {
	started  bool
	spanName string
	span     recordingSpan
	// spanCount tracks how many times Start was called (hook + fault = 2).
	spanCount int
}

type recordingSpan struct {
	resultRecorded bool
	errorRecorded  bool
	ended          bool
	attrs          []attribute.KeyValue
	events         []string
}

func (t *recordingTracer) Start(ctx context.Context, name string, _ ...attribute.KeyValue) (context.Context, trace.Span) {
	t.started = true
	t.spanCount++
	// Record the first span name (hook or fault depending on call order).
	if t.spanCount == 1 {
		t.spanName = name
	}
	return ctx, &t.span
}

func (s *recordingSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.attrs = append(s.attrs, attrs...)
}
func (s *recordingSpan) AddEvent(name string, _ ...attribute.KeyValue) {
	s.events = append(s.events, name)
}
func (s *recordingSpan) RecordResult(_ fault.Result) { s.resultRecorded = true }
func (s *recordingSpan) EndWithError(_ error)        { s.errorRecorded = true; s.ended = true }
func (s *recordingSpan) End()                        { s.ended = true }

// eventFault is a test fault implementing EventAware.
// It calls the injected EventEmitter during Start to verify the interceptor
// wires up span events correctly.
type eventFault struct {
	emit      fault.EventEmitter
	emitName  string
	emitAttrs []attribute.KeyValue
}

func (f *eventFault) Validate() error { return nil }
func (f *eventFault) Start(ctx context.Context) (*fault.Handle, error) {
	_, cancel := context.WithCancel(ctx)
	h := fault.NewHandle(cancel)
	go func() {
		defer cancel()
		if f.emit != nil {
			f.emit(f.emitName, f.emitAttrs...)
		}
		h.Send(fault.Result{ActualDuration: time.Millisecond})
	}()
	return h, nil
}
func (f *eventFault) SetEventEmitter(fn fault.EventEmitter) { f.emit = fn }

func TestCheck_EventAwareFault_EmitsSpanEvents(t *testing.T) {
	ef := &eventFault{
		emitName:  "test.event",
		emitAttrs: []attribute.KeyValue{attribute.String("key", "value")},
	}
	eval := &mockEvaluator{decision: &evaluator.Decision{
		Fault:  ef,
		Reason: "test: event aware",
		Mode:   evaluator.Inline,
	}}
	recorder := &recordingTracer{}
	i := New(eval, recorder)

	cr, err := i.Check(context.Background(), evaluator.Request{
		Point:  evaluator.Custom,
		Labels: map[string]string{"test": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-cr.Handle.Done()

	if len(recorder.span.events) == 0 {
		t.Fatal("expected EventAware fault to emit span events")
	}
	found := false
	for _, e := range recorder.span.events {
		if e == "test.event" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected event 'test.event', got events: %v", recorder.span.events)
	}
}
