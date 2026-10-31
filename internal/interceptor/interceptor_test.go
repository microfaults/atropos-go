package interceptor

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	fault "git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/log/logtest"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
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
	// startCtx is the context the last Start received (span parent).
	startCtx context.Context
	// links collects the links StartLinked was given.
	links []oteltrace.Link
}

type recordingSpan struct {
	resultRecorded bool
	errorRecorded  bool
	ended          bool
	attrs          []attribute.KeyValue
	events         []string
	links          []oteltrace.Link
}

func (t *recordingTracer) Start(ctx context.Context, name string, _ ...attribute.KeyValue) (context.Context, trace.Span) {
	t.started = true
	t.spanCount++
	t.startCtx = ctx
	// Record the first span name (hook or fault depending on call order).
	if t.spanCount == 1 {
		t.spanName = name
	}
	return ctx, &t.span
}

func (t *recordingTracer) StartLinked(ctx context.Context, name string, links []oteltrace.Link, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	t.links = append(t.links, links...)
	return t.Start(ctx, name, attrs...)
}

func (s *recordingSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.attrs = append(s.attrs, attrs...)
}
func (s *recordingSpan) AddEvent(name string, _ ...attribute.KeyValue) {
	s.events = append(s.events, name)
}
func (s *recordingSpan) AddLink(l oteltrace.Link)           { s.links = append(s.links, l) }
func (s *recordingSpan) SpanContext() oteltrace.SpanContext { return oteltrace.SpanContext{} }
func (s *recordingSpan) RecordResult(_ fault.Result)        { s.resultRecorded = true }
func (s *recordingSpan) EndWithError(_ error)               { s.errorRecorded = true; s.ended = true }
func (s *recordingSpan) End()                               { s.ended = true }

// hasAttr reports whether attrs holds key with value, compared in the
// value's string form (Value.Emit).
func hasAttr(attrs []attribute.KeyValue, key, value string) bool {
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.Emit() == value {
			return true
		}
	}
	return false
}

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

// ctxCapturingHandler records the context and message of every record.
type ctxCapturingHandler struct {
	mu   sync.Mutex
	ctxs []context.Context
	msgs []string
}

func (h *ctxCapturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *ctxCapturingHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ctxs = append(h.ctxs, ctx)
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *ctxCapturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *ctxCapturingHandler) WithGroup(string) slog.Handler      { return h }

func sampledContext(t *testing.T) (context.Context, oteltrace.SpanContext) {
	t.Helper()
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: oteltrace.FlagsSampled,
	})
	return oteltrace.ContextWithSpanContext(context.Background(), sc), sc
}

// invalidFaultRule decides, at point only, on a fault that fails Validate (a
// latency with neither delay nor jitter), so a check at that point returns an
// error. Other points find no rule: the Transient evaluation that follows an
// ingress response must not add a second, asynchronous record.
func invalidFaultRule(point evaluator.InjectionPoint) evaluator.StaticRule {
	return evaluator.StaticRule{Name: "invalid", Point: point, Decision: evaluator.Decision{
		Fault: &inline.Latency{Delay: 0}, // invalid: Validate fails, Check returns an error
		Mode:  evaluator.Inline, Reason: "test",
	}}
}

func TestIngressMiddleware_CheckErrorLogsWithRequestContext(t *testing.T) {
	h := &ctxCapturingHandler{}
	eval := evaluator.NewStaticEvaluator(invalidFaultRule(evaluator.Ingress))
	i := New(eval, trace.Noop(), WithLogger(slog.New(h)))
	ctx, sc := sampledContext(t)
	req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
	i.IngressMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(httptest.NewRecorder(), req)

	if len(h.msgs) != 1 || h.msgs[0] != "atropos: ingress check error" {
		t.Fatalf("messages = %q", h.msgs)
	}
	if got := oteltrace.SpanContextFromContext(h.ctxs[0]); got.TraceID() != sc.TraceID() {
		t.Fatalf("log context trace id = %s, want %s", got.TraceID(), sc.TraceID())
	}
}

func TestNew_LoggerDefaultsToSlogDefault(t *testing.T) {
	if New(nil, nil).Logger() != slog.Default() {
		t.Fatal("Logger() must fall back to slog.Default()")
	}
}

// WithLogger's logger is the one Logger() returns; a nil one leaves the default.
func TestWithLogger_IsReturnedByLogger(t *testing.T) {
	l := slog.New(&ctxCapturingHandler{})
	if got := New(nil, nil, WithLogger(l)).Logger(); got != l {
		t.Fatalf("Logger() = %p, want the logger passed to WithLogger (%p)", got, l)
	}
	if got := New(nil, nil, WithLogger(nil)).Logger(); got != slog.Default() {
		t.Fatal("WithLogger(nil) must leave the slog.Default() fallback")
	}
}

// installRecordingProvider makes the global tracer provider export
// synchronously to an in-memory exporter for the test's lifetime.
func installRecordingProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample())))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return exp
}

func findSpan(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no span %q in %d spans", name, len(spans))
	return tracetest.SpanStub{}
}

func linksOfKind(links []sdktrace.Link, kind string) []sdktrace.Link {
	var out []sdktrace.Link
	for _, l := range links {
		for _, a := range l.Attributes {
			if a.Key == trace.AttrLinkKind && a.Value.AsString() == kind {
				out = append(out, l)
			}
		}
	}
	return out
}

func backgroundDecision(name string) *evaluator.Decision {
	return &evaluator.Decision{Name: name, Fault: newStubFault(60 * time.Millisecond), Reason: "test", Mode: evaluator.Background}
}

// heldDecision is a background decision whose fault runs until the registry
// stops it, so a test that joins it never races the fault's end.
func heldDecision(name string) *evaluator.Decision {
	return &evaluator.Decision{Name: name, Fault: newStubFault(time.Hour), Reason: "test", Mode: evaluator.Background}
}

func TestStartFault_BackgroundSpanLinksTriggerAndRecent(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	tr := otel.Tracer("test")

	var recent []oteltrace.SpanContext
	for n := 0; n < 3; n++ { // three earlier requests, evaluated with no fault
		ctx, s := tr.Start(context.Background(), "earlier")
		i.Evaluate(ctx, evaluator.Request{Point: evaluator.Ingress, Labels: map[string]string{"n": "x"}})
		recent = append(recent, s.SpanContext())
		s.End()
	}
	ctx, reqSpan := tr.Start(context.Background(), "trigger")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil || cr.RunID == "" || cr.Joined {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	reqSpan.End()

	spans := exp.GetSpans()
	fs := findSpan(t, spans, trace.SpanFaultInject)
	if !hasAttr(fs.Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatalf("fault span attrs = %v, want %s=%s", fs.Attributes, trace.AttrFaultID, cr.RunID)
	}
	if fs.Parent.IsValid() {
		t.Fatal("background fault span must stay a root (detached from the request)")
	}
	trig := linksOfKind(fs.Links, trace.LinkKindTrigger)
	if len(trig) != 1 || trig[0].SpanContext.SpanID() != reqSpan.SpanContext().SpanID() {
		t.Fatalf("trigger links = %+v", trig)
	}
	rl := linksOfKind(fs.Links, trace.LinkKindRecent)
	if len(rl) != 3 {
		t.Fatalf("recent links = %d, want 3 (trigger excluded)", len(rl))
	}
	if rl[0].SpanContext.SpanID() != recent[2].SpanID() {
		t.Fatal("recent links must be newest first")
	}
	rs := findSpan(t, spans, "trigger")
	if !hasAttr(rs.Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatalf("request span must carry atropos.fault.id, got %v", rs.Attributes)
	}
}

// The fault runs until reg.Stop, so the second request meets it running
// however long the first one took.
func TestStartFault_JoinLinksRequestToFaultSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: heldDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	tr := otel.Tracer("test")

	ctx1, s1 := tr.Start(context.Background(), "first")
	first, err := i.Check(ctx1, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || first.Handle == nil {
		t.Fatalf("first: %+v %v", first, err)
	}
	s1.End()
	ctx2, s2 := tr.Start(context.Background(), "joiner")
	second, err := i.Check(ctx2, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || !second.Joined || second.Handle != nil || second.RunID != first.RunID {
		t.Fatalf("second: %+v %v (first run %s)", second, err, first.RunID)
	}
	s2.End()
	reg.Stop("bg")
	<-first.Handle.Done()

	spans := exp.GetSpans()
	fs := findSpan(t, spans, trace.SpanFaultInject)
	js := findSpan(t, spans, "joiner")
	fl := linksOfKind(js.Links, trace.LinkKindFault)
	if len(fl) != 1 || fl[0].SpanContext.SpanID() != fs.SpanContext.SpanID() {
		t.Fatalf("joiner links = %+v, want one to the fault span", js.Links)
	}
	if !hasAttr(js.Attributes, trace.AttrFaultID, first.RunID) || !hasAttr(fs.Attributes, trace.AttrFaultJoined, "1") {
		t.Fatalf("joiner attrs %v / fault attrs %v", js.Attributes, fs.Attributes)
	}
}

// Review Focus 5: Core.Arm starts from context.Background() (marked by
// ArmedContext); without a request span there is no trigger to link.
func TestStartFault_ArmWithoutRequestSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	cr, err := i.StartFault(context.Background(), evaluator.Request{Point: evaluator.Egress, Labels: map[string]string{}}, backgroundDecision("bg"))
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	fs := findSpan(t, exp.GetSpans(), trace.SpanFaultInject)
	if len(linksOfKind(fs.Links, trace.LinkKindTrigger)) != 0 {
		t.Fatalf("no trigger link expected without a request span, got %+v", fs.Links)
	}
}

func TestStartFault_InlineStampsRunIDOnRequestSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	i := New(&mockEvaluator{decision: &evaluator.Decision{Fault: &inline.Latency{Delay: 5 * time.Millisecond}, Mode: evaluator.Inline, Reason: "test"}}, trace.NewOTelTracer())
	ctx, s := otel.Tracer("test").Start(context.Background(), "req")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.RunID == "" {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	s.End()
	spans := exp.GetSpans()
	if !hasAttr(findSpan(t, spans, "req").Attributes, trace.AttrFaultID, cr.RunID) || !hasAttr(findSpan(t, spans, trace.SpanFaultInject).Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatal("both the request span and the inline fault span must carry atropos.fault.id")
	}
}

// Ruling (plan A): a Transient run goes through the registry like any
// background fault and links the request that triggered it, while its span
// stays a child of that request's span. The request itself is excluded from
// the recent links (it is the trigger); the request before it is not.
func TestStartTransient_SpanLinksTriggerRequest(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(nil, trace.NewOTelTracer(), WithRegistry(reg))
	tr := otel.Tracer("test")

	ctx0, s0 := tr.Start(context.Background(), "earlier")
	i.Evaluate(ctx0, evaluator.Request{Point: evaluator.Ingress})
	s0.End()
	// What IngressMiddleware does: evaluate Ingress, serve, then start the
	// Transient fault with the request's context.
	ctx, reqSpan := tr.Start(context.Background(), "request")
	i.Evaluate(ctx, evaluator.Request{Point: evaluator.Ingress})
	cr, err := i.StartTransient(ctx, evaluator.Request{Point: evaluator.Transient}, backgroundDecision("after"))
	if err != nil || cr.Handle == nil || cr.RunID == "" || cr.Joined {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	reqSpan.End()

	spans := exp.GetSpans()
	fs := findSpan(t, spans, trace.SpanFaultInject)
	if fs.Parent.SpanID() != reqSpan.SpanContext().SpanID() {
		t.Fatalf("transient fault span parent = %s, want the request span %s", fs.Parent.SpanID(), reqSpan.SpanContext().SpanID())
	}
	trig := linksOfKind(fs.Links, trace.LinkKindTrigger)
	if len(trig) != 1 || trig[0].SpanContext.SpanID() != reqSpan.SpanContext().SpanID() {
		t.Fatalf("trigger links = %+v, want one to the request span", trig)
	}
	rl := linksOfKind(fs.Links, trace.LinkKindRecent)
	if len(rl) != 1 || rl[0].SpanContext.SpanID() != s0.SpanContext().SpanID() {
		t.Fatalf("recent links = %+v, want only the earlier request", rl)
	}
	if !hasAttr(fs.Attributes, trace.AttrFaultID, cr.RunID) || !hasAttr(findSpan(t, spans, "request").Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatal("the transient fault span and its request span must carry atropos.fault.id")
	}
}

// Ruling (Review Focus 5): an armed run (Core.Arm starts from
// ArmedContext(context.Background()): no request is behind it) links the
// recent requests and nothing else -- newest first, at most ringLinks of
// them.
func TestStartFault_ArmLinksOnlyRecentRequests(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(nil, trace.NewOTelTracer(), WithRegistry(reg))
	tr := otel.Tracer("test")
	var seen []oteltrace.SpanContext
	for range ringLinks + 4 {
		ctx, s := tr.Start(context.Background(), "earlier")
		i.Evaluate(ctx, evaluator.Request{Point: evaluator.Ingress})
		seen = append(seen, s.SpanContext())
		s.End()
	}
	cr, err := i.StartFault(ArmedContext(context.Background()), evaluator.Request{Point: evaluator.Egress, Labels: map[string]string{}}, backgroundDecision("armed"))
	if err != nil || cr.Handle == nil || cr.RunID == "" {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()

	fs := findSpan(t, exp.GetSpans(), trace.SpanFaultInject)
	if rl := linksOfKind(fs.Links, trace.LinkKindRecent); len(fs.Links) != ringLinks || len(rl) != ringLinks {
		t.Fatalf("links = %d (%d recent), want exactly %d, all recent", len(fs.Links), len(rl), ringLinks)
	}
	for k, l := range fs.Links {
		if want := seen[len(seen)-1-k]; l.SpanContext.SpanID() != want.SpanID() {
			t.Fatalf("link %d = %s, want %s (newest first)", k, l.SpanContext.SpanID(), want.SpanID())
		}
	}
}

// Review Focus 2: an unsampled request's trace is never stored, so nothing
// links to it -- not as the trigger of a background run, and not as a fault
// span a sampled joiner would link to. Under the SDK's default sampler
// (ParentBased(AlwaysSample)) a background run is a root, so it is sampled
// and exported; a Transient run is a child of its request and inherits the
// request's unsampled flag.
func TestStartFault_UnsampledSpansAreNeverLinkTargets(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))) // default sampler
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	unsampled := oteltrace.ContextWithRemoteSpanContext(context.Background(), sampledSC(7).WithTraceFlags(0))

	// An unsampled trigger: the run starts, as a sampled root, without links.
	cr, err := i.Check(unsampled, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	fs := findSpan(t, exp.GetSpans(), trace.SpanFaultInject)
	if len(fs.Links) != 0 {
		t.Fatalf("fault span links = %+v, want none (the trigger was unsampled and the ring holds nothing)", fs.Links)
	}

	// An unsampled Transient run, then a sampled request joins it: the joiner
	// counts as joined but must not link to a fault span that was never stored.
	run, err := i.StartTransient(unsampled, evaluator.Request{Point: evaluator.Transient}, heldDecision("after"))
	if err != nil || run.Handle == nil {
		t.Fatalf("transient: %+v %v", run, err)
	}
	ctx, joiner := otel.Tracer("test").Start(context.Background(), "joiner")
	joined, err := i.StartTransient(ctx, evaluator.Request{Point: evaluator.Transient}, heldDecision("after"))
	if err != nil || !joined.Joined || joined.RunID != run.RunID {
		t.Fatalf("joiner: %+v %v (run %s)", joined, err, run.RunID)
	}
	joiner.End()
	reg.Stop("after")
	<-run.Handle.Done()
	js := findSpan(t, exp.GetSpans(), "joiner")
	if len(js.Links) != 0 || !hasAttr(js.Attributes, trace.AttrFaultID, run.RunID) {
		t.Fatalf("joiner links = %+v attrs = %v, want no link to the unsampled fault span and atropos.fault.id=%s", js.Links, js.Attributes, run.RunID)
	}
}

// A Hook that joins a running fault reports it as injected, not skipped: the
// annotated block runs under that fault.
func TestHook_JoinedFaultCountsAsInjected(t *testing.T) {
	reg := NewFaultRegistry()
	defer reg.Close()
	recorder := &recordingTracer{}
	i := New(&mockEvaluator{decision: heldDecision("bg")}, recorder, WithRegistry(reg))

	_, _, first, err := i.Hook(context.Background(), "block", nil)
	if err != nil || first.Handle == nil {
		t.Fatalf("first: %+v %v", first, err)
	}
	_, _, second, err := i.Hook(context.Background(), "block", nil)
	if err != nil || !second.Joined {
		t.Fatalf("second: %+v %v, want a join", second, err)
	}
	var injected, skipped int
	for _, ev := range recorder.span.events {
		switch ev {
		case trace.EventFaultInjected:
			injected++
		case trace.EventFaultSkipped:
			skipped++
		}
	}
	if injected != 2 || skipped != 0 {
		t.Fatalf("hook events: injected=%d skipped=%d, want 2 and 0", injected, skipped)
	}
	reg.Stop("bg")
	<-first.Handle.Done()
}

// failStartFault fails in Start, after Validate passed.
type failStartFault struct{}

func (failStartFault) Validate() error { return nil }
func (failStartFault) Start(context.Context) (*fault.Handle, error) {
	return nil, errors.New("no start")
}

// A fault that fails to start leaves no run: the request's span gets no
// atropos.fault.id on either path (the failed fault span keeps its own id and
// its error), and the result carries none.
func TestStartFault_FailedStartLeavesNoRunIDOnTheRequest(t *testing.T) {
	for _, withRegistry := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "registry"}[withRegistry], func(t *testing.T) {
			exp := installRecordingProvider(t)
			var opts []Option
			if withRegistry {
				reg := NewFaultRegistry()
				t.Cleanup(reg.Close)
				opts = append(opts, WithRegistry(reg))
			}
			i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "fails", Fault: failStartFault{}, Mode: evaluator.Background, Reason: "test"}}, trace.NewOTelTracer(), opts...)
			ctx, s := otel.Tracer("test").Start(context.Background(), "req")
			cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
			s.End()
			if err == nil || cr.RunID != "" || cr.Handle != nil {
				t.Fatalf("cr=%+v err=%v, want an error and no run", cr, err)
			}
			spans := exp.GetSpans()
			for _, a := range findSpan(t, spans, "req").Attributes {
				if a.Key == trace.AttrFaultID {
					t.Fatalf("request span carries %s=%s for a fault that never started", a.Key, a.Value.Emit())
				}
			}
			if fs := findSpan(t, spans, trace.SpanFaultInject); fs.Status.Code != codes.Error {
				t.Fatalf("failed fault span status = %v, want Error", fs.Status.Code)
			}
		})
	}
}

// faultSpanOf finds the exported fault span of the run with the given id.
func faultSpanOf(t *testing.T, spans tracetest.SpanStubs, runID string) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == trace.SpanFaultInject && hasAttr(s.Attributes, trace.AttrFaultID, runID) {
			return s
		}
	}
	t.Fatalf("no fault span with %s=%s in %d spans", trace.AttrFaultID, runID, len(spans))
	return tracetest.SpanStub{}
}

// Eager arming (Core.Arm, through ArmedContext) re-arms every background
// rule on each delivery. A re-arm that meets the running fault is reported
// to the caller as a join (no handle, the running run's id), but no request
// made it, so it is not counted on the fault span. A request that joins an
// armed fault is counted, and its span gets the run id and the link; so is
// an unsampled request, whose span is merely not stored, and a request with
// no span context at all (a gRPC call without otelgrpc), which has no span
// to stamp or link.
func TestStartFault_RearmsAreNotCountedAsJoins(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(nil, trace.NewOTelTracer(), WithRegistry(reg))
	arm := func(name string) CheckResult {
		t.Helper()
		cr, err := i.StartFault(ArmedContext(context.Background()), evaluator.Request{Point: evaluator.Ingress, Labels: map[string]string{}}, heldDecision(name))
		if err != nil {
			t.Fatal(err)
		}
		return cr
	}

	rearmed := arm("rearmed")
	if rearmed.Handle == nil || rearmed.Joined {
		t.Fatalf("first arm = %+v, want a start", rearmed)
	}
	for range 2 {
		if again := arm("rearmed"); !again.Joined || again.Handle != nil || again.RunID != rearmed.RunID {
			t.Fatalf("re-arm = %+v, want a join on run %s with no handle", again, rearmed.RunID)
		}
	}

	joined := arm("joined")
	ctx, req := otel.Tracer("test").Start(context.Background(), "request")
	cr, err := i.StartFault(ctx, evaluator.Request{Point: evaluator.Ingress}, heldDecision("joined"))
	if err != nil || !cr.Joined || cr.RunID != joined.RunID {
		t.Fatalf("request join = %+v %v, want a join on run %s", cr, err, joined.RunID)
	}
	req.End()

	unsampledJoin := arm("unsampled-join")
	unsampled := oteltrace.ContextWithRemoteSpanContext(context.Background(), sampledSC(9).WithTraceFlags(0))
	if cr, err := i.StartFault(unsampled, evaluator.Request{Point: evaluator.Ingress}, heldDecision("unsampled-join")); err != nil || !cr.Joined {
		t.Fatalf("unsampled request join = %+v %v", cr, err)
	}

	bareJoin := arm("bare-join")
	if cr, err := i.StartFault(context.Background(), evaluator.Request{Point: evaluator.Ingress}, heldDecision("bare-join")); err != nil || !cr.Joined || cr.RunID != bareJoin.RunID {
		t.Fatalf("request join without a span context = %+v %v, want a join on run %s", cr, err, bareJoin.RunID)
	}

	for _, run := range []CheckResult{rearmed, joined, unsampledJoin, bareJoin} {
		reg.Stop(run.Decision.Name)
		<-run.Handle.Done()
	}

	spans := exp.GetSpans()
	for _, a := range faultSpanOf(t, spans, rearmed.RunID).Attributes {
		if a.Key == trace.AttrFaultJoined {
			t.Fatalf("re-armed fault span carries %s=%s, want none: no request joined it", a.Key, a.Value.Emit())
		}
	}
	fs := faultSpanOf(t, spans, joined.RunID)
	if !hasAttr(fs.Attributes, trace.AttrFaultJoined, "1") {
		t.Fatalf("fault span attrs = %v, want %s=1 (the one request)", fs.Attributes, trace.AttrFaultJoined)
	}
	rs := findSpan(t, spans, "request")
	if fl := linksOfKind(rs.Links, trace.LinkKindFault); len(fl) != 1 || fl[0].SpanContext.SpanID() != fs.SpanContext.SpanID() || !hasAttr(rs.Attributes, trace.AttrFaultID, joined.RunID) {
		t.Fatalf("request span links %+v attrs %v, want the fault link and %s=%s", rs.Links, rs.Attributes, trace.AttrFaultID, joined.RunID)
	}
	if us := faultSpanOf(t, spans, unsampledJoin.RunID); !hasAttr(us.Attributes, trace.AttrFaultJoined, "1") {
		t.Fatalf("fault span attrs = %v, want %s=1: an unsampled request is still a request", us.Attributes, trace.AttrFaultJoined)
	}
	if bs := faultSpanOf(t, spans, bareJoin.RunID); !hasAttr(bs.Attributes, trace.AttrFaultJoined, "1") {
		t.Fatalf("fault span attrs = %v, want %s=1: a request without a span context is still a request", bs.Attributes, trace.AttrFaultJoined)
	}
}

// The armed marker, not the span context, says that no request is behind a
// call. An arm whose context carries a sampled span (an arm made while
// serving an admin request, say) is no request either: its start neither
// links that span as the trigger nor stamps it with the run id, and its
// re-arm is not counted, stamps nothing and links nothing.
func TestStartFault_ArmedCallIsNoRequestWhateverItsSpanContext(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(nil, trace.NewOTelTracer(), WithRegistry(reg))
	ctx, admin := otel.Tracer("test").Start(context.Background(), "admin")
	armed := ArmedContext(ctx)
	req := evaluator.Request{Point: evaluator.Egress, Labels: map[string]string{}}

	start, err := i.StartFault(armed, req, heldDecision("armed"))
	if err != nil || start.Handle == nil || start.Joined {
		t.Fatalf("arm = %+v %v, want a start", start, err)
	}
	again, err := i.StartFault(armed, req, heldDecision("armed"))
	if err != nil || !again.Joined || again.Handle != nil || again.RunID != start.RunID {
		t.Fatalf("re-arm = %+v %v, want a join on run %s with no handle", again, err, start.RunID)
	}
	admin.End()
	reg.Stop("armed")
	<-start.Handle.Done()

	spans := exp.GetSpans()
	fs := faultSpanOf(t, spans, start.RunID)
	if len(fs.Links) != 0 {
		t.Fatalf("armed fault span links = %+v, want none: the arm's span is no trigger and the ring is empty", fs.Links)
	}
	for _, a := range fs.Attributes {
		if a.Key == trace.AttrFaultJoined {
			t.Fatalf("armed fault span carries %s=%s, want none: only a re-arm met it", a.Key, a.Value.Emit())
		}
	}
	as := findSpan(t, spans, "admin")
	if len(as.Links) != 0 {
		t.Fatalf("the arm's span links = %+v, want none", as.Links)
	}
	for _, a := range as.Attributes {
		if a.Key == trace.AttrFaultID {
			t.Fatalf("the arm's span carries %s=%s, want none: it is no request", a.Key, a.Value.Emit())
		}
	}
}

// The direct path honours the mark too: an armed inline start (no registry)
// stamps no run id on the span its context carries.
func TestStartFault_ArmedDirectStartStampsNoSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	i := New(nil, trace.NewOTelTracer())
	ctx, admin := otel.Tracer("test").Start(context.Background(), "admin")
	cr, err := i.StartFault(ArmedContext(ctx), evaluator.Request{Point: evaluator.Ingress, Labels: map[string]string{}},
		&evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "test"})
	if err != nil || cr.Handle == nil || cr.RunID == "" {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	admin.End()
	for _, a := range findSpan(t, exp.GetSpans(), "admin").Attributes {
		if a.Key == trace.AttrFaultID {
			t.Fatalf("the arm's span carries %s=%s, want none: it is no request", a.Key, a.Value.Emit())
		}
	}
}

// ArmedContext keeps the context it marks: its values, deadline and
// cancellation.
func TestArmedContext_MarksWithoutDroppingTheContext(t *testing.T) {
	type key struct{}
	base, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "v"))
	armed := ArmedContext(base)
	if !isArmed(armed) || isArmed(base) || isArmed(context.Background()) {
		t.Fatalf("isArmed: armed=%v base=%v background=%v, want only the marked context armed", isArmed(armed), isArmed(base), isArmed(context.Background()))
	}
	if armed.Value(key{}) != "v" {
		t.Fatal("ArmedContext dropped the context's values")
	}
	cancel()
	if armed.Err() == nil {
		t.Fatal("ArmedContext dropped the context's cancellation")
	}
}

// The first request an interceptor sees is the ring's only entry; when it
// triggers a background fault, the fault span links it once, as the trigger,
// and has no recent link to it.
func TestStartFault_FirstRequestLinksOnlyItsTrigger(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	ctx, reqSpan := otel.Tracer("test").Start(context.Background(), "first")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	if ids := i.recent.TraceIDs(); len(ids) != 1 {
		t.Fatalf("ring holds %v, want only the trigger", ids)
	}
	<-cr.Handle.Done()
	reqSpan.End()

	fs := findSpan(t, exp.GetSpans(), trace.SpanFaultInject)
	if len(fs.Links) != 1 || len(linksOfKind(fs.Links, trace.LinkKindTrigger)) != 1 || fs.Links[0].SpanContext.SpanID() != reqSpan.SpanContext().SpanID() {
		t.Fatalf("fault span links = %+v, want exactly the trigger link to the request span", fs.Links)
	}
}

func installDecisionRecorder(t *testing.T) *logtest.Recorder {
	t.Helper()
	rec := logtest.NewRecorder()
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(rec)
	t.Cleanup(func() { global.SetLoggerProvider(prev) })
	return rec
}

func decisionRecords(rec *logtest.Recorder) []logtest.Record {
	return rec.Result()[logtest.Scope{Name: trace.DecisionLoggerName}]
}

func attrString(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

func TestStartFault_EmitsDecisionEvent(t *testing.T) {
	installRecordingProvider(t)
	rec := installDecisionRecorder(t)
	i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "compiled"}}, trace.NewOTelTracer())
	ctx, span := otel.Tracer("test").Start(context.Background(), "req")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil {
		t.Fatal(err)
	}
	<-cr.Handle.Done()
	span.End()

	recs := decisionRecords(rec)
	if len(recs) != 1 || recs[0].EventName != trace.EventDecision {
		t.Fatalf("records = %+v", recs)
	}
	a := recs[0].Attributes
	if attrString(a, trace.AttrRuleName) != "lat" || attrString(a, trace.AttrReason) != "compiled" || attrString(a, trace.AttrFaultID) != cr.RunID || attrString(a, trace.AttrInjectionPoint) != "ingress" || attrString(a, "atropos.action") != "fault.start" {
		t.Fatalf("attributes = %v", a)
	}
	if oteltrace.SpanContextFromContext(recs[0].Context).TraceID() != span.SpanContext().TraceID() {
		t.Fatal("decision event must be emitted with the request context")
	}
}

// The brief joined a 60 ms fault, so the second Check had to come within
// 60 ms of the first; the fault here runs until reg.Stop.
func TestStartFault_JoinEmitsDecisionEvent(t *testing.T) {
	installRecordingProvider(t)
	rec := installDecisionRecorder(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: heldDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	first, _ := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	second, _ := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	reg.Stop("bg")
	<-first.Handle.Done()
	recs := decisionRecords(rec)
	if len(recs) != 2 || attrString(recs[1].Attributes, "atropos.action") != "fault.join" || attrString(recs[1].Attributes, trace.AttrFaultID) != second.RunID {
		t.Fatalf("records = %+v", recs)
	}
}

// A decision record carries the request's span, not the fault span the
// interceptor starts: on the direct path the fault span is the request's
// child (same trace, another span); on the registry path it is a root of its
// own trace.
func TestStartFault_DecisionRecordCarriesTheRequestSpan(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision *evaluator.Decision
		registry bool
	}{
		{"direct", &evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "test"}, false},
		{"registry", heldDecision("held"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := installRecordingProvider(t)
			rec := installDecisionRecorder(t)
			var opts []Option
			reg := NewFaultRegistry()
			t.Cleanup(reg.Close)
			if tc.registry {
				opts = append(opts, WithRegistry(reg))
			}
			i := New(&mockEvaluator{decision: tc.decision}, trace.NewOTelTracer(), opts...)
			ctx, span := otel.Tracer("test").Start(context.Background(), "req")
			cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
			if err != nil || cr.Handle == nil {
				t.Fatalf("cr=%+v err=%v", cr, err)
			}
			span.End()
			reg.Stop("held")
			<-cr.Handle.Done()

			recs := decisionRecords(rec)
			if len(recs) != 1 {
				t.Fatalf("records = %d, want 1", len(recs))
			}
			got := oteltrace.SpanContextFromContext(recs[0].Context)
			fs := faultSpanOf(t, exp.GetSpans(), cr.RunID)
			if got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() || got.SpanID() == fs.SpanContext.SpanID() {
				t.Fatalf("record span = %s/%s, want the request's %s/%s (the fault span is %s)", got.TraceID(), got.SpanID(), span.SpanContext().TraceID(), span.SpanContext().SpanID(), fs.SpanContext.SpanID())
			}
		})
	}
}

// Ruling 3: an arm's fresh start is an applied decision (fault.start, at the
// rule's injection point, with no request span); a re-arm that meets the
// running fault emits nothing; a request that joins the armed fault emits
// fault.join under its own context.
func TestStartFault_ArmEmitsStartButNoJoin(t *testing.T) {
	rec := installDecisionRecorder(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(nil, trace.Noop(), WithRegistry(reg))
	armed := ArmedContext(context.Background())
	req := evaluator.Request{Point: evaluator.Egress, Labels: map[string]string{}}

	start, err := i.StartFault(armed, req, heldDecision("armed"))
	if err != nil || start.Handle == nil {
		t.Fatalf("arm = %+v %v, want a start", start, err)
	}
	if again, err := i.StartFault(armed, req, heldDecision("armed")); err != nil || !again.Joined {
		t.Fatalf("re-arm = %+v %v, want a join", again, err)
	}
	recs := decisionRecords(rec)
	if len(recs) != 1 {
		t.Fatalf("records after arm and re-arm = %d, want 1 (the start)", len(recs))
	}
	a := recs[0].Attributes
	if attrString(a, "atropos.action") != "fault.start" || attrString(a, trace.AttrInjectionPoint) != "egress" || attrString(a, trace.AttrFaultID) != start.RunID || attrString(a, trace.AttrRuleName) != "armed" {
		t.Fatalf("armed start attributes = %v", a)
	}
	if sc := oteltrace.SpanContextFromContext(recs[0].Context); sc.IsValid() {
		t.Fatalf("armed start record carries span %s, want none", sc.SpanID())
	}

	ctx, sc := sampledContext(t)
	if cr, err := i.StartFault(ctx, req, heldDecision("armed")); err != nil || !cr.Joined {
		t.Fatalf("request = %+v %v, want a join", cr, err)
	}
	reg.Stop("armed")
	<-start.Handle.Done()
	recs = decisionRecords(rec)
	if len(recs) != 2 || attrString(recs[1].Attributes, "atropos.action") != "fault.join" || attrString(recs[1].Attributes, trace.AttrFaultID) != start.RunID {
		t.Fatalf("records = %+v, want the start, then the request's join", recs)
	}
	if got := oteltrace.SpanContextFromContext(recs[1].Context); got.SpanID() != sc.SpanID() {
		t.Fatalf("join record span = %s, want the request's %s", got.SpanID(), sc.SpanID())
	}
}

// recentTraceIDs returns a record's atropos.recent.trace_ids, and whether it
// has the attribute.
func recentTraceIDs(attrs []attribute.KeyValue) ([]string, bool) {
	for _, a := range attrs {
		if a.Key == trace.AttrRecentTraceIDs {
			return a.Value.AsStringSlice(), true
		}
	}
	return nil, false
}

// spanIn is a sampled span context of trace tr with span id sp.
func spanIn(tr, sp byte) context.Context {
	return oteltrace.ContextWithSpanContext(context.Background(), oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{tr, 7}, SpanID: oteltrace.SpanID{sp, 7}, TraceFlags: oteltrace.FlagsSampled,
	}))
}

// Ruling 4: the ring holds one entry per evaluation, so a request that made
// several calls repeats its trace. The decision record names each trace
// once, newest first -- the deciding request's own trace among them -- while
// the ring keeps every entry.
func TestStartFault_DecisionRecentTraceIDsAreDistinctNewestFirst(t *testing.T) {
	rec := installDecisionRecorder(t)
	i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "test"}}, trace.Noop())
	for _, ctx := range []context.Context{spanIn(1, 1), spanIn(1, 2), spanIn(2, 3), spanIn(1, 4)} {
		i.Evaluate(ctx, evaluator.Request{Point: evaluator.Egress})
	}
	cr, err := i.Check(spanIn(2, 5), evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()

	recs := decisionRecords(rec)
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	t1 := oteltrace.SpanContextFromContext(spanIn(1, 1)).TraceID().String()
	t2 := oteltrace.SpanContextFromContext(spanIn(2, 3)).TraceID().String()
	if got, _ := recentTraceIDs(recs[0].Attributes); !slices.Equal(got, []string{t2, t1}) {
		t.Fatalf("recent trace ids = %v, want [%s %s]", got, t2, t1)
	}
	if n := len(i.recent.TraceIDs()); n != 5 {
		t.Fatalf("the ring holds %d entries, want all 5", n)
	}
}

// A decision record's recent trace ids are left out when the ring holds no
// request (an arm before any traffic).
func TestStartFault_DecisionWithAnEmptyRingHasNoRecentTraceIDs(t *testing.T) {
	rec := installDecisionRecorder(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(nil, trace.Noop(), WithRegistry(reg))
	cr, err := i.StartFault(ArmedContext(context.Background()), evaluator.Request{Point: evaluator.Ingress, Labels: map[string]string{}}, heldDecision("armed"))
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	reg.Stop("armed")
	<-cr.Handle.Done()
	recs := decisionRecords(rec)
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	if ids, ok := recentTraceIDs(recs[0].Attributes); ok {
		t.Fatalf("recent trace ids = %v, want the attribute left out", ids)
	}
}

// A fault that fails to start is no applied decision: neither path emits.
func TestStartFault_FailedStartEmitsNoDecision(t *testing.T) {
	for _, withRegistry := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "registry"}[withRegistry], func(t *testing.T) {
			rec := installDecisionRecorder(t)
			var opts []Option
			if withRegistry {
				reg := NewFaultRegistry()
				t.Cleanup(reg.Close)
				opts = append(opts, WithRegistry(reg))
			}
			i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "fails", Fault: failStartFault{}, Mode: evaluator.Background, Reason: "test"}}, trace.Noop(), opts...)
			if _, err := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress}); err == nil {
				t.Fatal("want the start error")
			}
			if recs := decisionRecords(rec); len(recs) != 0 {
				t.Fatalf("records = %+v, want none", recs)
			}
		})
	}
}

// When the registered logger provider would drop a decision event, the
// interceptor does not emit one (nor assemble its recent trace ids). The
// recorder stores whatever reaches Emit, enabled or not.
func TestStartFault_NoDecisionRecordWhenLogsAreDisabled(t *testing.T) {
	rec := logtest.NewRecorder(logtest.WithEnabledFunc(func(context.Context, otellog.EnabledParameters) bool { return false }))
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(rec)
	t.Cleanup(func() { global.SetLoggerProvider(prev) })
	i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "test"}}, trace.Noop())
	cr, err := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	if recs := decisionRecords(rec); len(recs) != 0 {
		t.Fatalf("records = %d, want none: the provider said it would drop them", len(recs))
	}
}

// recordingLogExporter is an in-memory sdklog.Exporter, the shape of the
// instrument package's test helper.
type recordingLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordingLogExporter) Export(_ context.Context, rs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rs {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (e *recordingLogExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }
func (e *recordingLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

// sdkAttr returns an exported record's string attribute.
func sdkAttr(r sdklog.Record, key string) string {
	var v string
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			v = kv.Value.AsString()
			return false
		}
		return true
	})
	return v
}

// gatedFault's Start waits until its gate opens, then runs until stopped.
type gatedFault struct{ gate chan struct{} }

func (f *gatedFault) Validate() error { return nil }
func (f *gatedFault) Start(ctx context.Context) (*fault.Handle, error) {
	<-f.gate
	ctx, cancel := context.WithCancel(ctx)
	h := fault.NewHandle(cancel)
	go func() {
		<-ctx.Done()
		h.Send(fault.Result{})
	}()
	return h, nil
}

// Ruling 6: a Transient fault starts after the response, on the request's
// context detached from its cancellation, when otelhttp has already ended the
// server span. The decision is emitted with that context, so the record the
// SDK exports still carries the server span's trace and span ids. The
// fault's Start is held until ServeHTTP has returned, so the server span has
// ended before the decision is emitted.
func TestStartTransient_DecisionRecordCarriesTheEndedServerSpan(t *testing.T) {
	logs := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(logs)))
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(lp)
	t.Cleanup(func() { global.SetLoggerProvider(prev); _ = lp.Shutdown(context.Background()) })
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	f := &gatedFault{gate: make(chan struct{})}
	hooks := newHookCalls()
	i := New(evaluator.NewStaticEvaluator(transientRule("after", nil, f)), trace.Noop(), WithRegistry(reg), WithInjectionHook(hooks.hook()))
	var server oteltrace.SpanContext
	h := otelhttp.NewHandler(i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server = oteltrace.SpanContextFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})), "svc", otelhttp.WithTracerProvider(tp))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	if ended := spans.GetSpans(); len(ended) != 1 || ended[0].SpanContext.SpanID() != server.SpanID() {
		t.Fatalf("ended spans = %d, want the server span ended once ServeHTTP returned", len(ended))
	}
	close(f.gate)
	if p, ok := hooks.next(2 * time.Second); !ok || p != "transient" {
		t.Fatalf("injection hook = %q (called=%v), want transient", p, ok)
	}
	defer reg.Stop("after")

	recs := logs.Records()
	if len(recs) != 1 {
		t.Fatalf("exported records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r.EventName() != trace.EventDecision || sdkAttr(r, "atropos.action") != "fault.start" || sdkAttr(r, trace.AttrInjectionPoint) != "transient" {
		t.Fatalf("record event %q action %q point %q", r.EventName(), sdkAttr(r, "atropos.action"), sdkAttr(r, trace.AttrInjectionPoint))
	}
	if !server.IsValid() || r.TraceID() != server.TraceID() || r.SpanID() != server.SpanID() || !r.TraceFlags().IsSampled() {
		t.Fatalf("record trace %s span %s flags %s, want the ended server span's %s/%s, sampled", r.TraceID(), r.SpanID(), r.TraceFlags(), server.TraceID(), server.SpanID())
	}
}
