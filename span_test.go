package atropos

import (
	"context"
	"testing"
	"time"

	"github.com/microfaults/atropos-go/internal/evaluator"
	"github.com/microfaults/atropos-go/internal/fault/inline"
	"github.com/microfaults/atropos-go/internal/interceptor"
	"github.com/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTestTP(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { tp.Shutdown(context.Background()) })
	return exporter
}

func TestSpan_CreatesSpan(t *testing.T) {
	exporter := setupTestTP(t)

	ctx, span := Span(context.Background(), "drain-queue",
		attribute.Int("queue_depth", 42),
	)
	_ = ctx
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	expectedName := trace.SpanHookPrefix + "drain-queue"
	if s.Name != expectedName {
		t.Fatalf("expected span name %q, got %q", expectedName, s.Name)
	}

	// Verify the attribute was set.
	found := false
	for _, a := range s.Attributes {
		if string(a.Key) == "queue_depth" && a.Value.AsInt64() == 42 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected queue_depth=42 attribute on span")
	}
}

func TestSpan_SetAttributesAfterCreation(t *testing.T) {
	exporter := setupTestTP(t)

	ctx, span := Span(context.Background(), "process",
		attribute.String("step", "start"),
	)
	_ = ctx

	// Add attributes after creation.
	span.SetAttributes(attribute.Int("items_processed", 17))
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	found := false
	for _, a := range s(spans[0]) {
		if string(a.Key) == "items_processed" && a.Value.AsInt64() == 17 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected items_processed=17 attribute")
	}
}

func s(span tracetest.SpanStub) []attribute.KeyValue {
	return span.Attributes
}

func TestSpanWithFault_NoFault(t *testing.T) {
	exporter := setupTestTP(t)

	// Configure with nil evaluator (no faults).
	defaultInterceptor = interceptor.New(nil, trace.NewOTelTracer())

	ctx, span, cr, err := SpanWithFault(context.Background(), "checkout",
		map[string]string{"tenant": "acme"},
		attribute.String("customer_id", "c-123"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx

	// No fault should fire.
	if cr.Handle != nil {
		t.Fatal("expected nil handle with nil evaluator")
	}

	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span (hook only, no fault), got %d", len(spans))
	}

	hookSpan := spans[0]
	expectedName := trace.SpanHookPrefix + "checkout"
	if hookSpan.Name != expectedName {
		t.Fatalf("expected %q, got %q", expectedName, hookSpan.Name)
	}

	// Verify labels and attrs are on the span.
	attrMap := make(map[string]string)
	for _, a := range hookSpan.Attributes {
		attrMap[string(a.Key)] = a.Value.Emit()
	}
	if attrMap["tenant"] != "acme" {
		t.Errorf("expected tenant=acme, got %q", attrMap["tenant"])
	}

	// Verify fault.skipped event.
	foundSkipped := false
	for _, ev := range hookSpan.Events {
		if ev.Name == trace.EventFaultSkipped {
			foundSkipped = true
		}
	}
	if !foundSkipped {
		t.Fatal("expected fault.skipped event on hook span")
	}
}

type testEvaluator struct {
	decision *evaluator.Decision
}

func (e *testEvaluator) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return e.decision
}

func TestSpanWithFault_WithFault(t *testing.T) {
	exporter := setupTestTP(t)

	// Configure with a latency fault.
	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 20 * time.Millisecond},
			Reason: "test: always",
			Mode:   evaluator.Inline,
		},
	}
	defaultInterceptor = interceptor.New(eval, trace.NewOTelTracer())

	ctx, hookSpan, cr, err := SpanWithFault(context.Background(), "payment",
		map[string]string{"amount": "100"},
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx
	if cr.Handle == nil {
		t.Fatal("expected handle from fault")
	}

	// Wait for inline fault.
	<-cr.Handle.Done()
	hookSpan.End()

	spans := exporter.GetSpans()
	// Should have 2 spans: hook span + fault.inject child span.
	if len(spans) < 2 {
		t.Fatalf("expected >= 2 spans (hook + fault), got %d", len(spans))
	}

	// Find the fault.inject span.
	var faultSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == trace.SpanFaultInject {
			faultSpan = &spans[i]
			break
		}
	}
	if faultSpan == nil {
		t.Fatal("expected a fault.inject span")
	}

	// Verify the fault span is a child of the hook span.
	var hookS *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == trace.SpanHookPrefix+"payment" {
			hookS = &spans[i]
			break
		}
	}
	if hookS == nil {
		t.Fatal("expected hook span")
	}

	if faultSpan.Parent.SpanID() != hookS.SpanContext.SpanID() {
		t.Fatalf("fault span parent (%s) != hook span ID (%s)",
			faultSpan.Parent.SpanID(), hookS.SpanContext.SpanID())
	}

	// Verify fault.injected event on hook span.
	foundInjected := false
	for _, ev := range hookS.Events {
		if ev.Name == trace.EventFaultInjected {
			foundInjected = true
		}
	}
	if !foundInjected {
		t.Fatal("expected fault.injected event on hook span")
	}
}
