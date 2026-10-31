package trace

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// newTestTracer is an OTelTracer on a provider of its own (not the global
// one) that exports synchronously to the returned exporter.
func newTestTracer(t *testing.T) (*OTelTracer, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return &OTelTracer{tracer: tp.Tracer(tracerName)}, exp
}

func spanContext(b byte) oteltrace.SpanContext {
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{b}, SpanID: oteltrace.SpanID{b}, TraceFlags: oteltrace.FlagsSampled,
	})
}

func kind(k string) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String(AttrLinkKind, k)}
}

// StartLinked starts the span with its links and attributes, AddLink adds one
// afterwards, and SpanContext is the wrapped span's own.
func TestOTelTracer_StartLinkedAddLinkAndSpanContext(t *testing.T) {
	tr, exp := newTestTracer(t)
	_, span := tr.StartLinked(context.Background(), "s",
		[]oteltrace.Link{{SpanContext: spanContext(1), Attributes: kind(LinkKindTrigger)}},
		attribute.String(AttrFaultID, "run-1"))
	span.AddLink(oteltrace.Link{SpanContext: spanContext(2), Attributes: kind(LinkKindFault)})
	sc := span.SpanContext()
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	got := spans[0]
	if !sc.IsValid() || got.SpanContext.SpanID() != sc.SpanID() || got.SpanContext.TraceID() != sc.TraceID() {
		t.Fatalf("SpanContext() = %s/%s, want the exported span's %s/%s", sc.TraceID(), sc.SpanID(), got.SpanContext.TraceID(), got.SpanContext.SpanID())
	}
	if len(got.Links) != 2 {
		t.Fatalf("links = %+v, want the start link and the added one", got.Links)
	}
	for k, want := range []struct {
		sc   oteltrace.SpanContext
		kind string
	}{{spanContext(1), LinkKindTrigger}, {spanContext(2), LinkKindFault}} {
		l := got.Links[k]
		if l.SpanContext.SpanID() != want.sc.SpanID() || len(l.Attributes) != 1 || l.Attributes[0].Value.AsString() != want.kind {
			t.Fatalf("link %d = %+v, want span %s with %s=%s", k, l, want.sc.SpanID(), AttrLinkKind, want.kind)
		}
	}
	var id string
	for _, a := range got.Attributes {
		if a.Key == AttrFaultID {
			id = a.Value.AsString()
		}
	}
	if id != "run-1" {
		t.Fatalf("attributes = %v, want %s=run-1", got.Attributes, AttrFaultID)
	}
}

type ctxKey struct{}

// The no-op tracer hands the context back unchanged, and its span has no
// identity to link to.
func TestNoop_StartLinkedAndSpanContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	got, span := Noop().StartLinked(ctx, "s", []oteltrace.Link{{SpanContext: spanContext(1)}})
	if got != ctx {
		t.Fatal("StartLinked must return the context it was given")
	}
	span.AddLink(oteltrace.Link{SpanContext: spanContext(2)})
	if span.SpanContext().IsValid() {
		t.Fatalf("SpanContext() = %v, want the zero value", span.SpanContext())
	}
}
