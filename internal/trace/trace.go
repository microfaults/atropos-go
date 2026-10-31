package trace

import (
	"context"
	"fmt"

	fault "git.ucsc.edu/microfaults/atropos-go/internal/fault"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const tracerName = "git.ucsc.edu/microfaults/atropos-go/fault"

// Tracer creates and manages spans.
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, Span)
	// StartLinked is Start with links to other spans, set when the span
	// starts. A link whose span context is invalid is not dropped for that
	// reason alone (the OTel SDK keeps one that carries attributes), so a
	// caller links only spans it knows exist.
	StartLinked(ctx context.Context, name string, links []oteltrace.Link, attrs ...attribute.KeyValue) (context.Context, Span)
}

// Span records attributes, events, links, and lifecycle signals on a trace
// span.
type Span interface {
	SetAttributes(attrs ...attribute.KeyValue)
	AddEvent(name string, attrs ...attribute.KeyValue)
	// AddLink links the span to another after it started.
	AddLink(l oteltrace.Link)
	// SpanContext identifies the span, e.g. as the target of another span's
	// link; the zero value when there is no span behind it.
	SpanContext() oteltrace.SpanContext
	RecordResult(r fault.Result)
	EndWithError(err error)
	End()
}

// OTelTracer delegates to the global OTel TracerProvider.
type OTelTracer struct {
	tracer oteltrace.Tracer
}

// NewOTelTracer creates a tracer from the global TracerProvider.
func NewOTelTracer() *OTelTracer {
	return &OTelTracer{tracer: otel.Tracer(tracerName)}
}

func (t *OTelTracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, Span) {
	ctx, span := t.tracer.Start(ctx, name,
		oteltrace.WithAttributes(attrs...),
	)
	return ctx, &otelSpan{span: span}
}

func (t *OTelTracer) StartLinked(ctx context.Context, name string, links []oteltrace.Link, attrs ...attribute.KeyValue) (context.Context, Span) {
	ctx, span := t.tracer.Start(ctx, name,
		oteltrace.WithLinks(links...),
		oteltrace.WithAttributes(attrs...),
	)
	return ctx, &otelSpan{span: span}
}

type otelSpan struct {
	span oteltrace.Span
}

func (s *otelSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.span.SetAttributes(attrs...)
}

func (s *otelSpan) AddEvent(name string, attrs ...attribute.KeyValue) {
	s.span.AddEvent(name, oteltrace.WithAttributes(attrs...))
}

func (s *otelSpan) AddLink(l oteltrace.Link) {
	s.span.AddLink(l)
}

func (s *otelSpan) SpanContext() oteltrace.SpanContext {
	return s.span.SpanContext()
}

func (s *otelSpan) RecordResult(r fault.Result) {
	s.span.SetAttributes(
		attribute.Int64(AttrFaultDurationMs, r.ActualDuration.Milliseconds()),
		attribute.String(AttrFaultActualDuration, r.ActualDuration.String()),
	)
	if r.Err != nil {
		s.span.SetStatus(codes.Error, r.Err.Error())
		s.span.RecordError(r.Err)
	} else {
		s.span.SetStatus(codes.Ok, "completed")
	}
	if r.Detail != nil {
		s.span.SetAttributes(attribute.String(AttrFaultDetail, fmt.Sprintf("%+v", r.Detail)))
	}
	s.span.End()
}

func (s *otelSpan) EndWithError(err error) {
	s.span.SetStatus(codes.Error, err.Error())
	s.span.RecordError(err)
	s.span.End()
}

func (s *otelSpan) End() {
	s.span.End()
}
