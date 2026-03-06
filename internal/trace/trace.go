package trace

import (
	"context"
	"fmt"

	fault "atropos-go/internal/fault"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const tracerName = "atropos-go/fault"

// Tracer creates and manages spans for fault injection.
// Abstracted so the interceptor doesn't import OTel directly.
type Tracer interface {
	// Start creates a span for a fault injection event.
	// The returned context carries the span; the returned Span
	// is used to record the result and end the span.
	Start(ctx context.Context, faultType, injectionPoint, reason string) (context.Context, Span)
}

// Span records fault-specific attributes and ends the trace span.
type Span interface {
	RecordResult(r fault.Result)
	EndWithError(err error)
	End()
}

// OTelTracer is the production tracer backed by go.opentelemetry.io/otel.
// It uses the globally registered TracerProvider.
type OTelTracer struct {
	tracer oteltrace.Tracer
}

// NewOTelTracer creates a tracer using the global OTel TracerProvider.
func NewOTelTracer() *OTelTracer {
	return &OTelTracer{tracer: otel.Tracer(tracerName)}
}

func (t *OTelTracer) Start(ctx context.Context, faultType, injectionPoint, reason string) (context.Context, Span) {
	ctx, span := t.tracer.Start(ctx, "fault.inject",
		oteltrace.WithAttributes(
			attribute.String("fault.type", faultType),
			attribute.String("fault.injection_point", injectionPoint),
			attribute.String("fault.reason", reason),
		),
	)
	return ctx, &otelSpan{span: span}
}

type otelSpan struct {
	span oteltrace.Span
}

func (s *otelSpan) RecordResult(r fault.Result) {
	s.span.SetAttributes(
		attribute.Int64("fault.duration_ms", r.ActualDuration.Milliseconds()),
		attribute.String("fault.actual_duration", r.ActualDuration.String()),
	)
	if r.Err != nil {
		s.span.SetStatus(codes.Error, r.Err.Error())
		s.span.RecordError(r.Err)
	} else {
		s.span.SetStatus(codes.Ok, "completed")
	}
	if r.Detail != nil {
		s.span.SetAttributes(attribute.String("fault.detail", fmt.Sprintf("%+v", r.Detail)))
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
