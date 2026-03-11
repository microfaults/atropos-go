package atropos

import (
	"context"

	"atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Span creates an always-on span. Caller must End() it.
func Span(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, TraceSpan) {
	tracer := trace.NewOTelTracer()
	return tracer.Start(ctx, trace.SpanHookPrefix+name, attrs...)
}

// SpanWithFault creates an always-on span and checks for fault injection.
// Labels flow to both the evaluator (rule matching) and the span (attributes).
func SpanWithFault(ctx context.Context, name string, labels map[string]string, attrs ...attribute.KeyValue) (context.Context, TraceSpan, CheckResult, error) {
	if labels == nil {
		labels = make(map[string]string)
	}
	for _, a := range attrs {
		labels[string(a.Key)] = a.Value.Emit()
	}

	return defaultInterceptor.Hook(ctx, name, labels)
}
