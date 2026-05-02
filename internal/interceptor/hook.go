package interceptor

import (
	"context"
	"fmt"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	fault "git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Hook always creates a span and checks for faults. Caller must End() the span.
func (i *Interceptor) Hook(ctx context.Context, name string, labels map[string]string) (context.Context, trace.Span, CheckResult, error) {
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[trace.AttrHookName] = name

	// Always create a hook span for the annotated code block.
	ctx, hookSpan := i.tracer.Start(ctx, trace.SpanHookPrefix+name,
		attribute.String(trace.AttrHookName, name),
	)

	// Thread developer labels onto the hook span.
	for k, v := range labels {
		hookSpan.SetAttributes(attribute.String(k, v))
	}

	// Check for faults — the fault span nests under the hook span
	// because ctx already carries the hook span.
	cr, err := i.Check(ctx, evaluator.Request{
		Point:  evaluator.Custom,
		Labels: labels,
	})
	if err != nil {
		hookSpan.AddEvent(trace.EventFaultCheckErr,
			attribute.String("error", err.Error()),
		)
		return ctx, hookSpan, cr, err
	}

	if cr.Handle != nil {
		hookSpan.AddEvent(trace.EventFaultInjected,
			attribute.String(trace.AttrFaultType, fmt.Sprintf("%T", cr.Decision.Fault)),
			attribute.String(trace.AttrFaultReason, cr.Decision.Reason),
		)
	} else {
		hookSpan.AddEvent(trace.EventFaultSkipped)
	}

	return ctx, hookSpan, cr, nil
}

// MustFault bypasses the evaluator and injects the fault directly.
func (i *Interceptor) MustFault(ctx context.Context, point evaluator.InjectionPoint, name string, f fault.Fault) (*fault.Handle, error) {
	// Bypass evaluator — inject directly.
	return i.startTraced(ctx, point, name, f)
}

func (i *Interceptor) startTraced(ctx context.Context, point evaluator.InjectionPoint, name string, f fault.Fault) (*fault.Handle, error) {
	cr, err := i.Check(ctx, evaluator.Request{
		Point:  point,
		Labels: map[string]string{trace.AttrHookName: name},
	})
	if err != nil {
		return nil, err
	}
	if cr.Handle != nil {
		return cr.Handle, nil
	}

	// If evaluator returned nil (no decision), but MustFault was called,
	// start the fault directly with tracing.
	return i.directStart(ctx, point, name, f)
}

func (i *Interceptor) directStart(ctx context.Context, point evaluator.InjectionPoint, name string, f fault.Fault) (*fault.Handle, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	ctx, span := i.tracer.Start(ctx, trace.SpanFaultInject,
		attribute.String(trace.AttrFaultType, fmt.Sprintf("%T", f)),
		attribute.String(trace.AttrFaultInjectionPoint, point.String()),
		attribute.String(trace.AttrFaultReason, "direct: "+name),
	)

	// If the fault can emit events, wire it up to the span.
	if ea, ok := f.(fault.EventAware); ok {
		ea.SetEventEmitter(func(evName string, attrs ...attribute.KeyValue) {
			span.AddEvent(evName, attrs...)
		})
	}

	handle, err := f.Start(ctx)
	if err != nil {
		span.EndWithError(err)
		return nil, err
	}

	handle.SetOnResult(func(r fault.Result) {
		span.RecordResult(r)
	})

	return handle, nil
}
