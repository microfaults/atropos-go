package interceptor

import (
	"context"
	"fmt"

	"github.com/microfaults/atropos-go/internal/evaluator"
	fault "github.com/microfaults/atropos-go/internal/fault"
	"github.com/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Interceptor ties the evaluator, fault execution, and OTel together.
type Interceptor struct {
	eval   evaluator.Evaluator
	tracer trace.Tracer
}

// noopEval never matches (tracing only, no faults).
type noopEval struct{}

func (noopEval) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return nil
}

// New creates an Interceptor. Nil eval/tracer use no-op defaults.
func New(eval evaluator.Evaluator, tracer trace.Tracer) *Interceptor {
	if eval == nil {
		eval = noopEval{}
	}
	if tracer == nil {
		tracer = trace.Noop()
	}
	return &Interceptor{eval: eval, tracer: tracer}
}

// CheckResult holds the outcome of an injection point check.
type CheckResult struct {
	Handle   *fault.Handle       // non-nil if a fault was started
	Decision *evaluator.Decision // non-nil if a rule matched
}

// Check evaluates rules and starts a fault if one matches.
func (i *Interceptor) Check(ctx context.Context, req evaluator.Request) (CheckResult, error) {
	decision := i.eval.Evaluate(ctx, req)
	if decision == nil {
		return CheckResult{}, nil
	}

	if err := decision.Fault.Validate(); err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: invalid fault from evaluator: %w", err)
	}

	// Start OTel span as child of current trace.
	faultType := fmt.Sprintf("%T", decision.Fault)
	ctx, span := i.tracer.Start(ctx, trace.SpanFaultInject,
		attribute.String(trace.AttrFaultType, faultType),
		attribute.String(trace.AttrFaultInjectionPoint, req.Point.String()),
		attribute.String(trace.AttrFaultReason, decision.Reason),
	)

	// Thread request labels into the span for trace correlation.
	for k, v := range req.Labels {
		span.SetAttributes(attribute.String(k, v))
	}

	// If the fault can emit events, wire it up to the span.
	if ea, ok := decision.Fault.(fault.EventAware); ok {
		ea.SetEventEmitter(func(name string, attrs ...attribute.KeyValue) {
			span.AddEvent(name, attrs...)
		})
	}

	// Start the fault with the span-carrying context.
	handle, err := decision.Fault.Start(ctx)
	if err != nil {
		span.EndWithError(err)
		return CheckResult{}, fmt.Errorf("interceptor: fault start failed: %w", err)
	}

	// Hook into the handle: when Send is called, end the span with result data.
	handle.SetOnResult(func(r fault.Result) {
		span.RecordResult(r)
	})

	return CheckResult{Handle: handle, Decision: decision}, nil
}
