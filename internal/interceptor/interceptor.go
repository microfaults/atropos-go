package interceptor

import (
	"context"
	"fmt"

	"atropos-go/internal/evaluator"
	fault "atropos-go/internal/fault"
	"atropos-go/internal/trace"
)

// Interceptor evaluates rules at injection points and executes faults
// with OTel instrumentation. It is the glue between the evaluator
// (rule engine), fault execution, and observability.
type Interceptor struct {
	eval   evaluator.Evaluator
	tracer trace.Tracer
}

// New creates an Interceptor. If tracer is nil, a no-op tracer is used.
func New(eval evaluator.Evaluator, tracer trace.Tracer) *Interceptor {
	if tracer == nil {
		tracer = trace.Noop()
	}
	return &Interceptor{eval: eval, tracer: tracer}
}

// CheckResult holds the outcome of an injection point check.
type CheckResult struct {
	// Handle is non-nil if a fault was started. The caller can wait
	// on Handle.Done() (for inline faults) or let it run (background).
	Handle *fault.Handle

	// Decision is non-nil if the evaluator matched a rule.
	Decision *evaluator.Decision
}

// Check evaluates rules at the given injection point. If a fault is
// selected, it validates it, starts it with an OTel span, and returns
// the handle. Returns a zero CheckResult if no fault was selected.
//
// The OTel span is a child of whatever trace is active in ctx.
// It ends automatically when the fault completes (via Handle.OnResult).
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
	ctx, span := i.tracer.Start(ctx, faultType, req.Point.String(), decision.Reason)

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
