package interceptor

import (
	"context"

	"atropos-go/internal/evaluator"
	fault "atropos-go/internal/fault"
)

// Hook checks for faults at a developer-annotated code point.
//
// Usage:
//
//	cr, _ := interceptor.Hook(ctx, "process-payment", map[string]string{"amount": "100"})
//	if cr.Handle != nil {
//	    <-cr.Handle.Done()
//	}
func (i *Interceptor) Hook(ctx context.Context, name string, labels map[string]string) (CheckResult, error) {
	if labels == nil {
		labels = make(map[string]string)
	}
	labels["hook.name"] = name

	return i.Check(ctx, evaluator.Request{
		Point:  evaluator.Custom,
		Labels: labels,
	})
}

// MustFault is a convenience for injection points where a specific fault
// is always injected (useful for testing without an evaluator).
// It starts the fault with OTel tracing and returns the handle.
func (i *Interceptor) MustFault(ctx context.Context, point evaluator.InjectionPoint, name string, f fault.Fault) (*fault.Handle, error) {
	// Bypass evaluator — inject directly.
	return i.startTraced(ctx, point, name, f)
}

func (i *Interceptor) startTraced(ctx context.Context, point evaluator.InjectionPoint, name string, f fault.Fault) (*fault.Handle, error) {
	cr, err := i.Check(ctx, evaluator.Request{
		Point:  point,
		Labels: map[string]string{"hook.name": name},
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

	faultType := ""
	ctx, span := i.tracer.Start(ctx, faultType, point.String(), "direct: "+name)

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
