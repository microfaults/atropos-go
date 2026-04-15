package interceptor

import (
	"context"
	"fmt"

	"atropos-go/internal/cachebox"
	"atropos-go/internal/evaluator"
	fault "atropos-go/internal/fault"
	"atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Option configures an Interceptor at construction time.
type Option func(*Interceptor)

// WithCacheBox attaches a cache-box coordinator. A nil coordinator disables
// cache-box dispatch (the middleware falls through to the fault path).
func WithCacheBox(cb *cachebox.CacheBox) Option {
	return func(i *Interceptor) { i.cacheBox = cb }
}

// Interceptor ties the evaluator, fault execution, OTel, and cache-box
// together. It is the per-service policy dispatcher -- middleware layers
// call into it at each injection point.
type Interceptor struct {
	eval     evaluator.Evaluator
	tracer   trace.Tracer
	cacheBox *cachebox.CacheBox
}

// noopEval never matches (tracing only, no faults).
type noopEval struct{}

func (noopEval) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return nil
}

// New creates an Interceptor. Nil eval/tracer use no-op defaults.
func New(eval evaluator.Evaluator, tracer trace.Tracer, opts ...Option) *Interceptor {
	if eval == nil {
		eval = noopEval{}
	}
	if tracer == nil {
		tracer = trace.Noop()
	}
	i := &Interceptor{eval: eval, tracer: tracer}
	for _, opt := range opts {
		if opt != nil {
			opt(i)
		}
	}
	return i
}

// CacheBox returns the configured cache-box coordinator, or nil if disabled.
// Middleware uses this to decide whether to dispatch on cache-box decisions.
func (i *Interceptor) CacheBox() *cachebox.CacheBox {
	return i.cacheBox
}

// Tracer returns the interceptor's tracer. Used by cache-box dispatch.
func (i *Interceptor) Tracer() trace.Tracer {
	return i.tracer
}

// CheckResult holds the outcome of a fault-injection check.
type CheckResult struct {
	Handle   *fault.Handle
	Decision *evaluator.Decision
}

// Evaluate runs the rule engine and returns the matching decision, if any.
// This is a side-effect-free read of the current rule set; callers must
// dispatch (StartFault or cache-box handleCacheBox) based on the result.
func (i *Interceptor) Evaluate(ctx context.Context, req evaluator.Request) *evaluator.Decision {
	return i.eval.Evaluate(ctx, req)
}

// StartFault validates the fault on a decision, creates a fault-injection
// span, and starts the fault. Returns an empty CheckResult if the decision
// is nil or has no fault (i.e. a cache-box-only decision).
//
// The decision is assumed to be the output of a prior Evaluate call; this
// method does not re-run the evaluator.
func (i *Interceptor) StartFault(ctx context.Context, req evaluator.Request, decision *evaluator.Decision) (CheckResult, error) {
	if decision == nil || decision.Fault == nil {
		return CheckResult{}, nil
	}

	if err := decision.Fault.Validate(); err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: invalid fault from evaluator: %w", err)
	}

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

	// Hook into the handle: when the fault ends, record the result on the span.
	handle.SetOnResult(func(r fault.Result) {
		span.RecordResult(r)
	})

	return CheckResult{Handle: handle, Decision: decision}, nil
}

// Check is a convenience that evaluates and (if appropriate) starts a fault.
// It is the fault-only path: cache-box decisions are explicitly skipped so
// that callers who don't handle cache-box won't accidentally treat a
// cache-box decision as a "no-op" fault match.
//
// HTTP middleware layers that want to dispatch cache-box should call
// Evaluate directly and branch on the Decision.
func (i *Interceptor) Check(ctx context.Context, req evaluator.Request) (CheckResult, error) {
	decision := i.Evaluate(ctx, req)
	if decision == nil {
		return CheckResult{}, nil
	}
	if decision.CacheBox != evaluator.CacheBoxNone {
		// Cache-box decision -- Check() is the fault-only path; middleware
		// that handles cache-box uses Evaluate directly.
		return CheckResult{}, nil
	}
	return i.StartFault(ctx, req, decision)
}
