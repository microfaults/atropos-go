package interceptor

import (
	"context"
	"fmt"

	"github.com/microfaults/atropos-go/internal/cachebox"
	"github.com/microfaults/atropos-go/internal/evaluator"
	fault "github.com/microfaults/atropos-go/internal/fault"
	inlinefault "github.com/microfaults/atropos-go/internal/fault/inline"
	"github.com/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Option configures an Interceptor at construction time.
type Option func(*Interceptor)

// WithCacheBox attaches a cache-box coordinator. A nil coordinator disables
// cache-box dispatch (the middleware falls through to the fault path).
func WithCacheBox(cb *cachebox.CacheBox) Option {
	return func(i *Interceptor) { i.cacheBox = cb }
}

// WithRegistry attaches a fault registry for deduplicating service-scoped
// faults (network proxies, CPU stress, etc.).
func WithRegistry(r *FaultRegistry) Option {
	return func(i *Interceptor) { i.registry = r }
}

// Interceptor ties the evaluator, fault execution, OTel, and cache-box
// together. It is the per-service policy dispatcher -- middleware layers
// call into it at each injection point.
type Interceptor struct {
	eval     evaluator.Evaluator
	tracer   trace.Tracer
	cacheBox *cachebox.CacheBox
	registry *FaultRegistry
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

// isInlineFault reports whether f is one of the inline fault types (latency,
// error, hang) that execute in the request goroutine.
func isInlineFault(f fault.Fault) bool {
	switch f.(type) {
	case *inlinefault.Latency, *inlinefault.Error, *inlinefault.Hang:
		return true
	default:
		return false
	}
}

// StartFault validates the fault on a decision and starts it. Non-inline
// faults are forced to Background mode. If a registry is configured,
// non-inline faults are routed through it for deduplication.
//
// Returns an empty CheckResult if the decision is nil, has no fault, or was
// deduplicated by the registry.
func (i *Interceptor) StartFault(ctx context.Context, req evaluator.Request, decision *evaluator.Decision) (CheckResult, error) {
	if decision == nil || decision.Fault == nil {
		return CheckResult{}, nil
	}

	if err := decision.Fault.Validate(); err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: invalid fault from evaluator: %w", err)
	}

	faultType := fmt.Sprintf("%T", decision.Fault)

	// Force Background mode for non-inline faults.
	effectiveMode := decision.Mode
	if !isInlineFault(decision.Fault) {
		effectiveMode = evaluator.Background
	}

	// Registry path: non-inline faults with a registry get deduped.
	if i.registry != nil && !isInlineFault(decision.Fault) {
		return i.startRegistered(ctx, req, decision, faultType, effectiveMode)
	}

	// Direct path: inline faults (or no registry configured).
	return i.startDirect(ctx, req, decision, faultType, effectiveMode)
}

// startDirect is the inline-fault path. It creates a span, wires up events,
// starts the fault, and hooks the result callback.
func (i *Interceptor) startDirect(ctx context.Context, req evaluator.Request, decision *evaluator.Decision, faultType string, mode evaluator.Mode) (CheckResult, error) {
	ctx, span := i.tracer.Start(ctx, trace.SpanFaultInject,
		attribute.String(trace.AttrFaultType, faultType),
		attribute.String(trace.AttrFaultInjectionPoint, req.Point.String()),
		attribute.String(trace.AttrFaultReason, decision.Reason),
	)
	for k, v := range req.Labels {
		span.SetAttributes(attribute.String(k, v))
	}
	if ea, ok := decision.Fault.(fault.EventAware); ok {
		ea.SetEventEmitter(func(name string, attrs ...attribute.KeyValue) {
			span.AddEvent(name, attrs...)
		})
	}

	handle, err := decision.Fault.Start(ctx)
	if err != nil {
		span.EndWithError(err)
		return CheckResult{}, fmt.Errorf("interceptor: fault start failed: %w", err)
	}
	handle.SetOnResult(func(r fault.Result) {
		span.RecordResult(r)
	})

	d := *decision
	d.Mode = mode
	return CheckResult{Handle: handle, Decision: &d}, nil
}

// startRegistered routes non-inline faults through the registry for
// deduplication. The span is anchored to the registry context so faults can
// outlive the triggering request.
func (i *Interceptor) startRegistered(ctx context.Context, req evaluator.Request, decision *evaluator.Decision, faultType string, mode evaluator.Mode) (CheckResult, error) {
	key := decision.Name
	if key == "" {
		key = faultType
	}

	handle, deduped, err := i.registry.StartOrJoin(key, decision.StartPolicy, func(regCtx context.Context) (*fault.Handle, error) {
		_, span := i.tracer.Start(regCtx, trace.SpanFaultInject,
			attribute.String(trace.AttrFaultType, faultType),
			attribute.String(trace.AttrFaultInjectionPoint, req.Point.String()),
			attribute.String(trace.AttrFaultReason, decision.Reason),
		)
		for k, v := range req.Labels {
			span.SetAttributes(attribute.String(k, v))
		}
		if ea, ok := decision.Fault.(fault.EventAware); ok {
			ea.SetEventEmitter(func(name string, attrs ...attribute.KeyValue) {
				span.AddEvent(name, attrs...)
			})
		}
		h, err := decision.Fault.Start(regCtx)
		if err != nil {
			span.EndWithError(err)
			return nil, err
		}
		h.SetOnResult(func(r fault.Result) {
			span.RecordResult(r)
		})
		return h, nil
	})
	if err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: fault start failed: %w", err)
	}
	if deduped {
		return CheckResult{}, nil
	}

	d := *decision
	d.Mode = mode
	return CheckResult{Handle: handle, Decision: &d}, nil
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
