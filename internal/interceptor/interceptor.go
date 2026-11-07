package interceptor

import (
	"context"
	"fmt"
	"log/slog"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	fault "git.ucsc.edu/microfaults/atropos-go/internal/fault"
	inlinefault "git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
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

// InjectionHook is called once each time the interceptor starts a fault,
// with the context the fault was started under (the request's, so it carries
// the request span; an eager arm's carries none), the fault's Go type and the
// injection point ("ingress", "egress", "transient" or "custom"). Every host
// wires it to metrics.FaultInjectionHook; keep it cheap and non-blocking.
type InjectionHook func(ctx context.Context, faultType, injectionPoint string)

// WithInjectionHook sets the per-injection callback. A nil hook is ignored.
func WithInjectionHook(h InjectionHook) Option {
	return func(i *Interceptor) { i.onInject = h }
}

// WithService names the service the interceptor dispatches for. The egress
// transport labels its cache-box counters with it; empty (the default) means
// the service is not known, as on the in-process baseline before Serve.
func WithService(name string) Option {
	return func(i *Interceptor) { i.service = name }
}

// Service is the service name WithService set; "" when none was.
func (i *Interceptor) Service() string { return i.service }

// WithLogger sets the logger the interceptor reports its own errors to: a
// failed fault check, a recovered panic. A log call that has a request passes
// the request's context, so a logger with an OpenTelemetry bridge stamps the
// record with the request's trace and span. A nil logger is ignored; the
// default is slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(i *Interceptor) {
		if l != nil {
			i.logger = l
		}
	}
}

// Interceptor ties the evaluator, fault execution, OTel, and cache-box
// together. It is the per-service policy dispatcher -- middleware layers
// call into it at each injection point.
type Interceptor struct {
	eval     evaluator.Evaluator
	tracer   trace.Tracer
	cacheBox *cachebox.CacheBox
	registry *FaultRegistry
	onInject InjectionHook
	logger   *slog.Logger
	service  string
	// recent holds the last requests this interceptor evaluated; background
	// fault spans link to them.
	recent *requestRing
}

// noopEval never matches (tracing only, no faults).
type noopEval struct{}

func (noopEval) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return nil
}

// New creates an Interceptor. Nil eval/tracer use no-op defaults, and the
// logger is slog.Default() unless WithLogger sets one.
func New(eval evaluator.Evaluator, tracer trace.Tracer, opts ...Option) *Interceptor {
	if eval == nil {
		eval = noopEval{}
	}
	if tracer == nil {
		tracer = trace.Noop()
	}
	i := &Interceptor{eval: eval, tracer: tracer, logger: slog.Default(), recent: newRequestRing(ringSize)}
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

// Logger returns the logger the interceptor reports its own errors to; never
// nil. The layers around it (the gRPC interceptors) log through it too, so one
// logger carries every request-scoped line.
func (i *Interceptor) Logger() *slog.Logger {
	return i.logger
}

// CheckResult holds the outcome of a fault-injection check. RunID is the id
// of the fault run the request started or joined (atropos.fault.id). Joined
// reports that the call joined a background run already in progress instead
// of starting one: Handle is then nil, so no caller waits on a run it did
// not start, while Decision still describes the fault. A join is reported
// whatever made it; the run counts it only when a request did -- any call
// whose context ArmedContext did not mark, with or without a span context
// (see startRegistered).
type CheckResult struct {
	Handle   *fault.Handle
	Decision *evaluator.Decision
	RunID    string
	Joined   bool
}

// armedKey is the context key ArmedContext sets.
type armedKey struct{}

// ArmedContext returns ctx marked as an eager arm's: a fault start that no
// request made (Core.Arm starts a rule's background fault when the rule
// applies, and re-arms it on every delivery). The interceptor treats a
// marked call as no request, whatever span context ctx carries: the span is
// not the run's trigger, it is not stamped with the run id or linked to the
// fault span, and a join the call makes is not counted. A call without the
// mark is a request, with or without a span context.
func ArmedContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, armedKey{}, true)
}

// isArmed reports whether ArmedContext marked ctx.
func isArmed(ctx context.Context) bool {
	armed, _ := ctx.Value(armedKey{}).(bool)
	return armed
}

// Evaluate records the request in the interceptor's ring of recent requests
// (a no-op unless ctx carries a sampled span), then runs the rule engine and
// returns the matching decision, if any. It does not touch the rule set;
// callers must dispatch (StartFault or cache-box handleCacheBox) based on
// the result. A Transient evaluation is not recorded: it runs for a request
// whose Ingress evaluation already was, and a second entry would spend a
// ring slot and a link on the same span.
func (i *Interceptor) Evaluate(ctx context.Context, req evaluator.Request) *evaluator.Decision {
	if req.Point != evaluator.Transient {
		i.recent.Record(oteltrace.SpanContextFromContext(ctx), req.Labels)
	}
	return i.eval.Evaluate(ctx, req)
}

// IsInlineFault reports whether f is one of the inline fault types (latency,
// error, hang) that execute in the request goroutine.
func IsInlineFault(f fault.Fault) bool {
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
// Returns an empty CheckResult if the decision is nil or has no fault. A
// start the registry deduplicated returns Joined with the running run's
// RunID and no Handle.
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
	if !IsInlineFault(decision.Fault) {
		effectiveMode = evaluator.Background
	}

	// Registry path: non-inline faults with a registry get deduped. The
	// fault span is a root (parented on the registry context), linked to
	// the request.
	if i.registry != nil && !IsInlineFault(decision.Fault) {
		return i.startRegistered(ctx, i.registry.ctx, req, decision, faultType, effectiveMode)
	}

	// Direct path: inline faults (or no registry configured).
	return i.startDirect(ctx, req, decision, faultType, effectiveMode)
}

// StartTransient starts decision's fault for the Transient point, where the
// response is already complete: inline mode has nothing left to block, so
// every fault -- inline types included -- runs in Background mode and, when
// a registry is configured, through it (stoppable by rule name, deduplicated
// by StartPolicy) regardless of its type. The fault span is parented on
// ctx's trace context while the fault runs detached from ctx's cancellation
// (context.WithoutCancel): it outlives the request without losing the link
// to it; through the registry the span also links the request as its
// trigger, like any background fault's. An inline error fault here has no
// response to shape; it only leaves its span. It returns once the fault has
// started, and a fault's Start can block (the registry holds its lock
// meanwhile), so a caller that still has a response to finish runs it on its
// own goroutine, as IngressMiddleware does -- by then the request's span has
// usually ended, and the run id stamped on it is dropped.
func (i *Interceptor) StartTransient(ctx context.Context, req evaluator.Request, decision *evaluator.Decision) (CheckResult, error) {
	if decision == nil || decision.Fault == nil {
		return CheckResult{}, nil
	}
	if err := decision.Fault.Validate(); err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: invalid fault from evaluator: %w", err)
	}
	faultType := fmt.Sprintf("%T", decision.Fault)
	detached := context.WithoutCancel(ctx)
	if i.registry != nil {
		return i.startRegistered(detached, detached, req, decision, faultType, evaluator.Background)
	}
	return i.startDirect(detached, req, decision, faultType, evaluator.Background)
}

// faultSpanAttrs are the attributes every fault span starts with.
func faultSpanAttrs(req evaluator.Request, decision *evaluator.Decision, faultType, runID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(trace.AttrFaultType, faultType),
		attribute.String(trace.AttrFaultInjectionPoint, req.Point.String()),
		attribute.String(trace.AttrFaultReason, decision.Reason),
		attribute.String(trace.AttrFaultID, runID),
	}
}

// emitDecision reports a decision applied to req as an atropos.decision event
// (trace.EmitDecision). ctx is the context of the request it was applied to,
// so the event carries that request's trace and span -- not those of the
// fault or cache-box span the interceptor starts for it (Core.Arm's context
// carries none). faultID is the fault run's id ("" for a cache-box action).
// The recent trace ids are the ring's newest ringLinks distinct traces,
// newest first. The decision logger is resolved once per decision, and
// nothing is assembled when the registered logger provider would drop the
// event.
func (i *Interceptor) emitDecision(ctx context.Context, req evaluator.Request, decision *evaluator.Decision, action, faultID string) {
	logger := trace.DecisionLogger()
	if !trace.DecisionEnabledOn(ctx, logger) {
		return
	}
	experimentID, phaseID := decisionExperiment(ctx, decision)
	trace.EmitDecisionTo(ctx, logger, trace.Decision{
		RuleName:       decision.Name,
		Reason:         decision.Reason,
		FaultID:        faultID,
		InjectionPoint: req.Point.String(),
		ExperimentID:   experimentID,
		PhaseID:        phaseID,
		Action:         action,
		RecentTraceIDs: i.recent.RecentTraceIDs(ringLinks),
	})
}

// decisionExperiment names the experiment and phase a decision event is
// attributed to -- the one place the event resolves them: the matched
// rule's CacheBoxContext, when the decision carries one; otherwise the
// trace.BaggageExperimentID and trace.BaggagePhaseID members of the
// request's baggage (set by an upstream service's cache-box decision), or
// none.
func decisionExperiment(ctx context.Context, decision *evaluator.Decision) (experimentID, phaseID string) {
	if c := decision.CacheBoxContext; c != nil {
		return c.ExperimentID, c.PhaseID
	}
	return trace.ExperimentFromContext(ctx)
}

// startDirect is the inline-fault path. It mints the run's id, creates a
// span (a child of the request's), wires up events, starts the fault, hooks
// the result callback, and, once the fault has started, stamps the run id on
// the request's span -- unless the call is an arm (ArmedContext), which has
// no request span -- and emits the fault.start decision event.
func (i *Interceptor) startDirect(ctx context.Context, req evaluator.Request, decision *evaluator.Decision, faultType string, mode evaluator.Mode) (CheckResult, error) {
	runID := NewRunID()
	armed := isArmed(ctx)
	reqSpan := oteltrace.SpanFromContext(ctx)
	faultCtx, span := i.tracer.Start(ctx, trace.SpanFaultInject, faultSpanAttrs(req, decision, faultType, runID)...)
	for k, v := range req.Labels {
		span.SetAttributes(attribute.String(k, v))
	}
	if ea, ok := decision.Fault.(fault.EventAware); ok {
		ea.SetEventEmitter(func(name string, attrs ...attribute.KeyValue) {
			span.AddEvent(name, attrs...)
		})
	}

	handle, err := decision.Fault.Start(faultCtx)
	if err != nil {
		span.EndWithError(err)
		return CheckResult{}, fmt.Errorf("interceptor: fault start failed: %w", err)
	}
	if !armed {
		reqSpan.SetAttributes(attribute.String(trace.AttrFaultID, runID))
	}
	i.emitDecision(ctx, req, decision, trace.ActionFaultStart, runID)
	if i.onInject != nil {
		i.onInject(ctx, faultType, req.Point.String())
	}
	handle.SetOnResult(func(r fault.Result) {
		span.RecordResult(r)
	})

	d := *decision
	d.Mode = mode
	return CheckResult{Handle: handle, Decision: &d, RunID: runID}, nil
}

// startRegistered routes faults through the registry for deduplication and
// detachment: the fault runs under the registry context so it can outlive
// the triggering request and be stopped by key. ctx is the triggering
// request's context. The fault span is parented on spanParent -- the
// registry context for StartFault (a root span, as before) or the request
// context stripped of cancellation for StartTransient (a child of the
// request's server span) -- and links the request (trigger) and the
// requests this interceptor saw before it (recent). Started or joined, the
// request's span carries the run id; a joiner's span also links the run's
// fault span, and the run counts the join. A request is any call that
// ArmedContext did not mark: one without a span context (a gRPC call without
// otelgrpc) is counted too, it just has no span to stamp or link. An arm
// (Core.Arm, on every rule delivery) is no request, whatever its context
// carries: it has no trigger and no request span, and its join is reported
// to the caller but not counted. Once StartRun has returned (outside the
// registry lock, so a slow log pipeline never serializes fault starts), a
// start emits the fault.start decision event and a request's join the
// fault.join one; an arm's join emits none.
func (i *Interceptor) startRegistered(ctx, spanParent context.Context, req evaluator.Request, decision *evaluator.Decision, faultType string, mode evaluator.Mode) (CheckResult, error) {
	key := decision.Name
	if key == "" {
		key = faultType
	}
	armed := isArmed(ctx)
	var trigger oteltrace.SpanContext // an arm has none
	if !armed {
		trigger = oteltrace.SpanContextFromContext(ctx)
	}

	run, joined, err := i.registry.StartRun(key, decision.StartPolicy, func(regCtx context.Context, runID string) (*fault.Handle, trace.Span, error) {
		_, span := i.tracer.StartLinked(spanParent, trace.SpanFaultInject, i.faultLinks(trigger),
			faultSpanAttrs(req, decision, faultType, runID)...)
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
			return nil, nil, err
		}
		h.SetOnResult(func(r fault.Result) {
			span.RecordResult(r)
		})
		return h, span, nil
	})
	if err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: fault start failed: %w", err)
	}

	reqSpan := oteltrace.SpanFromContext(ctx)
	d := *decision
	d.Mode = mode
	if joined {
		// Only a request joins: an eager re-arm meeting its own running
		// fault must not inflate atropos.fault.joined. Every request counts,
		// sampled, unsampled or without a span context.
		if !armed {
			reqSpan.SetAttributes(attribute.String(trace.AttrFaultID, run.ID))
			if run.Span != nil {
				if sc := run.Span.SpanContext(); linkable(sc) {
					reqSpan.AddLink(oteltrace.Link{SpanContext: sc, Attributes: []attribute.KeyValue{
						attribute.String(trace.AttrLinkKind, trace.LinkKindFault),
					}})
				}
			}
			run.Join()
			i.emitDecision(ctx, req, decision, trace.ActionFaultJoin, run.ID)
		}
		return CheckResult{Decision: &d, RunID: run.ID, Joined: true}, nil
	}
	if !armed {
		reqSpan.SetAttributes(attribute.String(trace.AttrFaultID, run.ID))
	}
	i.emitDecision(ctx, req, decision, trace.ActionFaultStart, run.ID)
	if i.onInject != nil {
		i.onInject(ctx, faultType, req.Point.String())
	}
	return CheckResult{Handle: run.Handle, Decision: &d, RunID: run.ID}, nil
}

// faultLinks are a background fault span's links: the triggering request
// (kind trigger), then up to ringLinks of the requests this interceptor saw,
// newest first, without the trigger (kind recent). A trigger that is not
// linkable is left out: an arm has none (the zero span context), a request
// may carry none, and the SDK would still record an invalid span context as
// a link because the link carries an attribute; an unsampled request's trace
// is never stored.
func (i *Interceptor) faultLinks(trigger oteltrace.SpanContext) []oteltrace.Link {
	recent := i.recent.Links(ringLinks, trigger.SpanID())
	if !linkable(trigger) {
		return recent
	}
	return append([]oteltrace.Link{{SpanContext: trigger, Attributes: []attribute.KeyValue{
		attribute.String(trace.AttrLinkKind, trace.LinkKindTrigger),
	}}}, recent...)
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
