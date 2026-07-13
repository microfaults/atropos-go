// @title       Atropos SDK Admin API
// @version     1.0
// @description Admin and health handlers exposed by the atropos-go SDK.
// @description Host services mount these handlers on an internal/admin mux.
// @description The generated spec documents the recommended mount paths used
// @description by README examples.
//
// Package atropos provides OpenTelemetry instrumentation and fault
// injection for Go services. See README.md for architecture and usage.
package atropos

import (
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

var (
	defaultInterceptor *Interceptor

	// defaultRegistry holds every running background fault so rule/slot
	// removal can stop it (see FaultRegistry). It is created ONCE in init()
	// and shared across every Configure call: the registry is rule-name-keyed
	// and evaluator-agnostic, so reconfiguring the evaluator/cache-box must
	// not orphan the faults the live middleware is still routing through it
	// (A1). Like defaultInterceptor, it is unsynchronized package state --
	// Configure is init-time API.
	defaultRegistry *interceptor.FaultRegistry

	// hostConfigured records whether a host service has called Configure. Once
	// set, the zero-arg FaultAdminHandler refuses to lazily reconfigure the
	// SDK onto the demo evaluator -- doing so would drop the host's evaluator
	// and cache-box and rebuild the interceptor mid-experiment (A1).
	hostConfigured atomic.Bool
)

func init() {
	defaultRegistry = interceptor.NewFaultRegistry()
	defaultInterceptor = interceptor.New(nil, trace.NewOTelTracer(),
		interceptor.WithRegistry(defaultRegistry),
		interceptor.WithInjectionHook(recordFaultInjection))
}

// stopBackgroundFaults cancels the running background faults started under
// the given rule/slot keys (Decision.Name; the registry's key). Called
// wherever a rule or fault slot is removed -- rule removal is the
// platform's fault-stop signal, and a background fault that outlives its
// phase contaminates the next phase's measurement (X1).
func stopBackgroundFaults(keys ...string) {
	reg := defaultRegistry
	for _, k := range keys {
		reg.Stop(k)
	}
}

// ConfigureOption mutates the package-level interceptor configuration when
// passed to Configure.
type ConfigureOption func(*configureState)

type configureState struct {
	eval     Evaluator
	cacheBox *cachebox.CacheBox
}

// WithEvaluator sets the rule engine used on every injection-point check.
// A nil evaluator is equivalent to "no evaluator" (tracing-only, no faults).
func WithEvaluator(e Evaluator) ConfigureOption {
	return func(s *configureState) { s.eval = e }
}

// WithCacheBoxCoordinator attaches a cache-box coordinator to the default
// interceptor. Pass nil to disable cache-box.
func WithCacheBoxCoordinator(cb *cachebox.CacheBox) ConfigureOption {
	return func(s *configureState) { s.cacheBox = cb }
}

// Configure replaces the default package-level interceptor with a newly
// constructed one built from the supplied options. Calling Configure with
// no options resets the interceptor to "no evaluator, no cache-box"
// (tracing only) -- this is the migration path for legacy callers that
// previously wrote `atropos.Configure(nil)`.
//
// Legacy positional call sites of the form `atropos.Configure(eval)` must
// migrate to `atropos.Configure(atropos.WithEvaluator(eval))`.
func Configure(opts ...ConfigureOption) {
	s := configureState{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&s)
	}
	interceptOpts := []interceptor.Option{interceptor.WithInjectionHook(recordFaultInjection)}
	if s.cacheBox != nil {
		interceptOpts = append(interceptOpts, interceptor.WithCacheBox(s.cacheBox))
	}

	// The fault registry is process-lifetime and shared across Configure calls
	// (see defaultRegistry). It is deliberately NOT swapped or closed here: the
	// live middleware captured the previous interceptor at construction and
	// still routes background faults through this registry, so replacing it
	// would orphan running faults and make stopBackgroundFaults consult a
	// registry the middleware never uses (A1). Reconfigure swaps the evaluator
	// and cache-box only.
	interceptOpts = append(interceptOpts, interceptor.WithRegistry(defaultRegistry))

	defaultInterceptor = interceptor.New(s.eval, trace.NewOTelTracer(), interceptOpts...)
	hostConfigured.Store(true)
}

// DefaultInterceptor returns the package-level interceptor.
func DefaultInterceptor() *Interceptor {
	return defaultInterceptor
}
