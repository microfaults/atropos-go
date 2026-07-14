// @title       Atropos SDK Admin API
// @version     1.0
// @description Admin and health handlers exposed by the atropos-go SDK.
// @description Serve mounts these on the returned handler at the documented
// @description paths; the generated spec documents that control surface.
//
// Package atropos embeds the faults-lab measurement instrument in a Go
// service: OpenTelemetry instrumentation, rule-driven fault injection, the
// record/replay cache-box, and the manteion control-plane connection.
//
// Serve is the embed path — see its doc for the full wiring. EgressTransport
// wraps a service's outbound HTTP client so egress rules (faults and
// cache-box record/replay) apply. Everything else exported here is a wire
// type shared with manteion.
package atropos

import (
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

var (
	// defaultInterceptor is the process-wide policy dispatcher. It is an
	// atomic pointer so the middleware can resolve it per request/round-trip:
	// an EgressTransport built before Serve runs still routes through the
	// evaluator Serve installs — construction order cannot strand a client
	// on the tracing-only baseline.
	defaultInterceptor atomic.Pointer[interceptor.Interceptor]

	// defaultRegistry holds every running background fault so rule/slot
	// removal can stop it (see FaultRegistry). It is created ONCE in init()
	// and shared across every configure call: the registry is rule-name-keyed
	// and evaluator-agnostic, so reconfiguring the evaluator/cache-box must
	// not orphan the faults the live middleware is still routing through it
	// (A1).
	defaultRegistry *interceptor.FaultRegistry
)

func init() {
	defaultRegistry = interceptor.NewFaultRegistry()
	defaultInterceptor.Store(interceptor.New(nil, trace.NewOTelTracer(),
		interceptor.WithRegistry(defaultRegistry),
		interceptor.WithInjectionHook(recordFaultInjection)))
}

// currentInterceptor returns the live package interceptor.
func currentInterceptor() *interceptor.Interceptor {
	return defaultInterceptor.Load()
}

// Interceptor ties the evaluator, fault execution, and OTel together. It is
// exported only as the currency between DefaultInterceptor and the grpc
// subpackage's interceptor constructors.
type Interceptor = interceptor.Interceptor

// DefaultInterceptor returns the package-level interceptor installed by
// Serve (or the tracing-only baseline before Serve runs). It exists for the
// grpc subpackage's constructors, which take one explicitly:
//
//	atroposgrpc.UnaryServerInterceptor(atropos.DefaultInterceptor())
func DefaultInterceptor() *Interceptor {
	return currentInterceptor()
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

// configure swaps the package-level interceptor to one built around eval
// and cb. Called once by Serve; the fault registry is process-lifetime and
// deliberately NOT swapped or closed here — the live middleware still
// routes background faults through it, so replacing it would orphan
// running faults (A1).
func configure(eval evaluator.Evaluator, cb *cachebox.CacheBox) {
	opts := []interceptor.Option{
		interceptor.WithInjectionHook(recordFaultInjection),
		interceptor.WithRegistry(defaultRegistry),
	}
	if cb != nil {
		opts = append(opts, interceptor.WithCacheBox(cb))
	}
	defaultInterceptor.Store(interceptor.New(eval, trace.NewOTelTracer(), opts...))
}
