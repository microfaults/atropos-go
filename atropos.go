// @title       Atropos SDK Admin API
// @version     1.0
// @description Admin and health handlers exposed by the atropos-go SDK.
// @description Serve mounts these on the returned handler at the documented
// @description paths; the generated spec documents that control surface.
// @description The manteion-protocol operations document the desired-state
// @description endpoints the SDK calls (the poll and the status report);
// @description manteion serves those, Serve does not mount them.
//
// Package atropos embeds the faults-lab measurement instrument in a Go
// service: OpenTelemetry instrumentation, rule-driven fault injection, the
// record/replay cache-box, and the manteion control-plane connection.
//
// Serve is the embed path — see its doc for the full wiring. EgressTransport
// wraps a service's outbound HTTP client so egress rules (faults and
// cache-box record/replay) apply. Everything else exported here is a wire
// type shared with manteion (see types.go); the instrument itself lives in
// internal/instrument and is shared with the atroposd sidecar host.
package atropos

import (
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
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
	// removal can stop it. It is created ONCE in init() and handed to the
	// core Serve builds: the registry is rule-name-keyed and
	// evaluator-agnostic, so reconfiguring the evaluator/cache-box must not
	// orphan the faults the live middleware is still routing through it
	// (A1).
	defaultRegistry *interceptor.FaultRegistry
)

func init() {
	defaultRegistry = interceptor.NewFaultRegistry()
	defaultInterceptor.Store(interceptor.New(nil, trace.NewOTelTracer(),
		interceptor.WithRegistry(defaultRegistry),
		interceptor.WithInjectionHook(metrics.FaultInjectionHook(""))))
}

// currentInterceptor returns the live package interceptor.
func currentInterceptor() *interceptor.Interceptor {
	return defaultInterceptor.Load()
}

// DefaultInterceptor returns the package-level interceptor installed by
// Serve (or the tracing-only baseline before Serve runs). It exists for the
// grpc subpackage's constructors, which take one explicitly:
//
//	atroposgrpc.UnaryServerInterceptor(atropos.DefaultInterceptor())
func DefaultInterceptor() *Interceptor {
	return currentInterceptor()
}

// configure swaps the package-level interceptor to one built around eval
// and cb over the process-lifetime registry. Serve installs its core's
// interceptor directly; this remains for tests that pin a private
// evaluator. The fault registry is deliberately NOT swapped or closed here
// (A1).
func configure(eval evaluator.Evaluator, cb *cachebox.CacheBox) {
	opts := []interceptor.Option{
		interceptor.WithInjectionHook(metrics.FaultInjectionHook("")),
		interceptor.WithRegistry(defaultRegistry),
	}
	if cb != nil {
		opts = append(opts, interceptor.WithCacheBox(cb))
	}
	defaultInterceptor.Store(interceptor.New(eval, trace.NewOTelTracer(), opts...))
}
