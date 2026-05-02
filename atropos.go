// Package atropos provides OpenTelemetry instrumentation and fault
// injection for Go services. See README.md for architecture and usage.
package atropos

import (
	"github.com/microfaults/atropos-go/internal/cachebox"
	"github.com/microfaults/atropos-go/internal/interceptor"
	"github.com/microfaults/atropos-go/internal/trace"
)

var defaultInterceptor *Interceptor

func init() {
	defaultInterceptor = interceptor.New(nil, trace.NewOTelTracer())
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
	var interceptOpts []interceptor.Option
	if s.cacheBox != nil {
		interceptOpts = append(interceptOpts, interceptor.WithCacheBox(s.cacheBox))
	}
	defaultInterceptor = interceptor.New(s.eval, trace.NewOTelTracer(), interceptOpts...)
}

// DefaultInterceptor returns the package-level interceptor.
func DefaultInterceptor() *Interceptor {
	return defaultInterceptor
}
