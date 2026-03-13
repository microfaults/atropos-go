// Package atropos provides OpenTelemetry instrumentation and fault
// injection for Go services. See README.md for architecture and usage.
package atropos

import (
	"github.com/microfaults/atropos-go/internal/interceptor"
	"github.com/microfaults/atropos-go/internal/trace"
)

var defaultInterceptor *Interceptor

func init() {
	defaultInterceptor = interceptor.New(nil, trace.NewOTelTracer())
}

// Configure swaps the evaluator on the default interceptor.
// Pass nil for tracing only (no faults).
func Configure(eval Evaluator) {
	defaultInterceptor = interceptor.New(eval, trace.NewOTelTracer())
}

// DefaultInterceptor returns the package-level interceptor.
func DefaultInterceptor() *Interceptor {
	return defaultInterceptor
}
