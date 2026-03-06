package trace

import (
	"context"

	fault "atropos-go/internal/fault"
)

type noopTracer struct{}
type noopSpan struct{}

// Noop returns a Tracer that does nothing. Default when OTel is not configured.
func Noop() Tracer { return &noopTracer{} }

func (t *noopTracer) Start(ctx context.Context, _, _, _ string) (context.Context, Span) {
	return ctx, &noopSpan{}
}

func (s *noopSpan) RecordResult(_ fault.Result) {}
func (s *noopSpan) EndWithError(_ error)        {}
func (s *noopSpan) End()                        {}
