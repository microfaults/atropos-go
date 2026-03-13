package trace

import (
	"context"

	fault "github.com/microfaults/atropos-go/internal/fault"
	"go.opentelemetry.io/otel/attribute"
)

type noopTracer struct{}
type noopSpan struct{}

// Noop returns a Tracer that does nothing. Default when OTel is not configured.
func Noop() Tracer { return &noopTracer{} }

func (t *noopTracer) Start(ctx context.Context, _ string, _ ...attribute.KeyValue) (context.Context, Span) {
	return ctx, &noopSpan{}
}

func (s *noopSpan) SetAttributes(_ ...attribute.KeyValue) {}
func (s *noopSpan) AddEvent(_ string, _ ...attribute.KeyValue) {}
func (s *noopSpan) RecordResult(_ fault.Result)                {}
func (s *noopSpan) EndWithError(_ error)                       {}
func (s *noopSpan) End()                                       {}
