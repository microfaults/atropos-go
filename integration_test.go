package atropos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestIntegration_IngressMiddleware_WithFault(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	Configure(WithEvaluator(&integrationEval{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 50 * time.Millisecond},
			Reason: "integration test",
			Mode:   evaluator.Inline,
		},
	}))
	defer Configure()

	handler := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), "test-service")

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected >= 40ms from inline fault, got %s", elapsed)
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected spans to be recorded")
	}

	var foundFaultSpan bool
	for _, s := range spans {
		if s.Name == "atropos.fault.inject" {
			foundFaultSpan = true
		}
	}
	if !foundFaultSpan {
		names := make([]string, len(spans))
		for i, s := range spans {
			names[i] = s.Name
		}
		t.Fatalf("expected 'atropos.fault.inject' span, got: %v", names)
	}
}

func TestIntegration_SpanWithFault_ProducesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	Configure(WithEvaluator(&integrationEval{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 20 * time.Millisecond},
			Reason: "span-with-fault test",
			Mode:   evaluator.Inline,
		},
	}))
	defer Configure()

	ctx, span, cr, err := SpanWithFault(context.Background(), "checkout", map[string]string{"user": "test"})
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx

	if cr.Handle != nil {
		<-cr.Handle.Done()
	}
	span.End()

	spans := exporter.GetSpans()
	if len(spans) < 2 {
		t.Fatalf("expected >= 2 spans (hook + fault), got %d", len(spans))
	}
}

func TestIntegration_NoFault_StillCreatesSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	Configure()

	ctx, span := Span(context.Background(), "my-operation")
	_ = ctx
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span for always-on tracing")
	}
}

type integrationEval struct {
	decision *evaluator.Decision
}

func (e *integrationEval) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return e.decision
}
