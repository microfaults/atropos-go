package atropos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestIngressMiddleware_CreatesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	// No faults, just span creation.
	i := interceptor.New(nil, trace.NewOTelTracer())

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := IngressMiddleware(inner, "test-service", WithInterceptor(i))

	req := httptest.NewRequest("GET", "/api/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span from otelhttp")
	}

	// Should have an otelhttp span named "test-service".
	found := false
	for _, s := range spans {
		if s.Name == "test-service" {
			found = true
		}
	}
	if !found {
		names := make([]string, len(spans))
		for i, s := range spans {
			names[i] = s.Name
		}
		t.Fatalf("expected otelhttp span named 'test-service', got spans: %v", names)
	}
}

func TestIngressMiddleware_WithFault(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 30 * time.Millisecond},
			Reason: "test: middleware fault",
			Mode:   evaluator.Inline,
		},
	}
	i := interceptor.New(eval, trace.NewOTelTracer())

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := IngressMiddleware(inner, "test-service", WithInterceptor(i))

	req := httptest.NewRequest("POST", "/checkout", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected >= 20ms from fault, got %s", elapsed)
	}

	spans := exporter.GetSpans()
	// Should have otelhttp span + fault.inject span.
	if len(spans) < 2 {
		t.Fatalf("expected >= 2 spans, got %d", len(spans))
	}

	// Verify fault.inject span exists.
	foundFault := false
	for _, s := range spans {
		if s.Name == trace.SpanFaultInject {
			foundFault = true
		}
	}
	if !foundFault {
		t.Fatal("expected fault.inject span")
	}
}

func TestEgressTransport_CreatesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	i := interceptor.New(nil, trace.NewOTelTracer())

	// Create a test server to call.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := &http.Client{
		Transport: EgressTransport(http.DefaultTransport, WithInterceptor(i)),
	}

	resp, err := client.Get(ts.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span from otelhttp transport")
	}
}
