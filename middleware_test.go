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

// testEvaluator returns a fixed decision for every request.
type testEvaluator struct {
	decision *evaluator.Decision
}

func (e *testEvaluator) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return e.decision
}

// The server span takes otelhttp's semconv name: the method alone, or
// "METHOD /route" when a ServeMux in front of the middleware set the
// request's route pattern. It is no longer named after the service.
func TestIngressMiddleware_CreatesSpans(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	cases := []struct {
		name    string
		handler func(http.Handler) http.Handler
		want    string
	}{
		{"no route pattern", func(h http.Handler) http.Handler { return h }, "GET"},
		{"route pattern", func(h http.Handler) http.Handler {
			mux := http.NewServeMux()
			mux.Handle("GET /api/{name}", h)
			return mux
		}, "GET /api/{name}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithSyncer(exporter),
				sdktrace.WithSampler(sdktrace.AlwaysSample()),
			)
			otel.SetTracerProvider(tp)
			defer tp.Shutdown(context.Background())

			// No faults, just span creation.
			i := interceptor.New(nil, trace.NewOTelTracer())
			handler := tc.handler(ingressMiddleware(inner, "test-service", i))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/health", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", rec.Code)
			}

			spans := exporter.GetSpans()
			names := make([]string, len(spans))
			for k, s := range spans {
				names[k] = s.Name
			}
			if len(spans) != 1 || spans[0].Name != tc.want {
				t.Fatalf("server spans = %v, want one named %q", names, tc.want)
			}
		})
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

	handler := ingressMiddleware(inner, "test-service", i)

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
		Transport: egressTransport(http.DefaultTransport, func() *interceptor.Interceptor { return i }),
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
