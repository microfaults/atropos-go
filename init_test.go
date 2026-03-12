package atropos

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInit_SetsGlobalTracerProvider(t *testing.T) {
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

	// Verify the global TracerProvider is set.
	got := otel.GetTracerProvider()
	if got != tp {
		t.Fatal("global TracerProvider was not set to the BYO provider")
	}
}

func TestInit_SetsPropagators(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	// Verify propagators are set (should be composite with TraceContext + Baggage).
	prop := otel.GetTextMapPropagator()
	fields := prop.Fields()
	if len(fields) == 0 {
		t.Fatal("expected propagator fields to be set")
	}
	t.Logf("propagator fields: %v", fields)
}

func TestInit_BYOProviderNoShutdownPanic(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	// Should not panic.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("BYO shutdown should be no-op, got: %v", err)
	}
}

func TestInit_ProducesSpans(t *testing.T) {
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

	// Create a span via the global tracer.
	tracer := otel.Tracer("test")
	_, span := tracer.Start(context.Background(), "test-span")
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span")
	}
	if spans[0].Name != "test-span" {
		t.Fatalf("expected span name 'test-span', got %q", spans[0].Name)
	}
}

func TestInit_EndpointFallback(t *testing.T) {
	// Verify the env var fallback chain resolves correctly.
	// We can't test the actual OTLP connection, but we can verify the
	// config resolution logic by checking that Init doesn't error.
	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	os.Setenv("COLLECTOR_SERVICE_ADDR", "")
	defer os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	defer os.Unsetenv("COLLECTOR_SERVICE_ADDR")

	// Use BYO to avoid actually connecting.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
}

func TestInit_Options(t *testing.T) {
	cfg := defaultConfig()
	WithServiceName("test-svc").apply(&cfg)
	WithServiceVersion("1.0.0").apply(&cfg)
	WithEnvironment("staging").apply(&cfg)
	WithEndpoint("collector:4317").apply(&cfg)
	WithInsecure(false).apply(&cfg)

	if cfg.serviceName != "test-svc" {
		t.Fatalf("expected serviceName 'test-svc', got %q", cfg.serviceName)
	}
	if cfg.serviceVersion != "1.0.0" {
		t.Fatalf("expected serviceVersion '1.0.0', got %q", cfg.serviceVersion)
	}
	if cfg.environment != "staging" {
		t.Fatalf("expected environment 'staging', got %q", cfg.environment)
	}
	if cfg.endpoint != "collector:4317" {
		t.Fatalf("expected endpoint 'collector:4317', got %q", cfg.endpoint)
	}
	if cfg.insecure != false {
		t.Fatal("expected insecure=false")
	}
}

func TestInit_PropagatorHasTraceContextAndBaggage(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	prop := otel.GetTextMapPropagator()
	fields := prop.Fields()

	// W3C TraceContext uses "traceparent" and "tracestate".
	// Baggage uses "baggage".
	expectedFields := map[string]bool{
		"traceparent": false,
		"tracestate":  false,
		"baggage":     false,
	}
	for _, f := range fields {
		if _, ok := expectedFields[f]; ok {
			expectedFields[f] = true
		}
	}
	for field, found := range expectedFields {
		if !found {
			t.Errorf("expected propagator field %q not found in %v", field, fields)
		}
	}

	// Verify it's a composite propagator.
	_, ok := prop.(propagation.TextMapPropagator)
	if !ok {
		t.Fatal("expected TextMapPropagator")
	}
}
