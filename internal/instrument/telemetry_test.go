package instrument

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInitTelemetry_SetsGlobalTracerProvider(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
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

func TestInitTelemetry_BYOProviderNoShutdownPanic(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	// Should not panic.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("BYO shutdown should be no-op, got: %v", err)
	}
}

func TestInitTelemetry_ProducesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
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

func TestInitTelemetry_PropagatorHasTraceContextAndBaggage(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
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

func TestExporterEnvFallback_CollectorServiceAddr(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("COLLECTOR_SERVICE_ADDR", "otel-collector:4317")
	applied := applyExporterEnvFallback()
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != "http://otel-collector:4317" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); got != "grpc" {
		t.Fatalf("protocol = %q", got)
	}
	if len(applied) != 2 {
		t.Fatalf("applied = %v, want endpoint+protocol", applied)
	}
}

// Review Focus 1: the bare host:port form the old exporter accepted.
func TestExporterEnvFallback_SchemelessEndpointKeepsGRPC(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("COLLECTOR_SERVICE_ADDR", "")
	applyExporterEnvFallback()
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != "http://otel-collector:4317" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); got != "grpc" {
		t.Fatalf("protocol = %q", got)
	}
}

func TestExporterEnvFallback_ExplicitEndpointUntouched(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("COLLECTOR_SERVICE_ADDR", "ignored:4317")
	if applied := applyExporterEnvFallback(); len(applied) != 0 {
		t.Fatalf("applied = %v, want nothing", applied)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != "https://collector:4318" {
		t.Fatalf("endpoint = %q", got)
	}
}

func TestNewSpanExporter_None(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	exp, err := newSpanExporter(context.Background())
	if err != nil || !autoexport.IsNoneSpanExporter(exp) {
		t.Fatalf("exp=%T err=%v, want the none exporter", exp, err)
	}
}

func TestNewSpanExporter_Console(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	exp, err := newSpanExporter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := exp.(*stdouttrace.Exporter); !ok {
		t.Fatalf("exp=%T, want *stdouttrace.Exporter", exp)
	}
}

func TestInitTelemetry_NoBYO_UsesEnvExporter(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("global provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
}
