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

// pinExporterEnv blanks the variables the exporter-env shim reads, so a test
// does not depend on the ambient OTLP environment. The shim writes the process
// environment with os.Setenv; t.Setenv restores each variable when the test
// ends, so nothing the shim maps leaks into later tests.
func pinExporterEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"COLLECTOR_SERVICE_ADDR",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
	} {
		t.Setenv(key, "")
	}
}

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

// A bare host:port endpoint, the form the old exporter accepted, still means a
// gRPC collector.
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

// Once mapped, the endpoint is a URL, so the shim has nothing left to do.
func TestExporterEnvFallback_SecondCallChangesNothing(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("COLLECTOR_SERVICE_ADDR", "otel-collector:4317")
	if first := applyExporterEnvFallback(); len(first) != 2 {
		t.Fatalf("first call applied = %v, want endpoint+protocol", first)
	}
	if second := applyExporterEnvFallback(); len(second) != 0 {
		t.Fatalf("second call applied = %v, want an empty map", second)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != "http://otel-collector:4317" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); got != "grpc" {
		t.Fatalf("protocol = %q", got)
	}
}

// A URL endpoint is the exporters' own business: a protocol the environment
// leaves empty stays empty, so autoexport's http/protobuf default applies.
func TestExporterEnvFallback_URLEndpointLeavesEmptyProtocolEmpty(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("COLLECTOR_SERVICE_ADDR", "")
	if applied := applyExporterEnvFallback(); len(applied) != 0 {
		t.Fatalf("applied = %v, want nothing", applied)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != "http://collector:4318" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); got != "" {
		t.Fatalf("protocol = %q, want it left empty", got)
	}
}

// A protocol the environment already names wins over the grpc default, even
// while the endpoint still needs its scheme.
func TestExporterEnvFallback_SchemelessEndpointKeepsNamedProtocol(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("COLLECTOR_SERVICE_ADDR", "")
	applied := applyExporterEnvFallback()
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != "http://otel-collector:4318" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); got != "http/protobuf" {
		t.Fatalf("protocol = %q, want the named protocol kept", got)
	}
	if len(applied) != 1 {
		t.Fatalf("applied = %v, want only the endpoint", applied)
	}
}

// Whitespace around a value does not change what the shim decides: a blank
// variable counts as unset and a padded schemeless endpoint becomes a clean URL.
func TestExporterEnvFallback_TrimsWhitespace(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		endpoint, protocol, addr string
		wantEndpoint             string
	}{
		{"padded schemeless endpoint", "  otel-collector:4317 ", "", "", "http://otel-collector:4317"},
		{"blank endpoint falls back to the bed address", "  ", "", " otel-collector:4317\n", "http://otel-collector:4317"},
		{"blank protocol counts as unset", "otel-collector:4317", " \t", "", "http://otel-collector:4317"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", tc.protocol)
			t.Setenv("COLLECTOR_SERVICE_ADDR", tc.addr)
			applyExporterEnvFallback()
			if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != tc.wantEndpoint {
				t.Fatalf("endpoint = %q, want %q", got, tc.wantEndpoint)
			}
			if got := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); got != "grpc" {
				t.Fatalf("protocol = %q, want grpc", got)
			}
		})
	}
}

func TestNewSpanExporter_None(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	exp, err := newSpanExporter(context.Background())
	if err != nil || !autoexport.IsNoneSpanExporter(exp) {
		t.Fatalf("exp=%T err=%v, want the none exporter", exp, err)
	}
}

func TestNewSpanExporter_Console(t *testing.T) {
	pinExporterEnv(t)
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
	pinExporterEnv(t)
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
