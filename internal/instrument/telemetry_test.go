package instrument

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
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

// unsetEnv removes key for the rest of the test. t.Setenv alone can only give a
// variable a value, and the SDK reads a set-but-empty OTEL_TRACES_SAMPLER as an
// unsupported sampler rather than as none; t.Setenv's cleanup restores the
// original.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
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

func TestInstanceIdentity(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if id, pod := instanceIdentity(env(map[string]string{"ATROPOS_POD_NAME": "pod-7", "HOSTNAME": "host-1"})); id != "pod-7" || pod != "pod-7" {
		t.Fatalf("got %q/%q, want pod-7/pod-7", id, pod)
	}
	if id, pod := instanceIdentity(env(map[string]string{"HOSTNAME": "host-1"})); id != "host-1" || pod != "host-1" {
		t.Fatalf("got %q/%q, want host-1/host-1", id, pod)
	}
	host, _ := os.Hostname()
	if id, pod := instanceIdentity(env(map[string]string{})); id != host || pod != "" {
		t.Fatalf("got %q/%q, want %q/\"\"", id, pod, host)
	}
}

// A blank variable is unset, and whitespace around a name is not part of it.
func TestInstanceIdentity_TrimsAndSkipsBlank(t *testing.T) {
	env := map[string]string{"ATROPOS_POD_NAME": " \t", "HOSTNAME": " host-1\n"}
	if id, pod := instanceIdentity(func(k string) string { return env[k] }); id != "host-1" || pod != "host-1" {
		t.Fatalf("got %q/%q, want host-1/host-1", id, pod)
	}
}

// resourceOf starts and ends one sampled span on the global provider and
// returns its resource; initTelemetry must have installed an SDK provider.
func resourceOf(t *testing.T) *resource.Resource {
	t.Helper()
	_, span := otel.Tracer("resource-test").Start(context.Background(), "probe")
	defer span.End()
	ro, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		t.Fatalf("span %T is not an SDK span (unsampled or no SDK provider)", span)
	}
	return ro.Resource()
}

func TestInitTelemetry_ResourceCarriesInstanceAndHost(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("ATROPOS_POD_NAME", "pod-7")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", host: HostSidecar})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	set := resourceOf(t).Set()
	for k, want := range map[attribute.Key]string{"service.instance.id": "pod-7", "k8s.pod.name": "pod-7", "atropos.host": "sidecar", "service.name": "svc"} {
		if v, ok := set.Value(k); !ok || v.AsString() != want {
			t.Fatalf("%s = %v (present=%v), want %q", k, v, ok, want)
		}
	}
}

// The service name and environment the caller leaves empty get their defaults.
func TestInitTelemetry_ResourceDefaults(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	set := resourceOf(t).Set()
	for k, want := range map[attribute.Key]string{"service.name": "unknown", "deployment.environment.name": "development"} {
		if v, ok := set.Value(k); !ok || v.AsString() != want {
			t.Fatalf("%s = %v (present=%v), want %q", k, v, ok, want)
		}
	}
}

func TestInitTelemetry_NoPodNameOmitsK8sAttribute(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("ATROPOS_POD_NAME", "")
	t.Setenv("HOSTNAME", "")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	set := resourceOf(t).Set()
	if _, ok := set.Value("k8s.pod.name"); ok {
		t.Fatal("k8s.pod.name must be omitted without ATROPOS_POD_NAME/HOSTNAME")
	}
	if _, ok := set.Value("atropos.host"); ok {
		t.Fatal("atropos.host must be omitted when the host is unset")
	}
}

func TestInitTelemetry_SamplerFromEnv(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	_, span := otel.Tracer("sampler-test").Start(context.Background(), "probe")
	span.End()
	if span.SpanContext().IsSampled() {
		t.Fatal("OTEL_TRACES_SAMPLER=always_off must be honoured")
	}
}

func TestInitTelemetry_DefaultSamplerSamples(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	_, span := otel.Tracer("sampler-test").Start(context.Background(), "probe")
	span.End()
	if !span.SpanContext().IsSampled() {
		t.Fatal("default sampler must be parent-based always-on")
	}
}

// A root span is sampled under AlwaysSample and under ParentBased(AlwaysSample)
// alike; a remote parent that was not sampled tells them apart.
func TestInitTelemetry_DefaultSamplerFollowsUnsampledParent(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	parent := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{1},
		SpanID:  oteltrace.SpanID{1},
		Remote:  true, // no TraceFlags: the caller did not sample this trace
	})
	ctx := oteltrace.ContextWithRemoteSpanContext(context.Background(), parent)
	_, span := otel.Tracer("sampler-test").Start(ctx, "probe")
	span.End()
	if span.SpanContext().IsSampled() {
		t.Fatal("with no OTEL_TRACES_SAMPLER the default is parent-based: an unsampled remote parent must win")
	}
}

// telemetryConfig.sampler is the tests-only seam; production reads the
// environment. The seam must still win when both are present.
func TestInitTelemetry_SamplerConfigOverridesEnv(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	shutdown, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", sampler: sdktrace.NeverSample()})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	_, span := otel.Tracer("sampler-test").Start(context.Background(), "probe")
	span.End()
	if span.SpanContext().IsSampled() {
		t.Fatal("telemetryConfig.sampler must win over OTEL_TRACES_SAMPLER")
	}
}
