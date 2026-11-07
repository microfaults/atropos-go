package instrument

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/log/logtest"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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

// restoreOTelGlobals puts back, when the test ends, the global tracer and
// logger providers the test found, so the providers initTelemetry installs do
// not outlive the test that built them.
func restoreOTelGlobals(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevLP := global.GetLoggerProvider()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		if global.GetLoggerProvider() != prevLP {
			global.SetLoggerProvider(prevLP)
		}
	})
}

func TestInitTelemetry_SetsGlobalTracerProvider(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())

	// Verify the global TracerProvider is set.
	got := otel.GetTracerProvider()
	if got != tp {
		t.Fatal("global TracerProvider was not set to the BYO provider")
	}
}

func TestInitTelemetry_BYOProviderNoShutdownPanic(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	// Should not panic.
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("BYO shutdown should be no-op, got: %v", err)
	}
}

func TestInitTelemetry_ProducesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())

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

	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())

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
		{"padded protocol is re-set trimmed, URL endpoint too", "http://collector:4317", " grpc ", "", "http://collector:4317"},
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
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
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
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("ATROPOS_POD_NAME", "pod-7")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", host: HostSidecar})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	set := resourceOf(t).Set()
	for k, want := range map[attribute.Key]string{"service.instance.id": "pod-7", "k8s.pod.name": "pod-7", trace.AttrHost: "sidecar", "service.name": "svc"} {
		if v, ok := set.Value(k); !ok || v.AsString() != want {
			t.Fatalf("%s = %v (present=%v), want %q", k, v, ok, want)
		}
	}
}

// A service name the caller leaves empty becomes "unknown". An empty version
// or environment is not stamped, so whatever the default resource carries (from
// OTEL_RESOURCE_ATTRIBUTES) shows through, and with nothing there the key is
// absent.
func TestInitTelemetry_ResourceDefaults(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")
	tel, err := initTelemetry(context.Background(), telemetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	set := resourceOf(t).Set()
	if v, ok := set.Value("service.name"); !ok || v.AsString() != "unknown" {
		t.Fatalf("service.name = %v (present=%v), want \"unknown\"", v, ok)
	}
	def := resource.Default().Set()
	for _, k := range []attribute.Key{"service.version", "deployment.environment.name"} {
		want, wantOK := def.Value(k)
		if got, ok := set.Value(k); ok != wantOK || got.Emit() != want.Emit() {
			t.Fatalf("%s = %q (present=%v), want the default resource's %q (present=%v): an empty config value must not be stamped", k, got.Emit(), ok, want.Emit(), wantOK)
		}
	}
}

// A version and environment the caller sets are stamped over the environment's.
func TestInitTelemetry_ResourceStampsVersionAndEnvironmentWhenSet(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", serviceVersion: "1.2.3", environment: "staging"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	set := resourceOf(t).Set()
	for k, want := range map[attribute.Key]string{"service.version": "1.2.3", "deployment.environment.name": "staging"} {
		if v, ok := set.Value(k); !ok || v.AsString() != want {
			t.Fatalf("%s = %v (present=%v), want %q", k, v, ok, want)
		}
	}
}

func TestInitTelemetry_NoPodNameOmitsK8sAttribute(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("ATROPOS_POD_NAME", "")
	t.Setenv("HOSTNAME", "")
	// resource.Default() reads OTEL_RESOURCE_ATTRIBUTES once per process: pin it
	// blank in case this is the first read, and skip when an earlier read
	// already took a pod name from the ambient environment.
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	if _, ok := resource.Default().Set().Value("k8s.pod.name"); ok {
		t.Skip("the process's default resource already carries k8s.pod.name (ambient OTEL_RESOURCE_ATTRIBUTES)")
	}
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	set := resourceOf(t).Set()
	if _, ok := set.Value("k8s.pod.name"); ok {
		t.Fatal("k8s.pod.name must be omitted without ATROPOS_POD_NAME/HOSTNAME")
	}
	if _, ok := set.Value(trace.AttrHost); ok {
		t.Fatal("atropos.host must be omitted when the host is unset")
	}
}

func TestInitTelemetry_SamplerFromEnv(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	_, span := otel.Tracer("sampler-test").Start(context.Background(), "probe")
	span.End()
	if span.SpanContext().IsSampled() {
		t.Fatal("OTEL_TRACES_SAMPLER=always_off must be honoured")
	}
}

func TestInitTelemetry_DefaultSamplerSamples(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
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
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
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
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", sampler: sdktrace.NeverSample()})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	_, span := otel.Tracer("sampler-test").Start(context.Background(), "probe")
	span.End()
	if span.SpanContext().IsSampled() {
		t.Fatal("telemetryConfig.sampler must win over OTEL_TRACES_SAMPLER")
	}
}

// An explicit instance id (the registration identity the hosts resolve) is
// service.instance.id; k8s.pod.name stays the pod name.
func TestInitTelemetry_ExplicitInstanceIDKeepsPodName(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")
	t.Setenv("ATROPOS_POD_NAME", "pod-7")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", instanceID: "registered-9"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	set := resourceOf(t).Set()
	for k, want := range map[attribute.Key]string{"service.instance.id": "registered-9", "k8s.pod.name": "pod-7"} {
		if v, ok := set.Value(k); !ok || v.AsString() != want {
			t.Fatalf("%s = %v (present=%v), want %q", k, v, ok, want)
		}
	}
}

func TestInitTelemetry_BuildsLoggerProvider(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	if _, ok := tel.LoggerProvider.(*sdklog.LoggerProvider); !ok {
		t.Fatalf("LoggerProvider = %T, want *sdklog.LoggerProvider", tel.LoggerProvider)
	}
	if global.GetLoggerProvider() != tel.LoggerProvider {
		t.Fatal("the SDK logger provider must be registered globally")
	}
	if tel.Logger == nil {
		t.Fatal("default logger must be built")
	}
}

// Review Focus 4: BYO tracing means no exporter is built for logs either.
func TestInitTelemetry_BYOTracerProviderSkipsLogPipeline(t *testing.T) {
	restoreOTelGlobals(t)
	rec := logtest.NewRecorder()
	global.SetLoggerProvider(rec)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	if tel.LoggerProvider != otellog.LoggerProvider(rec) {
		t.Fatalf("LoggerProvider = %T, want the pre-existing global recorder", tel.LoggerProvider)
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInitTelemetry_BYOLoggerProviderIsRegistered(t *testing.T) {
	restoreOTelGlobals(t)
	rec := logtest.NewRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp, loggerProvider: rec})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	if global.GetLoggerProvider() != otellog.LoggerProvider(rec) {
		t.Fatal("a BYO LoggerProvider must become the global one")
	}
}

// Review Focus 4, pinned at the environment: an OTEL_LOGS_EXPORTER the SDK
// path rejects is never even read when the host brings its tracer provider.
func TestInitTelemetry_BYOTracerProviderReadsNoExporterEnv(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "no-such-exporter")
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	tel, err := initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})
	if err != nil {
		t.Fatalf("BYO init built a log exporter: %v", err)
	}
	_ = tel.Shutdown(context.Background())

	// The control: the SDK path does build one, and fails on that value,
	// after shutting down the span exporter it had already built.
	spans := useTestSpanExporter(t)
	if _, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc"}); err == nil || !strings.Contains(err.Error(), "log exporter") {
		t.Fatalf("SDK path err = %v, want the log exporter's error", err)
	}
	if n := spans.Shutdowns(); n != 1 {
		t.Fatalf("span exporter shut down %d times after the log exporter failed, want 1", n)
	}
}

// A LoggerProvider without a TracerProvider is ignored: a host that owns one
// signal owns both, so the SDK builds and registers its own provider.
func TestInitTelemetry_LoggerProviderAloneIsIgnored(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	rec := logtest.NewRecorder()
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", loggerProvider: rec})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	if _, ok := tel.LoggerProvider.(*sdklog.LoggerProvider); !ok {
		t.Fatalf("LoggerProvider = %T, want the SDK's own", tel.LoggerProvider)
	}
	if global.GetLoggerProvider() == otellog.LoggerProvider(rec) {
		t.Fatal("a LoggerProvider without a TracerProvider must not become the global one")
	}
}

// A BYO logger is handed back as is, on either path.
func TestInitTelemetry_BYOLoggerIsReturned(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	logger := slog.New(slog.DiscardHandler)
	for name, cfg := range map[string]telemetryConfig{
		"BYO tracer provider": {tracerProvider: sdktrace.NewTracerProvider(), logger: logger},
		"SDK providers":       {serviceName: "svc", logger: logger},
	} {
		tel, err := initTelemetry(context.Background(), cfg)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if tel.Logger != logger {
			t.Errorf("%s: Logger = %p, want the BYO logger %p", name, tel.Logger, logger)
		}
		_ = tel.Shutdown(context.Background())
	}
}

// The exported InitTelemetry threads every BYO field through to the result: the
// host's tracer provider and logger provider become the global ones, the
// result's LoggerProvider is the one passed, and its Logger the one passed.
func TestInitTelemetry_ExportedBYOProvidersAndLogger(t *testing.T) {
	restoreOTelGlobals(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	rec := logtest.NewRecorder()
	logger := slog.New(slog.DiscardHandler)
	tel, err := InitTelemetry(context.Background(), TelemetryConfig{TracerProvider: tp, LoggerProvider: rec, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	if tel.LoggerProvider != otellog.LoggerProvider(rec) {
		t.Fatalf("LoggerProvider = %T, want the one passed in", tel.LoggerProvider)
	}
	if tel.Logger != logger {
		t.Fatalf("Logger = %p, want the one passed in (%p)", tel.Logger, logger)
	}
	if global.GetLoggerProvider() != otellog.LoggerProvider(rec) {
		t.Fatal("the BYO LoggerProvider must be registered globally")
	}
	if otel.GetTracerProvider() != oteltrace.TracerProvider(tp) {
		t.Fatal("the BYO TracerProvider must be registered globally")
	}
}

func TestNewLogExporter_None(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	exp, err := newLogExporter(context.Background())
	if err != nil || !autoexport.IsNoneLogExporter(exp) {
		t.Fatalf("exp=%T err=%v, want the none exporter", exp, err)
	}
}

func TestNewLogExporter_Console(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("OTEL_LOGS_EXPORTER", "console")
	exp, err := newLogExporter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := exp.(*stdoutlog.Exporter); !ok {
		t.Fatalf("exp=%T, want *stdoutlog.Exporter", exp)
	}
}

// The bed's COLLECTOR_SERVICE_ADDR means a gRPC collector for logs as for
// spans: the log exporter goes through the same fallback.
func TestNewLogExporter_CollectorServiceAddrMeansGRPC(t *testing.T) {
	pinExporterEnv(t)
	t.Setenv("COLLECTOR_SERVICE_ADDR", "otel-collector:4317")
	t.Setenv("OTEL_LOGS_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "")
	exp, err := newLogExporter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer exp.Shutdown(context.Background())
	if _, ok := exp.(*otlploggrpc.Exporter); !ok {
		t.Fatalf("exp=%T, want *otlploggrpc.Exporter", exp)
	}
}

// testLogExporterName is an OTEL_LOGS_EXPORTER value registered with autoexport
// for the tests; it hands initTelemetry the exporter useTestLogExporter staged.
const testLogExporterName = "atropos-test-recorder"

var (
	registerTestLogExporter sync.Once
	stagedLogExporter       atomic.Pointer[recordingLogExporter]
)

// useTestLogExporter makes the next initTelemetry export logs to a fresh
// recordingLogExporter, selected through OTEL_LOGS_EXPORTER like any other.
func useTestLogExporter(t *testing.T) *recordingLogExporter {
	t.Helper()
	registerTestLogExporter.Do(func() {
		autoexport.RegisterLogExporter(testLogExporterName, func(context.Context) (sdklog.Exporter, error) {
			return stagedLogExporter.Load(), nil
		})
	})
	exp := &recordingLogExporter{}
	stagedLogExporter.Store(exp)
	t.Setenv("OTEL_LOGS_EXPORTER", testLogExporterName)
	return exp
}

// recordingSpanExporter is the in-memory sdktrace.SpanExporter the tracer-half
// tests assert on. tracetest.InMemoryExporter will not do: its Shutdown clears
// the spans it holds, so a test could not read what the provider flushed on the
// way out. This one keeps them and counts its Shutdown calls.
type recordingSpanExporter struct {
	mu        sync.Mutex
	spans     []sdktrace.ReadOnlySpan
	shutdowns int
}

func (e *recordingSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...) // copies the elements: the processor reuses its batch slice
	return nil
}

func (e *recordingSpanExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdowns++
	return nil
}

func (e *recordingSpanExporter) Spans() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan(nil), e.spans...)
}

func (e *recordingSpanExporter) Shutdowns() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdowns
}

// testSpanExporterName is an OTEL_TRACES_EXPORTER value registered with
// autoexport for the tests, the traces twin of testLogExporterName.
const testSpanExporterName = "atropos-test-span-recorder"

var (
	registerTestSpanExporter sync.Once
	stagedSpanExporter       atomic.Pointer[recordingSpanExporter]
)

// useTestSpanExporter makes the next initTelemetry export spans to a fresh
// recordingSpanExporter, selected through OTEL_TRACES_EXPORTER like any other.
func useTestSpanExporter(t *testing.T) *recordingSpanExporter {
	t.Helper()
	registerTestSpanExporter.Do(func() {
		autoexport.RegisterSpanExporter(testSpanExporterName, func(context.Context) (sdktrace.SpanExporter, error) {
			return stagedSpanExporter.Load(), nil
		})
	})
	exp := &recordingSpanExporter{}
	stagedSpanExporter.Store(exp)
	t.Setenv("OTEL_TRACES_EXPORTER", testSpanExporterName)
	return exp
}

// Both SDK providers export through the exporters the environment names. The
// default logger's OTel half goes through the logger provider: the record
// carries the request's trace, the bridge's scope and this process's resource.
// Shutdown flushes both signals out of their batch processors and stops their
// exporters. The tracer provider's half is pinned through the span exporter: it
// is shut down once, which only the tracer provider's own Shutdown does, and by
// then it holds the request's span. (The 100 ms batch timer can deliver the span
// before Shutdown runs, so "nothing before Shutdown" is not asserted.)
func TestInitTelemetry_DefaultLoggerExportsThroughTheSDKProvider(t *testing.T) {
	pinExporterEnv(t)
	restoreOTelGlobals(t)
	unsetEnv(t, "OTEL_TRACES_SAMPLER")
	spans := useTestSpanExporter(t)
	exp := useTestLogExporter(t)
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "svc", host: HostSidecar})
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := otel.Tracer("log-test").Start(context.Background(), "req")
	tel.Logger.InfoContext(ctx, "exported")
	span.End()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := spans.Shutdowns(); got != 1 {
		t.Fatalf("span exporter shut down %d times, want 1: Shutdown must stop the tracer provider", got)
	}
	// The global provider is shared with whatever else runs in this process, so
	// count the request's own span rather than every span exported.
	delivered := 0
	for _, s := range spans.Spans() {
		if s.Name() == "req" && s.SpanContext().TraceID() == span.SpanContext().TraceID() {
			delivered++
		}
	}
	if delivered != 1 {
		t.Fatalf("span exporter holds %d spans named %q in trace %s, want 1", delivered, "req", span.SpanContext().TraceID())
	}

	recs := exp.Records()
	if len(recs) != 1 {
		t.Fatalf("exported records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r.Body().AsString() != "exported" || r.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("record body %q trace %s, want %q in trace %s", r.Body().AsString(), r.TraceID(), "exported", span.SpanContext().TraceID())
	}
	if got := r.InstrumentationScope().Name; got != loggerScopeName {
		t.Fatalf("scope = %q, want %q", got, loggerScopeName)
	}
	if v, ok := r.Resource().Set().Value(trace.AttrHost); !ok || v.AsString() != HostSidecar {
		t.Fatalf("record resource atropos.host = %v (present=%v), want %q", v, ok, HostSidecar)
	}
}

// shutdownAll runs every step once, in order, even past a failing one, joins
// the failures, and answers every later call with the first call's result.
func TestShutdownAll_RunsEveryStepOnceAndJoinsErrors(t *testing.T) {
	errTraces, errLogs := errors.New("tracer provider"), errors.New("logger provider")
	var calls []string
	step := func(name string, err error) func(context.Context) error {
		return func(context.Context) error {
			calls = append(calls, name)
			return err
		}
	}
	shutdown := shutdownAll(step("traces", errTraces), step("logs", errLogs))
	err := shutdown(context.Background())
	if !errors.Is(err, errTraces) || !errors.Is(err, errLogs) {
		t.Fatalf("err = %v, want both failures joined", err)
	}
	if again := shutdown(context.Background()); again != err {
		t.Fatalf("second call = %v, want the first call's error", again)
	}
	if got := strings.Join(calls, ","); got != "traces,logs" {
		t.Fatalf("steps run = %q, want each once, in order", got)
	}
}
