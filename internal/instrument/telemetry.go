package instrument

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// telemetryConfig is the OTel bootstrap input. Serve fills it from Config;
// tests fill it directly (tracerProvider is the hermetic seam).
type telemetryConfig struct {
	serviceName    string
	serviceVersion string
	environment    string // "" = "development"
	sampler        sdktrace.Sampler
	tracerProvider oteltrace.TracerProvider // BYO: registered globally, caller owns shutdown
}

// initTelemetry bootstraps OpenTelemetry for the calling service.
// Returns a shutdown function that flushes pending spans.
func initTelemetry(ctx context.Context, cfg telemetryConfig) (func(context.Context) error, error) {
	// Always set propagators regardless of BYO path.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// BYO TracerProvider path: register it globally and return.
	if cfg.tracerProvider != nil {
		otel.SetTracerProvider(cfg.tracerProvider)
		// No shutdown needed for BYO provider — caller manages it.
		return func(context.Context) error { return nil }, nil
	}

	// The span exporter is chosen by the standard OTEL_* environment (see
	// newSpanExporter).
	exporter, err := newSpanExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("atropos: init span exporter: %w", err)
	}

	serviceName := cfg.serviceName
	if serviceName == "" {
		serviceName = "unknown"
	}
	environment := cfg.environment
	if environment == "" {
		environment = "development"
	}

	// Build resource with service metadata.
	// Use NewSchemaless to avoid schema URL conflicts between
	// resource.Default() (which tracks the SDK's semconv version)
	// and the semconv import version used here.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(cfg.serviceVersion),
			semconv.DeploymentEnvironmentName(environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("atropos: build resource: %w", err)
	}

	// Build TracerProvider.
	sampler := cfg.sampler
	if sampler == nil {
		sampler = sdktrace.AlwaysSample()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(100*time.Millisecond)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// applyExporterEnvFallback maps the bed's COLLECTOR_SERVICE_ADDR onto the
// standard OTLP variables when they are unset, and keeps the legacy
// schemeless endpoint form working: the OTLP exporters parse
// OTEL_EXPORTER_OTLP_ENDPOINT as a URL (a bare host:port yields an empty
// host), and autoexport defaults to http/protobuf while every schemeless
// endpoint this SDK ever accepted was a gRPC collector port. It mutates the
// process environment so autoexport and the exporters see one truth.
//
// An OTEL_EXPORTER_OTLP_ENDPOINT that is already a URL is left alone, and so
// is an environment that names no endpoint at all (the exporters' own
// defaults apply). Otherwise the endpoint gets an http:// scheme if it lacks
// one and the protocol becomes grpc unless the environment already names one.
// The result holds the variables set and their values, empty when nothing
// changed; a second call changes nothing. Values are read with surrounding
// whitespace trimmed, as the OTLP exporters read theirs, and a blank value
// counts as unset.
func applyExporterEnvFallback() map[string]string {
	const (
		endpointKey = "OTEL_EXPORTER_OTLP_ENDPOINT"
		protocolKey = "OTEL_EXPORTER_OTLP_PROTOCOL"
		addrKey     = "COLLECTOR_SERVICE_ADDR"
	)
	applied := map[string]string{}
	set := func(key, value string) {
		// Ignoring the error is safe: the keys are constants, so only a NUL byte can fail.
		if err := os.Setenv(key, value); err == nil {
			applied[key] = value
		}
	}

	explicit := strings.TrimSpace(os.Getenv(endpointKey))
	endpoint := explicit
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv(addrKey))
	}
	if endpoint == "" || strings.Contains(explicit, "://") {
		// No endpoint named, or the standard variable already holds a URL.
		return applied
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	set(endpointKey, endpoint)
	if strings.TrimSpace(os.Getenv(protocolKey)) == "" {
		set(protocolKey, "grpc")
	}
	return applied
}

// newSpanExporter builds the span exporter the environment selects:
// OTEL_TRACES_EXPORTER (otlp by default, console, none) and, for otlp,
// OTEL_EXPORTER_OTLP_PROTOCOL and the other OTEL_EXPORTER_OTLP_* variables,
// after applyExporterEnvFallback.
func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	applyExporterEnvFallback()
	return autoexport.NewSpanExporter(ctx)
}

// TelemetryConfig is the host-facing OTel bootstrap input. The span exporter
// comes from the environment through contrib autoexport: OTEL_TRACES_EXPORTER
// (otlp by default, console, none), OTEL_EXPORTER_OTLP_ENDPOINT,
// OTEL_EXPORTER_OTLP_PROTOCOL (grpc or http/protobuf; autoexport defaults to
// http/protobuf) and the other OTEL_EXPORTER_OTLP_* variables the exporters
// read. The bed's COLLECTOR_SERVICE_ADDR, and a schemeless
// OTEL_EXPORTER_OTLP_ENDPOINT such as otel-collector:4317, still work and
// mean a gRPC collector. The sampler is AlwaysSample.
type TelemetryConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string // "" = "development"
	// TracerProvider, if set, is registered globally instead of building an
	// OTLP exporter — the seam for tests and hosts that own their OTel setup.
	TracerProvider oteltrace.TracerProvider
}

// InitTelemetry bootstraps OpenTelemetry for a host and returns the span
// flush/shutdown function.
func InitTelemetry(ctx context.Context, cfg TelemetryConfig) (func(context.Context) error, error) {
	return initTelemetry(ctx, telemetryConfig{
		serviceName:    cfg.ServiceName,
		serviceVersion: cfg.ServiceVersion,
		environment:    cfg.Environment,
		tracerProvider: cfg.TracerProvider,
	})
}
