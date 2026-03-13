package atropos

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Init bootstraps OpenTelemetry for the calling service.
// Returns a shutdown function that flushes pending spans.
func Init(ctx context.Context, opts ...Option) (func(context.Context) error, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}

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

	// Resolve OTLP endpoint.
	endpoint := cfg.endpoint
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		endpoint = os.Getenv("COLLECTOR_SERVICE_ADDR")
	}
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	// Build OTLP exporter options.
	exporterOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
	}
	if cfg.insecure {
		exporterOpts = append(exporterOpts, otlptracegrpc.WithInsecure())
	}

	// Build OTLP exporter.
	exporter, err := otlptracegrpc.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("atropos: init otlp exporter: %w", err)
	}

	// Build resource with service metadata.
	// Use NewSchemaless to avoid schema URL conflicts with resource.Default().
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(cfg.serviceName),
			semconv.ServiceVersion(cfg.serviceVersion),
			semconv.DeploymentEnvironment(cfg.environment),
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
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
