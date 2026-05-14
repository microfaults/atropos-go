package atropos

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
		if cfg.useHTTP {
			endpoint = "localhost:4318"
		} else {
			endpoint = "localhost:4317"
		}
	}

	var exporter sdktrace.SpanExporter
	var err error

	if cfg.useHTTP {
		exporterOpts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(endpoint),
		}
		if cfg.insecure {
			exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptracehttp.New(ctx, exporterOpts...)
	} else {
		dialOpts := []grpc.DialOption{}
		if cfg.insecure {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		exporterOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithDialOption(dialOpts...),
		}
		if cfg.insecure {
			exporterOpts = append(exporterOpts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, exporterOpts...)
	}

	if err != nil {
		return nil, fmt.Errorf("atropos: init otlp exporter: %w", err)
	}

	// Build resource with service metadata.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(resource.Default().SchemaURL(),
			semconv.ServiceName(cfg.serviceName),
			semconv.ServiceVersion(cfg.serviceVersion),
			semconv.DeploymentEnvironmentName(cfg.environment),
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
