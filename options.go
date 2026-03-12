package atropos

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type config struct {
	serviceName    string
	serviceVersion string
	environment    string
	endpoint       string
	insecure       bool
	sampler        sdktrace.Sampler
	tracerProvider oteltrace.TracerProvider
}

func defaultConfig() config {
	return config{
		serviceName: "unknown",
		environment: "development",
		insecure:    true,
	}
}

// Option configures the Init function.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

// WithServiceName sets the service.name resource attribute.
func WithServiceName(name string) Option {
	return optionFunc(func(c *config) { c.serviceName = name })
}

// WithServiceVersion sets the service.version resource attribute.
func WithServiceVersion(version string) Option {
	return optionFunc(func(c *config) { c.serviceVersion = version })
}

// WithEnvironment sets the deployment.environment resource attribute.
func WithEnvironment(env string) Option {
	return optionFunc(func(c *config) { c.environment = env })
}

// WithEndpoint overrides the OTLP collector endpoint.
func WithEndpoint(endpoint string) Option {
	return optionFunc(func(c *config) { c.endpoint = endpoint })
}

// WithInsecure controls TLS on the OTLP exporter. Default true.
func WithInsecure(insecure bool) Option {
	return optionFunc(func(c *config) { c.insecure = insecure })
}

// WithSampler overrides the default sampler (AlwaysSample).
func WithSampler(s sdktrace.Sampler) Option {
	return optionFunc(func(c *config) { c.sampler = s })
}

// WithTracerProvider uses a pre-configured TracerProvider instead of building one.
func WithTracerProvider(tp oteltrace.TracerProvider) Option {
	return optionFunc(func(c *config) { c.tracerProvider = tp })
}
