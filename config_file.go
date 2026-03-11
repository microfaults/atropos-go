package atropos

import (
	"context"
	"fmt"
	"os"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"gopkg.in/yaml.v3"
)

// FileConfig is the YAML configuration structure for atropos.
type FileConfig struct {
	Service   ServiceConfig   `yaml:"service"`
	Collector CollectorConfig `yaml:"collector"`
	Sampler   SamplerConfig   `yaml:"sampler"`
}

// ServiceConfig identifies the service in OTel resource attributes.
type ServiceConfig struct {
	Name        string `yaml:"name"`
	Version     string `yaml:"version"`
	Environment string `yaml:"environment"`
}

// CollectorConfig controls the OTLP exporter connection.
type CollectorConfig struct {
	Endpoint string `yaml:"endpoint"`
	Insecure bool   `yaml:"insecure"`
}

// SamplerConfig selects the trace sampling strategy.
type SamplerConfig struct {
	Strategy string  `yaml:"strategy"`
	Ratio    float64 `yaml:"ratio"`
}

// LoadConfig reads and parses a YAML config file.
func LoadConfig(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("atropos: read config: %w", err)
	}
	var fc FileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("atropos: parse config: %w", err)
	}
	return &fc, nil
}

// DiscoverConfig searches for a config file:
//  1. $ATROPOS_CONFIG env var
//  2. ./atropos.yaml in CWD
//
// Returns nil, nil if no config found.
func DiscoverConfig() (*FileConfig, error) {
	if path := os.Getenv("ATROPOS_CONFIG"); path != "" {
		return LoadConfig(path)
	}
	if _, err := os.Stat("atropos.yaml"); err == nil {
		return LoadConfig("atropos.yaml")
	}
	return nil, nil
}

// ToOptions converts FileConfig to Init options.
func (fc *FileConfig) ToOptions() []Option {
	var opts []Option
	if fc.Service.Name != "" {
		opts = append(opts, WithServiceName(fc.Service.Name))
	}
	if fc.Service.Version != "" {
		opts = append(opts, WithServiceVersion(fc.Service.Version))
	}
	if fc.Service.Environment != "" {
		opts = append(opts, WithEnvironment(fc.Service.Environment))
	}
	if fc.Collector.Endpoint != "" {
		opts = append(opts, WithEndpoint(fc.Collector.Endpoint))
		opts = append(opts, WithInsecure(fc.Collector.Insecure))
	}
	switch fc.Sampler.Strategy {
	case "always_on", "":
		opts = append(opts, WithSampler(sdktrace.AlwaysSample()))
	case "never":
		opts = append(opts, WithSampler(sdktrace.NeverSample()))
	case "ratio":
		opts = append(opts, WithSampler(sdktrace.TraceIDRatioBased(fc.Sampler.Ratio)))
	}
	return opts
}

// InitFromConfig bootstraps OTel from a YAML config file.
// Programmatic opts override config file values.
// If path is empty, uses DiscoverConfig().
func InitFromConfig(ctx context.Context, path string, opts ...Option) (func(context.Context) error, error) {
	var fc *FileConfig
	var err error
	if path != "" {
		fc, err = LoadConfig(path)
	} else {
		fc, err = DiscoverConfig()
	}
	if err != nil {
		return nil, err
	}
	var allOpts []Option
	if fc != nil {
		allOpts = append(allOpts, fc.ToOptions()...)
	}
	allOpts = append(allOpts, opts...)
	return Init(ctx, allOpts...)
}
