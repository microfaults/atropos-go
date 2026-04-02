package atropos

import (
	"os"
	"path/filepath"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLoadConfig_FullYAML(t *testing.T) {
	yaml := `
service:
  name: checkout
  version: "1.2.0"
  environment: staging
collector:
  endpoint: otel-collector:4317
  insecure: true
sampler:
  strategy: ratio
  ratio: 0.5
`
	path := writeTestConfig(t, yaml)
	fc, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if fc.Service.Name != "checkout" {
		t.Fatalf("expected 'checkout', got %q", fc.Service.Name)
	}
	if fc.Service.Version != "1.2.0" {
		t.Fatalf("expected '1.2.0', got %q", fc.Service.Version)
	}
	if fc.Service.Environment != "staging" {
		t.Fatalf("expected 'staging', got %q", fc.Service.Environment)
	}
	if fc.Collector.Endpoint != "otel-collector:4317" {
		t.Fatalf("expected endpoint, got %q", fc.Collector.Endpoint)
	}
	if !fc.Collector.Insecure {
		t.Fatal("expected insecure=true")
	}
	if fc.Sampler.Strategy != "ratio" {
		t.Fatalf("expected 'ratio', got %q", fc.Sampler.Strategy)
	}
	if fc.Sampler.Ratio != 0.5 {
		t.Fatalf("expected 0.5, got %f", fc.Sampler.Ratio)
	}
}

func TestLoadConfig_MinimalYAML(t *testing.T) {
	yaml := `
service:
  name: frontend
`
	path := writeTestConfig(t, yaml)
	fc, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if fc.Service.Name != "frontend" {
		t.Fatalf("expected 'frontend', got %q", fc.Service.Name)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/atropos.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	path := writeTestConfig(t, "{{invalid yaml")
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestFileConfigToOptions(t *testing.T) {
	fc := &FileConfig{
		Service: ServiceConfig{
			Name:        "test-svc",
			Version:     "2.0",
			Environment: "production",
		},
		Collector: CollectorConfig{
			Endpoint: "collector:4317",
			Insecure: false,
		},
		Sampler: SamplerConfig{
			Strategy: "ratio",
			Ratio:    0.1,
		},
	}
	opts := fc.ToOptions()
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	if cfg.serviceName != "test-svc" {
		t.Fatalf("expected 'test-svc', got %q", cfg.serviceName)
	}
	if cfg.serviceVersion != "2.0" {
		t.Fatalf("expected '2.0', got %q", cfg.serviceVersion)
	}
	if cfg.environment != "production" {
		t.Fatalf("expected 'production', got %q", cfg.environment)
	}
	if cfg.endpoint != "collector:4317" {
		t.Fatalf("expected 'collector:4317', got %q", cfg.endpoint)
	}
	if cfg.insecure != false {
		t.Fatal("expected insecure=false")
	}
	if cfg.sampler == nil {
		t.Fatal("expected sampler for ratio strategy")
	}
}

func TestFileConfigToOptions_SamplerStrategies(t *testing.T) {
	for _, strategy := range []string{"always_on", "never"} {
		t.Run(strategy, func(t *testing.T) {
			fc := &FileConfig{Sampler: SamplerConfig{Strategy: strategy}}
			opts := fc.ToOptions()
			cfg := defaultConfig()
			for _, o := range opts {
				o.apply(&cfg)
			}
			if cfg.sampler == nil {
				t.Fatalf("expected sampler for strategy %q", strategy)
			}
		})
	}
}

func TestInitFromConfig(t *testing.T) {
	yaml := `
service:
  name: integration-test
collector:
  endpoint: localhost:4317
  insecure: true
sampler:
  strategy: always_on
`
	path := writeTestConfig(t, yaml)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	shutdown, err := InitFromConfig(t.Context(), path, WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t.Context())
}

func TestDiscoverConfig_EnvVar(t *testing.T) {
	yaml := `
service:
  name: from-env
`
	path := writeTestConfig(t, yaml)
	t.Setenv("ATROPOS_CONFIG", path)
	fc, err := DiscoverConfig()
	if err != nil {
		t.Fatal(err)
	}
	if fc.Service.Name != "from-env" {
		t.Fatalf("expected 'from-env', got %q", fc.Service.Name)
	}
}

func TestDiscoverConfig_NoConfigReturnsNil(t *testing.T) {
	t.Setenv("ATROPOS_CONFIG", "")
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)
	fc, err := DiscoverConfig()
	if err != nil {
		t.Fatal(err)
	}
	if fc != nil {
		t.Fatal("expected nil config when no file found")
	}
}

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "atropos.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
