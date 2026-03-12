# Atropos-Go Readiness Refactor

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make atropos-go consumable by external Go modules (service-beds) by exposing public types, adding YAML config support, filling test coverage gaps, and adding integration tests.

**Architecture:** Use Go type aliases to re-export internal types in the root `atropos` package — zero file moves, no import cycles. Add a thin config layer that reads YAML and converts to `Option` values. Add fault factory functions so evaluator implementors can construct faults without importing `internal/`.

**Tech Stack:** Go 1.25, `gopkg.in/yaml.v3` for config, existing OTel SDK, existing test patterns (stdlib `testing`).

---

## Design Considerations & Pitfalls

> These are things to keep in mind during implementation and for future service-beds integration.

### Current Blockers (fixed by this plan)

1. **`internal/` lock-in** — All consumer-facing types (`Evaluator`, `Fault`, `Handle`, `Result`, `Decision`, `CheckResult`, `Interceptor`) are in `internal/`. External modules cannot import them. This is the #1 blocker.

2. **No fault constructors** — Even after type aliases, consumers need to construct `Fault` values for their `Evaluator.Evaluate()` return. Inline fault types (`Latency`, `Hang`, `Error`) are in `internal/fault/inline`. We add factory functions to root.

3. **No config file support** — `Init()` is purely programmatic. Service-beds wants YAML-driven bootstrap (matching their k8s/Skaffold conventions).

### Design Headaches to Watch

4. **Package-level singleton** — `defaultInterceptor` is a global `*interceptor.Interceptor` initialized in `init()`. Only one interceptor per process. No per-endpoint evaluators. This is fine for now (service-beds services are single-purpose) but will need fixing if a service wants different fault rules for different routes.

5. **`log.Printf` for errors** — Middleware/gRPC interceptors swallow evaluator errors with `log.Printf`. No structured logging, no way to silence or redirect. Will cause noise in production. Future fix: accept `*slog.Logger` via option.

6. **`AlwaysSample()` default** — The default sampler samples 100% of traces. In production with real traffic, this will overwhelm the collector. Service-beds should override with `TraceIDRatioBased` or `ParentBased` — but the default is a footgun.

7. **No collector timeout** — `Init()` creates an OTLP gRPC exporter with no dial timeout. If the collector is unreachable, the exporter will retry silently (gRPC default behavior), but the initial `New()` call can block. Service-beds should set `OTEL_EXPORTER_OTLP_TIMEOUT` env var.

8. **No metrics emission** — Atropos records traces but emits no Prometheus counters (faults injected, faults failed, evaluation latency). The OTel collector's spanmetrics connector partially covers this, but dedicated metrics would be more reliable.

9. **Evaluator is opaque** — The `Evaluator` interface has no reference implementation. Service-beds must build evaluators from scratch. Future: `manteion` oracle pushes rules; for now, a static map-based evaluator example in README would help.

10. **Background fault lifecycle** — When `Mode == Background`, the fault runs in a goroutine and the middleware continues immediately. There's no lifecycle tracking — if the service shuts down, background faults may be orphaned. The `Init()` shutdown function only flushes spans, not faults.

11. **No fault deduplication** — If the evaluator returns a fault for every request to `/checkout`, you get N concurrent faults. No debounce, no "already faulting" check. This is by-design (independent fault injection per request) but can surprise users.

### What's Working Well

- Always-on spans give visibility even without faults
- EventAware pattern avoids span explosion for network/resource faults
- Label threading correlates requests to faults cleanly
- gRPC subpackage prevents dependency bloat for HTTP-only services
- Ramp phases enable realistic fault profiles

---

## Task 1: Type Aliases — Public API Surface

**Files:**
- Create: `types.go`
- Modify: `atropos.go` (update signatures to use aliases)
- Modify: `span.go` (update signatures)
- Modify: `middleware.go` (update signatures)

**Step 1: Create `types.go` with type aliases and constant re-exports**

```go
// types.go
package atropos

import (
	"atropos-go/internal/evaluator"
	"atropos-go/internal/fault"
	"atropos-go/internal/interceptor"
	"atropos-go/internal/trace"
)

// --- Fault types ---

// Fault is the interface all fault types implement.
type Fault = fault.Fault

// FaultConfig holds duration and ramp parameters common to all faults.
type FaultConfig = fault.FaultConfig

// Handle provides non-blocking control over a running fault.
type Handle = fault.Handle

// Result reports what happened during a fault.
type Result = fault.Result

// EventEmitter records a timestamped event on a span.
type EventEmitter = fault.EventEmitter

// EventAware faults emit span events via an injected emitter.
type EventAware = fault.EventAware

// --- Evaluator types ---

// Evaluator is the rule engine contract. Must be safe for concurrent use.
type Evaluator = evaluator.Evaluator

// InjectionPoint identifies where a fault check occurs.
type InjectionPoint = evaluator.InjectionPoint

// Request carries context for the evaluator decision.
type Request = evaluator.Request

// Decision is what the evaluator returns when rules match.
type Decision = evaluator.Decision

// Mode indicates how the fault runs relative to the request.
type Mode = evaluator.Mode

// Re-export InjectionPoint constants.
const (
	Ingress   = evaluator.Ingress
	Egress    = evaluator.Egress
	Transient = evaluator.Transient
	Custom    = evaluator.Custom
)

// Re-export Mode constants.
const (
	Background = evaluator.Background
	Inline     = evaluator.Inline
)

// --- Trace types ---

// Span records attributes, events, and lifecycle signals on a trace span.
type Span = trace.Span

// --- Interceptor types ---

// Interceptor ties the evaluator, fault execution, and OTel together.
type Interceptor = interceptor.Interceptor

// CheckResult holds the outcome of an injection point check.
type CheckResult = interceptor.CheckResult
```

**Step 2: Update `atropos.go` to use aliased types**

Replace `evaluator.Evaluator` and `interceptor.Interceptor` references with the root aliases. Remove direct `internal/evaluator` import (it's now pulled in through `types.go`).

```go
// atropos.go — updated signatures
package atropos

import (
	"atropos-go/internal/interceptor"
	"atropos-go/internal/trace"
)

var defaultInterceptor *Interceptor

func init() {
	defaultInterceptor = interceptor.New(nil, trace.NewOTelTracer())
}

// Configure swaps the evaluator on the default interceptor.
func Configure(eval Evaluator) {
	defaultInterceptor = interceptor.New(eval, trace.NewOTelTracer())
}

// DefaultInterceptor returns the package-level interceptor.
func DefaultInterceptor() *Interceptor {
	return defaultInterceptor
}
```

**Step 3: Update `span.go` to use aliased types**

```go
// span.go — updated return types
package atropos

import (
	"context"

	"atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

func Span(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, Span) {
	tracer := trace.NewOTelTracer()
	return tracer.Start(ctx, trace.SpanHookPrefix+name, attrs...)
}

func SpanWithFault(ctx context.Context, name string, labels map[string]string, attrs ...attribute.KeyValue) (context.Context, Span, CheckResult, error) {
	if labels == nil {
		labels = make(map[string]string)
	}
	for _, a := range attrs {
		labels[string(a.Key)] = a.Value.Emit()
	}
	return defaultInterceptor.Hook(ctx, name, labels)
}
```

**Step 4: Update `middleware.go` to use aliased types**

Replace `*interceptor.Interceptor` with `*Interceptor` in `WithInterceptor` and `middlewareConfig`.

**Step 5: Run build to verify no cycles**

Run: `go build ./...`
Expected: clean build, no import cycles

**Step 6: Run tests to verify nothing broke**

Run: `go test ./...`
Expected: all pass

**Step 7: Commit**

```bash
git add types.go atropos.go span.go middleware.go
git commit -m "refactor: expose public types via aliases — unblock external consumers"
```

---

## Task 2: Fault Factory Functions

**Files:**
- Create: `faults.go`
- Test: `faults_test.go`

**Step 1: Write tests for fault factories**

```go
// faults_test.go
package atropos

import (
	"context"
	"testing"
	"time"
)

func TestNewLatencyFault(t *testing.T) {
	f := NewLatencyFault(100*time.Millisecond, 0)
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}

	handle, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := <-handle.Done()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if r.ActualDuration < 80*time.Millisecond {
		t.Fatalf("expected ~100ms, got %s", r.ActualDuration)
	}
}

func TestNewLatencyFault_WithJitter(t *testing.T) {
	f := NewLatencyFault(50*time.Millisecond, 50*time.Millisecond)
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestNewHangFault(t *testing.T) {
	f := NewHangFault(100 * time.Millisecond)
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}

	handle, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := <-handle.Done()
	if r.ActualDuration < 80*time.Millisecond {
		t.Fatalf("expected ~100ms, got %s", r.ActualDuration)
	}
}

func TestNewErrorFault(t *testing.T) {
	f := NewErrorFault(503, "service unavailable")
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}

	handle, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := <-handle.Done()
	if r.Detail == nil {
		t.Fatal("expected error detail")
	}
}

func TestNewErrorFault_InvalidStatus(t *testing.T) {
	f := NewErrorFault(0, "bad")
	if err := f.Validate(); err == nil {
		t.Fatal("expected validation error for status 0")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test -run TestNew -v`
Expected: FAIL — `NewLatencyFault` undefined

**Step 3: Write `faults.go` with factory functions**

```go
// faults.go
package atropos

import (
	"time"

	"atropos-go/internal/fault"
	"atropos-go/internal/fault/inline"
)

// NewLatencyFault creates a fault that delays by base + rand(jitter).
func NewLatencyFault(delay, jitter time.Duration) Fault {
	return &inline.Latency{
		FaultConfig: fault.FaultConfig{Duration: delay + jitter},
		Delay:       delay,
		Jitter:      jitter,
	}
}

// NewHangFault creates a fault that blocks until duration expires.
func NewHangFault(duration time.Duration) Fault {
	return &inline.Hang{
		FaultConfig: fault.FaultConfig{Duration: duration},
	}
}

// NewErrorFault creates a fault that completes immediately with an HTTP error code.
func NewErrorFault(statusCode int, message string) Fault {
	return &inline.Error{
		FaultConfig: fault.FaultConfig{Duration: 1}, // instant; must be >0 for validation
		StatusCode:  statusCode,
		Message:     message,
	}
}
```

**Step 4: Run test to verify it passes**

Run: `go test -run TestNew -v`
Expected: all 4 PASS

**Step 5: Commit**

```bash
git add faults.go faults_test.go
git commit -m "feat: add fault factory functions for external evaluator authors"
```

---

## Task 3: YAML Config Spec and Loader

**Files:**
- Create: `config_file.go`
- Create: `config_file_test.go`
- Modify: `go.mod` (add `gopkg.in/yaml.v3`)

**Step 1: Add YAML dependency**

Run: `go get gopkg.in/yaml.v3`

**Step 2: Write tests for config loading**

```go
// config_file_test.go
package atropos

import (
	"os"
	"path/filepath"
	"testing"
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
		t.Fatalf("expected service name 'checkout', got %q", fc.Service.Name)
	}
	if fc.Service.Version != "1.2.0" {
		t.Fatalf("expected version '1.2.0', got %q", fc.Service.Version)
	}
	if fc.Service.Environment != "staging" {
		t.Fatalf("expected environment 'staging', got %q", fc.Service.Environment)
	}
	if fc.Collector.Endpoint != "otel-collector:4317" {
		t.Fatalf("expected endpoint, got %q", fc.Collector.Endpoint)
	}
	if !fc.Collector.Insecure {
		t.Fatal("expected insecure=true")
	}
	if fc.Sampler.Strategy != "ratio" {
		t.Fatalf("expected sampler strategy 'ratio', got %q", fc.Sampler.Strategy)
	}
	if fc.Sampler.Ratio != 0.5 {
		t.Fatalf("expected ratio 0.5, got %f", fc.Sampler.Ratio)
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
		t.Fatal("expected sampler to be set for ratio strategy")
	}
}

func TestFileConfigToOptions_AlwaysOnSampler(t *testing.T) {
	fc := &FileConfig{
		Sampler: SamplerConfig{Strategy: "always_on"},
	}
	opts := fc.ToOptions()
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	if cfg.sampler == nil {
		t.Fatal("expected always_on sampler")
	}
}

func TestFileConfigToOptions_NeverSampler(t *testing.T) {
	fc := &FileConfig{
		Sampler: SamplerConfig{Strategy: "never"},
	}
	opts := fc.ToOptions()
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	if cfg.sampler == nil {
		t.Fatal("expected never sampler")
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

	// Use BYO provider to avoid real OTLP connection.
	// InitFromConfig should merge YAML opts with programmatic opts.
	shutdown, err := InitFromConfig(
		t.Context(),
		path,
		WithTracerProvider(noopTracerProvider()),
	)
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
	// Change to a temp dir with no atropos.yaml
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
```

**Step 3: Run tests to verify they fail**

Run: `go test -run "TestLoadConfig|TestFileConfig|TestInitFromConfig|TestDiscoverConfig" -v`
Expected: FAIL — types/functions undefined

**Step 4: Write `config_file.go`**

```go
// config_file.go
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
// Strategy: "always_on" | "ratio" | "never". Default: "always_on".
// Ratio: only used when strategy is "ratio" (0.0-1.0).
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

// DiscoverConfig searches for a config file in this order:
//  1. $ATROPOS_CONFIG env var (explicit path)
//  2. ./atropos.yaml in the current directory
//
// Returns nil, nil if no config file is found.
func DiscoverConfig() (*FileConfig, error) {
	if path := os.Getenv("ATROPOS_CONFIG"); path != "" {
		return LoadConfig(path)
	}

	if _, err := os.Stat("atropos.yaml"); err == nil {
		return LoadConfig("atropos.yaml")
	}

	return nil, nil
}

// ToOptions converts a FileConfig to Init options.
// Programmatic options passed to Init/InitFromConfig take precedence.
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
	}
	// Insecure defaults to false in Go; only set explicitly if endpoint is set.
	if fc.Collector.Endpoint != "" {
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
// Programmatic opts are applied AFTER config file opts (higher precedence).
// If path is empty, uses DiscoverConfig() to find a config file.
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
	// Programmatic opts override config file.
	allOpts = append(allOpts, opts...)

	return Init(ctx, allOpts...)
}
```

**Step 5: Add test helper for noopTracerProvider**

Add to `config_file_test.go`:

```go
func noopTracerProvider() *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(tracetest.NewInMemoryExporter()),
	)
}
```

with imports for `sdktrace` and `tracetest`.

**Step 6: Run tests**

Run: `go test -run "TestLoadConfig|TestFileConfig|TestInitFromConfig|TestDiscoverConfig" -v`
Expected: all PASS

**Step 7: Commit**

```bash
git add config_file.go config_file_test.go go.mod go.sum
git commit -m "feat: add YAML config file support with InitFromConfig and DiscoverConfig"
```

---

## Task 4: gRPC Coverage Gaps — Stream and Client Interceptor Tests

**Files:**
- Modify: `grpc/interceptor_test.go`

**Step 1: Add stream server interceptor test**

```go
func TestStreamServerInterceptor_NoFault(t *testing.T) {
	i := interceptor.New(nil, trace.Noop())
	interceptorFn := StreamServerInterceptor(i)

	handler := func(srv any, ss grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamStuff"}

	err := interceptorFn(nil, &fakeServerStream{ctx: context.Background()}, info, handler)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamServerInterceptor_WithFault(t *testing.T) {
	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 30 * time.Millisecond},
			Reason: "test: stream fault",
			Mode:   evaluator.Inline,
		},
	}
	i := interceptor.New(eval, trace.Noop())
	interceptorFn := StreamServerInterceptor(i)

	handler := func(srv any, ss grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamStuff"}
	start := time.Now()
	err := interceptorFn(nil, &fakeServerStream{ctx: context.Background()}, info, handler)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected >= 20ms from fault, got %s", elapsed)
	}
}
```

**Step 2: Add client unary interceptor test**

```go
func TestUnaryClientInterceptor_NoFault(t *testing.T) {
	i := interceptor.New(nil, trace.Noop())
	interceptorFn := UnaryClientInterceptor(i)

	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	err := interceptorFn(context.Background(), "/test.Service/Call", "req", "reply", nil, invoker)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnaryClientInterceptor_WithFault(t *testing.T) {
	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 30 * time.Millisecond},
			Reason: "test: client fault",
			Mode:   evaluator.Inline,
		},
	}
	i := interceptor.New(eval, trace.Noop())
	interceptorFn := UnaryClientInterceptor(i)

	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	start := time.Now()
	err := interceptorFn(context.Background(), "/test.Service/Call", "req", "reply", nil, invoker)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected >= 20ms from fault, got %s", elapsed)
	}
}
```

**Step 3: Add stream client interceptor test**

```go
func TestStreamClientInterceptor_NoFault(t *testing.T) {
	i := interceptor.New(nil, trace.Noop())
	interceptorFn := StreamClientInterceptor(i)

	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return &fakeClientStream{}, nil
	}

	desc := &grpc.StreamDesc{StreamName: "StreamStuff"}
	cs, err := interceptorFn(context.Background(), desc, nil, "/test.Service/StreamStuff", streamer)
	if err != nil {
		t.Fatal(err)
	}
	if cs == nil {
		t.Fatal("expected non-nil client stream")
	}
}
```

**Step 4: Add fakeServerStream and fakeClientStream helpers**

```go
// fakeServerStream implements grpc.ServerStream for testing.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

// fakeClientStream implements grpc.ClientStream for testing.
type fakeClientStream struct {
	grpc.ClientStream
}
```

**Step 5: Run tests**

Run: `go test ./grpc/ -v`
Expected: all PASS (old + new)

**Step 6: Commit**

```bash
git add grpc/interceptor_test.go
git commit -m "test: add gRPC stream and client interceptor tests"
```

---

## Task 5: EventAware and Hook Event Verification Tests

**Files:**
- Modify: `internal/interceptor/interceptor_test.go` (or create new file)

**Step 1: Add test that verifies EventAware wiring records span events**

This test creates a fault that implements EventAware, verifies that when the fault emits events, they show up on the span.

```go
func TestCheck_EventAwareFault_EmitsSpanEvents(t *testing.T) {
	// Create a fault that implements EventAware and emits an event during Start().
	ef := &eventFault{
		emitName:  "test.event",
		emitAttrs: []attribute.KeyValue{attribute.String("key", "value")},
	}

	eval := &testEval{decision: &evaluator.Decision{
		Fault:  ef,
		Reason: "test: event aware",
		Mode:   evaluator.Inline,
	}}

	recorder := &spanRecorder{}
	i := New(eval, recorder)

	cr, err := i.Check(context.Background(), evaluator.Request{
		Point:  evaluator.Custom,
		Labels: map[string]string{"test": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}

	<-cr.Handle.Done()

	if len(recorder.events) == 0 {
		t.Fatal("expected EventAware fault to emit span events")
	}

	found := false
	for _, e := range recorder.events {
		if e == "test.event" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected event 'test.event', got events: %v", recorder.events)
	}
}
```

With supporting types:

```go
// eventFault implements Fault + EventAware for testing.
type eventFault struct {
	emit      fault.EventEmitter
	emitName  string
	emitAttrs []attribute.KeyValue
}

func (f *eventFault) Validate() error { return nil }
func (f *eventFault) Start(ctx context.Context) (*fault.Handle, error) {
	_, cancel := context.WithCancel(ctx)
	h := fault.NewHandle(cancel)
	go func() {
		defer cancel()
		if f.emit != nil {
			f.emit(f.emitName, f.emitAttrs...)
		}
		h.Send(fault.Result{ActualDuration: time.Millisecond})
	}()
	return h, nil
}
func (f *eventFault) SetEventEmitter(fn fault.EventEmitter) { f.emit = fn }

// spanRecorder is a test Tracer that records events.
type spanRecorder struct {
	events []string
}

func (r *spanRecorder) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return ctx, &recordingSpan{recorder: r}
}

type recordingSpan struct {
	recorder *spanRecorder
}

func (s *recordingSpan) SetAttributes(attrs ...attribute.KeyValue) {}
func (s *recordingSpan) AddEvent(name string, attrs ...attribute.KeyValue) {
	s.recorder.events = append(s.recorder.events, name)
}
func (s *recordingSpan) RecordResult(r fault.Result)                {}
func (s *recordingSpan) EndWithError(err error)                      {}
func (s *recordingSpan) End()                                        {}
```

**Step 2: Run test**

Run: `go test ./internal/interceptor/ -run TestCheck_EventAware -v`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/interceptor/interceptor_test.go
git commit -m "test: verify EventAware fault wiring emits span events"
```

---

## Task 6: Integration Test — Full Init → Middleware → Fault → Span Flow

**Files:**
- Create: `integration_test.go`

This is a white-box integration test in the root package that wires Init (BYO provider) → Configure (test evaluator) → IngressMiddleware → verify fault ran and spans were recorded.

**Step 1: Write the integration test**

```go
// integration_test.go
package atropos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"atropos-go/internal/evaluator"
	"atropos-go/internal/fault/inline"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestIntegration_IngressMiddleware_WithFault(t *testing.T) {
	// 1. Set up in-memory exporter + BYO TracerProvider.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	// 2. Configure with a test evaluator that always returns a latency fault.
	Configure(&integrationEval{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 50 * time.Millisecond},
			Reason: "integration test",
			Mode:   evaluator.Inline,
		},
	})
	// Reset to noop after test.
	defer Configure(nil)

	// 3. Wrap a simple handler with IngressMiddleware.
	handler := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), "test-service")

	// 4. Fire a request and verify the fault delayed it.
	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected >= 40ms from inline fault, got %s", elapsed)
	}

	// 5. Verify spans were recorded.
	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected spans to be recorded")
	}

	// Should have at least: otelhttp server span + fault injection span.
	var foundFaultSpan bool
	for _, s := range spans {
		if s.Name == "atropos.fault.inject" {
			foundFaultSpan = true
		}
	}
	if !foundFaultSpan {
		names := make([]string, len(spans))
		for i, s := range spans {
			names[i] = s.Name
		}
		t.Fatalf("expected 'atropos.fault.inject' span, got: %v", names)
	}

	t.Logf("integration test: %d spans recorded, fault took %s", len(spans), elapsed)
}

func TestIntegration_SpanWithFault_ProducesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	Configure(&integrationEval{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 20 * time.Millisecond},
			Reason: "span-with-fault test",
			Mode:   evaluator.Inline,
		},
	})
	defer Configure(nil)

	ctx, span, cr, err := SpanWithFault(context.Background(), "checkout", map[string]string{"user": "test"})
	if err != nil {
		t.Fatal(err)
	}
	_ = ctx

	if cr.Handle != nil {
		<-cr.Handle.Done()
	}
	span.End()

	spans := exporter.GetSpans()
	if len(spans) < 2 {
		t.Fatalf("expected >= 2 spans (hook + fault), got %d", len(spans))
	}
}

func TestIntegration_NoFault_StillCreatesSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	shutdown, err := Init(context.Background(), WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	// No evaluator configured — should still create a span (always-on).
	Configure(nil)

	ctx, span := Span(context.Background(), "my-operation")
	_ = ctx
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span for always-on tracing")
	}
}

type integrationEval struct {
	decision *evaluator.Decision
}

func (e *integrationEval) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return e.decision
}
```

**Step 2: Run tests**

Run: `go test -run TestIntegration -v -count=1`
Expected: all PASS

**Step 3: Commit**

```bash
git add integration_test.go
git commit -m "test: add integration tests for Init→Middleware→Fault→Span flow"
```

---

## Task 7: Verify and Clean Up

**Step 1: Full build**

Run: `go vet ./... && go build ./...`
Expected: clean

**Step 2: Full test suite**

Run: `go test ./... -count=1`
Expected: all pass

**Step 3: Verify type aliases work from consumer perspective**

Create a temporary test in `/tmp` that imports `atropos-go` as an external module and verifies it can implement `Evaluator` and use `NewLatencyFault`. This is a manual verification step — if the type aliases are correct, this should compile.

**Step 4: Final commit if any cleanup needed**

---

## Summary of Changes

| Area | Files | What |
|------|-------|------|
| Type aliases | `types.go`, `atropos.go`, `span.go`, `middleware.go` | Expose `Evaluator`, `Fault`, `Handle`, `Result`, `Decision`, `CheckResult`, `Interceptor`, constants |
| Fault factories | `faults.go`, `faults_test.go` | `NewLatencyFault`, `NewHangFault`, `NewErrorFault` |
| Config YAML | `config_file.go`, `config_file_test.go` | `LoadConfig`, `DiscoverConfig`, `InitFromConfig`, `FileConfig` |
| gRPC tests | `grpc/interceptor_test.go` | Stream server/client, unary client interceptors |
| EventAware tests | `internal/interceptor/interceptor_test.go` | Verify event emission wiring |
| Integration tests | `integration_test.go` | End-to-end Init→Middleware→Fault→Span |
