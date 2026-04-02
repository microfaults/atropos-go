# Changelog

All notable changes to atropos-go are documented in this file.

## Unreleased

### Changed
- Refactored gRPC interceptors: extracted shared `checkAndWait` helper, reduced boilerplate across all four interceptors.
- Refactored network proxy `conn.go`: extracted `checkHijackers()` and `streamWorker()` helpers from `handleAffected`.
- Consolidated duplicate test cases into table-driven subtests (`config_file_test.go`, `grpc/interceptor_test.go`, `init_test.go`, `metrics_test.go`).

### Removed
- Loadgen service (moved to [zeus](https://git.ucsc.edu/microfaults/zeus)).

## 2026-03-28 — Prometheus Metrics

### Added
- Native Prometheus metrics for HTTP ingress/egress middleware (`http_server_requests_total`, `http_client_requests_total`, duration histograms).
- `MetricsHandler()` for `/metrics` scrape endpoint.

## 2026-03-27 — RAM Hogger

### Added
- Memory stress fault (`resource/memory`) with configurable target load and ramp.

## 2026-03-26 — Readiness Refactor

### Changed
- Public API layer: `Init()`, middleware wrappers, OTel bootstrap.
- Refactored interceptor and evaluator into cleaner public surface.

## 2026-03-20 — Demo Readiness

### Added
- `FaultAdminHandler` for runtime fault injection via HTTP.
- CPU and memory stress faults wired into admin handler.

### Fixed
- `NewSchemaless` usage to avoid OTel schema URL conflict.
- `otlptracegrpc.WithInsecure()` for plaintext gRPC.

## 2026-03-18 — Disk Utility

### Added
- Disk stress fault (`resource/disk`).

## 2026-03-15 — Network Faults & OTel

### Added
- TCP proxy with six toxics: latency, blackhole, RST, throttle, packet loss, drip.
- `Toxic` (stream) and `ConnToxic` (connection-level) interfaces.
- `ExternalProxy` interface as Toxiproxy integration hook.
- Injection point abstraction: Ingress, Egress, Transient, Custom.
- Evaluator interface (rule engine contract) with `Decision` and `Mode`.
- OTel instrumentation: `Tracer`/`Span` interfaces, span events, attribute constants.
- Interceptor tying evaluator + trace + fault execution.
- HTTP middleware (ingress/egress) and Hook for custom injection points.
- Inline HTTP faults: latency, error, hang.

## 2026-03-10 — I/O Fault

### Added
- I/O fault injection with rate-controlled reads (`resource/io`).

## 2026-03-05 — CPU Fault

### Added
- CPU stress fault (`resource/cpu`) with cgroup v1/v2 detection.
- Percentage-based utilization targeting.
- Core `fault.Fault` interface: `Validate()`, `Start(ctx) (*Handle, error)`.
- `fault.Handle` with `Done()`, `Stop()`, `Send()`, `SetOnResult()`.

## 2026-03-01 — Initial

### Added
- Project scaffolding and initial commit.
