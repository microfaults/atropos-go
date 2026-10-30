# Telemetry SDK (ADR-3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make both hosts of the instrument (`atropos.Serve` and `atroposd`) emit trace-correlated logs, one `atropos.decision` log event per applied decision, fault spans linked to the requests around them, experiment baggage on egress, exemplar-bearing Prometheus series, and exporters chosen by the standard `OTEL_*` environment.

**Architecture:** `internal/instrument/telemetry.go` stays the single OTel bootstrap for both hosts and grows a `LoggerProvider`, an `autoexport`-selected exporter pair, a resource carrying instance identity, and a filtered baggage propagator; `Config.Logger` defaults to a slog logger teed into stdout JSON and the `otelslog` bridge. `internal/interceptor` gains a per-instance ring of recent requests, run ids minted by the fault registry, span links on background fault spans, join accounting, and one decision event per applied decision emitted through the OTel Logs API (`internal/trace/decision.go`). Metrics stay on client_golang (a deliberate simplification of ADR-3 item 7): the request-duration histograms and the fault/cache-box counters gain exemplars and a `service` label, and `/metrics` serves OpenMetrics.

**Tech Stack:** Go 1.25 module (`go 1.25.5`, Go 1.27 host); `go.opentelemetry.io/otel` + `sdk` + `trace` v1.46.0; `otel/log` + `sdk/log` v0.22.0; `contrib/exporters/autoexport` + `otelhttp` v0.71.0; `contrib/bridges/otelslog` v0.20.1; `semconv/v1.40.0`; `client_golang` v1.24.1; optional `contrib/otelconf` v0.26.0.

**Spec:** `docs/plans/2026-09-27-daemon-design-review-2.md` §4 (ADR-3, items 1–5 and 8–9 as written; item 7 simplified: metrics stay on client_golang with exemplars and a `service` label; item 6 and 10 are collector/bed work, out of scope); `docs/plans/2026-09-08-atropos-daemon-design.md` §5.7 (sidecar resource: `service.instance.id`, `atropos.host=sidecar`). Current-state facts: the research notes' sections C and D (fault spans root-less, no links, no logs pipeline, no exemplars, `log.Printf` leftovers).

## Global Constraints

- The module stays `go 1.25.5` (CI runs `golang:1.25`). Every new module is pinned at a version whose own `go` directive is `1.25.0`; the verified set is: otel/sdk/trace/metric v1.46.0, otlptracegrpc/otlptracehttp/stdouttrace v1.46.0, otel/log, sdk/log, log/logtest, otlploggrpc, otlploghttp, stdoutlog v0.22.0, otelhttp and autoexport v0.71.0, otelslog v0.20.1, otelconf v0.26.0, client_golang v1.24.1 (autoexport/otelconf require ≥ v1.24.1). The next releases (otel v1.47.0, contrib v0.72.0, otelconf v0.27.0, client_golang v1.25.0) require `go 1.26.0` — never take them.
- Dependency discipline: run `go get` with the exact pins in ONE command, in the task that first imports the module, and run `go mod tidy` only after the importing code exists. `tidy` drops a pinned module nothing imports, and a later `tidy` re-resolves that import at `latest`, which bumps the directive to `go 1.26.0`. After every task: `grep -q '^go 1.25.5$' go.mod || echo "GO DIRECTIVE DRIFTED"` must print nothing.
- Verification after every task, all green: `gofmt -l $(git ls-files '*.go')` (empty; this skips `.claude/` and `vendor/`), `go vet ./...`, `go test -count=1 ./...`, `go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`.
- Commits are conventional commits and end with the trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- One `semconv` import for both hosts: `go.opentelemetry.io/otel/semconv/v1.40.0`, only in `internal/instrument/telemetry.go` (the sidecar reaches it through `InitTelemetry`). `otelhttp` v0.71.0 emits only the stable HTTP names (`http.request.method`, `http.response.status_code`, `url.path`, `server.address`); its `internal/semconv` has no legacy variant, so no `OTEL_SEMCONV_STABILITY_OPT_IN` handling exists or is needed.
- Fixed names. Event: `atropos.decision`. Decision attributes: `atropos.rule.name`, `atropos.reason`, `atropos.fault.id`, `atropos.injection_point`, `atropos.experiment.id`, `atropos.phase.id`, `atropos.recent.trace_ids`. Span attributes: `atropos.fault.id`, `atropos.fault.joined`; link attribute `atropos.link.kind` ∈ {`trigger`, `recent`, `fault`}. Baggage members: `atropos.workflow` (existing), `atropos.experiment_id`, `atropos.phase_id`. Resource: `service.instance.id`, `k8s.pod.name`, `atropos.host` ∈ {`in-process`, `sidecar`}. Exemplar labels: `trace_id`, `span_id`. Logger scope names: `git.ucsc.edu/microfaults/atropos-go` (slog bridge), `git.ucsc.edu/microfaults/atropos-go/decision` (decision events).
- Environment honoured by `InitTelemetry`: `OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER` (`otlp` default, `console`, `none`), `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc` | `http/protobuf`, autoexport default `http/protobuf`), the other `OTEL_EXPORTER_OTLP_*` read by the exporters, `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`, `OTEL_RESOURCE_ATTRIBUTES` / `OTEL_SERVICE_NAME` (through `resource.Default()`), `COLLECTOR_SERVICE_ADDR` (bed fallback), `ATROPOS_POD_NAME`, `HOSTNAME`, and (Task 11) `OTEL_CONFIG_FILE`.
- Hosts never build telemetry themselves: `serve.go` and `internal/atroposd/daemon.go` only call `instrument.InitTelemetry` and use what it returns.
- Ring and link sizes: 256 recent requests per interceptor, at most 16 of them as links on a fault span.

## Review Focus

1. A schemeless `OTEL_EXPORTER_OTLP_ENDPOINT` such as `otel-collector:4317` (what `deploy/sidecar/kustomize/recommendationservice-sidecar.yaml:61-62` ships today, and what `COLLECTOR_SERVICE_ADDR` carries in every bed manifest) must keep exporting over gRPC: the OTLP exporters parse the variable as a URL (`u.Host` is empty for a schemeless value) and autoexport defaults to `http/protobuf`. Pinned by `TestExporterEnvFallback_SchemelessEndpointKeepsGRPC` (Task 2).
2. An unsampled request (`OTEL_TRACES_SAMPLER=always_off`, or a remote parent that is not sampled) must not leave exemplars or links that point at traces that were never stored. Pinned by `TestRequestRing_IgnoresUnsampled` (Task 6) and `TestExemplar_NilWhenUnsampled` (Task 9).
3. Foreign baggage (`tenant=acme`) arriving at the SDK's transport or the sidecar's reverse proxy must not be forwarded unless its key is allowlisted, and the allowlist must be extendable per host. Pinned by `TestBaggagePropagator_FiltersForeignMembers` and `TestEgressTransport_ForeignBaggageDropped` (Task 8).
4. A host that brings its own `TracerProvider` (every existing test, hosts that own their OTel setup) must not get a network log exporter opened behind its back. Pinned by `TestInitTelemetry_BYOTracerProviderSkipsLogPipeline` (Task 4).
5. A background fault armed from `Core.Arm` (`context.Background()`, no request span) must still start with only ring links, and a joiner that meets a run whose span is nil (the `StartOrJoin` wrapper path) must not panic. Pinned by `TestStartFault_ArmWithoutRequestSpan` and `TestRegistry_StartRun_JoinWithNilSpan` (Task 6).

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/instrument/telemetry.go` (modify) | OTel bootstrap for both hosts: env fallback shim, autoexport span/log exporters, resource, sampler from env, propagator, `Telemetry` return value |
| `internal/instrument/logging.go` (create) | `NewLogger` (stdout JSON + otelslog bridge) and the `teeHandler` |
| `internal/instrument/otelconf.go` (create, Task 11) | `OTEL_CONFIG_FILE` declarative configuration branch |
| `internal/instrument/middleware.go` (modify) | metrics inside the otelhttp span so exemplars see the trace id; `service` label on cache-box counters |
| `internal/instrument/core.go` (modify) | wires logger, service name and the fault-injection hook into the interceptor |
| `internal/trace/trace.go`, `noop.go` (modify) | `Tracer.StartLinked`, `Span.AddLink`, `Span.SpanContext` |
| `internal/trace/attrs.go` (modify) | new attribute, event and link-kind names |
| `internal/trace/decision.go` (create) | `EmitDecision`: the `atropos.decision` log record through the Logs API |
| `internal/trace/baggage.go` (create) | baggage keys, allowlisted `BaggagePropagator`, `WithExperimentBaggage`, `ExperimentFromContext` |
| `internal/interceptor/ring.go` (create) | 256-entry ring of recent sampled requests, links and trace-id snapshots |
| `internal/interceptor/registry.go` (modify) | `FaultRun` (run id, span, joined count), `StartRun`; `StartOrJoin` becomes a wrapper |
| `internal/interceptor/interceptor.go` (modify) | logger/service options, ring recording in `Evaluate`, links and `atropos.fault.id` stamps, join accounting, decision events, hook with ctx |
| `internal/interceptor/middleware.go`, `cachebox.go`, `hook.go` (modify) | request-scoped logging, egress baggage injection, cache-box decision event, join-aware hook events |
| `internal/metrics/metrics.go` (modify) | `service` label, exemplar helpers, `FaultInjectionHook`, OpenMetrics handler |
| `grpc/interceptor.go` (modify) | `log.Printf` → interceptor logger with context |
| `serve.go`, `internal/atroposd/daemon.go`, `internal/atroposd/config.go`, `cmd/atroposd/main.go` (modify) | consume `Telemetry`; `Config.LoggerProvider`, `Config.PropagateBaggage`; sidecar `ATROPOSD_PROPAGATE_BAGGAGE` |
| `atropos.go` (modify) | baseline interceptor uses `metrics.FaultInjectionHook("")` |
| `deploy/sidecar/kustomize/recommendationservice-sidecar.yaml` (modify) | endpoint in URL form plus `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` |
| `README.md`, `AGENTS.md`, `CHANGELOG.md` (modify) | telemetry configuration, decision events, exemplars, baggage leak note |

---

### Task 1: Pin one OpenTelemetry version set on the Go 1.25 floor

**Files:**
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: nothing.
- Produces: the module versions every later task imports against (otel v1.46.0, otelhttp v0.71.0, client_golang v1.24.1).

- [ ] **Step 1: Record the baseline**

Run: `go test -count=1 ./... 2>&1 | tail -3 && grep '^go ' go.mod`
Expected: every package `ok`; `go 1.25.5`.

- [ ] **Step 2: Bump the modules the code already imports, pinned, in one command**

```bash
go get go.opentelemetry.io/otel@v1.46.0 go.opentelemetry.io/otel/sdk@v1.46.0 \
  go.opentelemetry.io/otel/trace@v1.46.0 go.opentelemetry.io/otel/metric@v1.46.0 \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@v1.46.0 \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.46.0 \
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp@v0.71.0 \
  github.com/prometheus/client_golang@v1.24.1
go mod tidy
```

- [ ] **Step 3: Verify the directive, the set and the build**

Run: `grep '^go ' go.mod && go list -m all | grep -E '^go.opentelemetry.io/(otel|otel/sdk|otel/trace) |otelhttp |client_golang |google.golang.org/grpc ' && go build ./...`
Expected: `go 1.25.5`; `go.opentelemetry.io/otel v1.46.0`, `go.opentelemetry.io/otel/sdk v1.46.0`, `go.opentelemetry.io/otel/trace v1.46.0`, `otelhttp v0.71.0`, `github.com/prometheus/client_golang v1.24.1`, `google.golang.org/grpc v1.83.2`; build silent.

- [ ] **Step 4: Full verification**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`
Expected: no gofmt output; all `ok`.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum
git commit -m "build(deps): pin OpenTelemetry v1.46.0 / contrib v0.71.0 and client_golang v1.24.1

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Exporters from the environment (autoexport) with `COLLECTOR_SERVICE_ADDR` compatibility

**Files:**
- Modify: `internal/instrument/telemetry.go:22-33` (config struct), `:51-101` (endpoint precedence and hand-rolled exporters), `:145-155` (`TelemetryConfig` doc)
- Modify: `internal/instrument/telemetry_test.go` (append tests)
- Modify: `deploy/sidecar/kustomize/recommendationservice-sidecar.yaml:61-62`
- Modify: `CHANGELOG.md:5` (`## Unreleased`)

**Interfaces:**
- Consumes: Task 1 versions.
- Produces:
  - `func applyExporterEnvFallback() map[string]string` — mutates the process environment (`os.Setenv`) and returns the variables it set, for logging.
  - `func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error)` — `autoexport.NewSpanExporter(ctx)` after the fallback.
  - `telemetryConfig` loses `endpoint`, `useHTTP`, `secure` (no caller sets them: `grep -rn 'useHTTP\|secure:' internal/ serve.go` is empty apart from this file).

- [ ] **Step 1: Add the new modules, pinned, in one command (the test file imports them)**

```bash
go get go.opentelemetry.io/contrib/exporters/autoexport@v0.71.0 \
  go.opentelemetry.io/otel/exporters/stdout/stdouttrace@v1.46.0
```

Do not run `go mod tidy` until Step 4's code imports both packages.

- [ ] **Step 2: Write the failing tests in `internal/instrument/telemetry_test.go`**

```go
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

// Review Focus 1: the bare host:port form the old exporter accepted.
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

func TestNewSpanExporter_None(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	exp, err := newSpanExporter(context.Background())
	if err != nil || !autoexport.IsNoneSpanExporter(exp) {
		t.Fatalf("exp=%T err=%v, want the none exporter", exp, err)
	}
}

func TestNewSpanExporter_Console(t *testing.T) {
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
```

Imports to add in the test file: `os`, `go.opentelemetry.io/contrib/exporters/autoexport`, `go.opentelemetry.io/otel/exporters/stdout/stdouttrace`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument/ -run 'TestExporterEnvFallback|TestNewSpanExporter|TestInitTelemetry_NoBYO'`
Expected: build failure `undefined: applyExporterEnvFallback` / `undefined: newSpanExporter`, `FAIL`.

- [ ] **Step 4: Implement in `internal/instrument/telemetry.go`**

Replace lines 51–101 (endpoint precedence and the two hand-built exporters) with:

```go
// applyExporterEnvFallback maps the bed's COLLECTOR_SERVICE_ADDR onto the
// standard OTLP variables when they are unset, and keeps the legacy
// schemeless endpoint form working: the OTLP exporters parse
// OTEL_EXPORTER_OTLP_ENDPOINT as a URL (a bare host:port yields an empty
// host), and autoexport defaults to http/protobuf while every schemeless
// endpoint this SDK ever accepted was a gRPC collector port. It mutates the
// process environment so autoexport and the exporters see one truth.
func applyExporterEnvFallback() map[string]string
```

Rules, in order: (1) if `OTEL_EXPORTER_OTLP_ENDPOINT` is empty and `COLLECTOR_SERVICE_ADDR` is not: endpoint = addr with `http://` prefixed when it has no `://`, and `OTEL_EXPORTER_OTLP_PROTOCOL` = `grpc` if empty; (2) else if the endpoint is non-empty and has no `://`: prefix `http://` and set protocol `grpc` if empty; (3) otherwise nothing. Return the map of keys set.

```go
func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	applyExporterEnvFallback()
	return autoexport.NewSpanExporter(ctx)
}
```

`initTelemetry` calls `newSpanExporter` (keep `sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(100*time.Millisecond))`). Remove `endpoint`, `useHTTP`, `secure` from `telemetryConfig` and the now-unused `otlptracegrpc`, `otlptracehttp`, `grpc`, `insecure` and `strings` imports (`os` stays: the fallback reads and sets the environment). Update the `TelemetryConfig` doc at `:145-147` to name the env variables from Global Constraints.

- [ ] **Step 5: Tidy, then run the tests to verify they pass**

Run: `go mod tidy && grep '^go ' go.mod && go test -count=1 ./internal/instrument/ -run 'TestExporterEnvFallback|TestNewSpanExporter|TestInitTelemetry' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `go 1.25.5`; `--- PASS` for the five new tests and the four existing `TestInitTelemetry_*`; `ok`.

- [ ] **Step 6: Fix the in-repo sidecar manifest and note the change**

`deploy/sidecar/kustomize/recommendationservice-sidecar.yaml:61-62` becomes:

```yaml
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: http://otel-collector:4317
            - name: OTEL_EXPORTER_OTLP_PROTOCOL
              value: grpc
```

Under `## Unreleased` in `CHANGELOG.md` add a `### Changed — Telemetry (ADR-3)` heading with one bullet: exporters are selected by `OTEL_TRACES_EXPORTER` / `OTEL_LOGS_EXPORTER` / `OTEL_EXPORTER_OTLP_*` through contrib `autoexport`; `COLLECTOR_SERVICE_ADDR` and a schemeless `OTEL_EXPORTER_OTLP_ENDPOINT` still work and imply gRPC.

- [ ] **Step 7: Full verification**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/... && grep '^go ' go.mod`
Expected: no gofmt output; all `ok`; `go 1.25.5`.

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum internal/instrument/telemetry.go internal/instrument/telemetry_test.go deploy/sidecar/kustomize/recommendationservice-sidecar.yaml CHANGELOG.md
git commit -m "feat(telemetry): select exporters with autoexport; map COLLECTOR_SERVICE_ADDR onto the OTLP env

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Sampler from the environment; resource identity (`service.instance.id`, `k8s.pod.name`, `atropos.host`)

**Files:**
- Modify: `internal/instrument/telemetry.go` (resource block, currently `:112-126`; sampler block `:128-138`; `TelemetryConfig` `:148-155`; `InitTelemetry` `:159-166`)
- Modify: `internal/instrument/telemetry_test.go` (append tests)
- Modify: `serve.go:137-142` (pass `Host: instrument.HostInProcess`), `internal/atroposd/daemon.go:70-75` (pass `Host: instrument.HostSidecar`)

**Interfaces:**
- Consumes: Task 2's `initTelemetry`/`telemetryConfig`.
- Produces:
  - `TelemetryConfig.Host string` and `telemetryConfig.host string` (`"in-process"` | `"sidecar"`; empty allowed, attribute then omitted).
  - `func instanceIdentity(getenv func(string) string) (instanceID, podName string)` — `podName` = `ATROPOS_POD_NAME`, else `HOSTNAME`; `instanceID` = `podName`, else `os.Hostname()`, else `"unknown"`.
  - `func newResource(cfg telemetryConfig) (*resource.Resource, error)` — `resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))` with `semconv.ServiceName`, `ServiceVersion`, `DeploymentEnvironmentName`, `ServiceInstanceID(instanceID)`, `K8SPodName(podName)` (only when non-empty), `attribute.String("atropos.host", cfg.host)` (only when non-empty). `resource.Default()` already folds in `OTEL_RESOURCE_ATTRIBUTES` / `OTEL_SERVICE_NAME`.
  - The TracerProvider gets `sdktrace.WithSampler` only when `cfg.sampler != nil`; otherwise the SDK reads `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` itself and defaults to `ParentBased(AlwaysSample)` (documented on `sdktrace.WithSampler`).

- [ ] **Step 1: Write the failing tests in `internal/instrument/telemetry_test.go`**

```go
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

func TestInitTelemetry_NoPodNameOmitsK8sAttribute(t *testing.T) {
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
```

Imports to add: `go.opentelemetry.io/otel/attribute`, `go.opentelemetry.io/otel/sdk/resource`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument/ -run 'TestInstanceIdentity|TestInitTelemetry_(Resource|NoPodName|Sampler|DefaultSampler)'`
Expected: `undefined: instanceIdentity` and `unknown field host in struct literal`, `FAIL`.

- [ ] **Step 3: Implement in `internal/instrument/telemetry.go`**

Add `host string` to `telemetryConfig` and `Host string` to `TelemetryConfig` (threaded in `InitTelemetry`); add `instanceIdentity` and `newResource` with the signatures above; replace the inline resource block with `res, err := newResource(cfg)`; make the sampler conditional:

```go
opts := []sdktrace.TracerProviderOption{
	sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(100*time.Millisecond)),
	sdktrace.WithResource(res),
}
if cfg.sampler != nil { // tests only; production reads OTEL_TRACES_SAMPLER(_ARG)
	opts = append(opts, sdktrace.WithSampler(cfg.sampler))
}
tp := sdktrace.NewTracerProvider(opts...)
```

Add a comment above the semconv import: `// The one semconv version for both hosts; the sidecar shares it through InitTelemetry.`

- [ ] **Step 4: Pass the host from both hosts**

`serve.go:137-142`: add `Host: instrument.HostInProcess,` to the `TelemetryConfig` literal. `internal/atroposd/daemon.go:70-75`: add `Host: instrument.HostSidecar,`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/instrument/ -run 'TestInstanceIdentity|TestInitTelemetry' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS` for all `TestInitTelemetry_*` and `TestInstanceIdentity`; `ok`.

- [ ] **Step 6: Full verification (plus the single-semconv check)**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/... && grep -rl 'otel/semconv' $(git ls-files '*.go')`
Expected: no gofmt output; all `ok`; exactly one line: `internal/instrument/telemetry.go`.

- [ ] **Step 7: Commit**

```bash
git add internal/instrument/telemetry.go internal/instrument/telemetry_test.go serve.go internal/atroposd/daemon.go
git commit -m "feat(telemetry): honour OTEL_TRACES_SAMPLER; stamp instance identity and atropos.host on the resource

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Logs as a signal — `LoggerProvider`, otelslog bridge, teed default logger

**Files:**
- Create: `internal/instrument/logging.go`, `internal/instrument/logging_test.go`
- Modify: `internal/instrument/telemetry.go` (BYO branch `:44-49`, provider build, `TelemetryConfig`, `InitTelemetry`), `internal/instrument/telemetry_test.go`
- Modify: `serve.go:68-75` (Config fields), `:121-124` (logger default), `:137-145` (init call), `:160-163`, `:194`, `:219` (shutdown uses)
- Modify: `internal/atroposd/daemon.go:57-60`, `:70-78`, `:110`, `:141`, `:163`, `:199`; `cmd/atroposd/main.go:32`, `:48`
- Modify: `integration_test.go:25`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 3's `initTelemetry`, `newResource`.
- Produces:

```go
// internal/instrument/telemetry.go
type Telemetry struct {
	Logger         *slog.Logger           // cfg.Logger, else NewLogger(os.Stdout, LoggerProvider)
	LoggerProvider otellog.LoggerProvider // the SDK provider, the BYO one, or the global as found
	Shutdown       func(context.Context) error
}
type TelemetryConfig struct {
	ServiceName, ServiceVersion, Environment, Host string
	TracerProvider oteltrace.TracerProvider // BYO: no exporters are built, for traces or logs
	LoggerProvider otellog.LoggerProvider   // BYO logs; registered globally when set
	Logger         *slog.Logger             // BYO logger; nil = the SDK default
}
func InitTelemetry(ctx context.Context, cfg TelemetryConfig) (*Telemetry, error)
func newLogExporter(ctx context.Context) (sdklog.Exporter, error) // applyExporterEnvFallback + autoexport.NewLogExporter

// internal/instrument/logging.go
const loggerScopeName = "git.ucsc.edu/microfaults/atropos-go"
func NewLogger(w io.Writer, lp otellog.LoggerProvider) *slog.Logger // tee(JSONHandler(w, Info), otelslog.NewHandler(loggerScopeName, otelslog.WithLoggerProvider(lp)))
type teeHandler struct{ handlers []slog.Handler }
func newTeeHandler(handlers ...slog.Handler) slog.Handler
func (h teeHandler) Enabled(ctx context.Context, l slog.Level) bool   // true if any handler is enabled
func (h teeHandler) Handle(ctx context.Context, r slog.Record) error  // every enabled handler, r.Clone() per handler, errors.Join
func (h teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler        // maps over handlers
func (h teeHandler) WithGroup(name string) slog.Handler              // maps over handlers
```

Behaviour of `initTelemetry`: SDK path (no BYO tracer provider) builds `newSpanExporter` + `newLogExporter`, `lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), sdklog.WithResource(res))`, registers it with `global.SetLoggerProvider` (`go.opentelemetry.io/otel/log/global`; at v1.46.0 the `otel` package has no logger globals), and `Shutdown` joins `tp.Shutdown` and `lp.Shutdown`. BYO tracer path: no exporter of any kind; `lp = cfg.loggerProvider` when set (registered globally), else `global.GetLoggerProvider()` untouched; `Shutdown` is a no-op. The stdlib's `slog.NewMultiHandler` (Go 1.26) is off limits on a `go 1.25.5` module (vet's stdversion check), hence the tee.

- [ ] **Step 1: Add the modules, pinned, in one command**

```bash
go get go.opentelemetry.io/otel/log@v0.22.0 go.opentelemetry.io/otel/sdk/log@v0.22.0 \
  go.opentelemetry.io/otel/log/logtest@v0.22.0 \
  go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc@v0.22.0 \
  go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp@v0.22.0 \
  go.opentelemetry.io/otel/exporters/stdout/stdoutlog@v0.22.0 \
  go.opentelemetry.io/contrib/bridges/otelslog@v0.20.1
```

(`otlplog*`/`stdoutlog` are autoexport's; pinning them keeps the set on v0.22.0 when tidy marks them indirect.)

- [ ] **Step 2: Write the failing tests**

`internal/instrument/logging_test.go`:

```go
// recordingLogExporter is the in-memory sdklog.Exporter the log tests assert on.
type recordingLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordingLogExporter) Export(_ context.Context, rs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rs {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (e *recordingLogExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }
func (e *recordingLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

func TestNewLogger_TeesToWriterAndBridgeWithTraceID(t *testing.T) {
	exp := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	var buf bytes.Buffer
	logger := NewLogger(&buf, lp)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	ctx, span := tp.Tracer("t").Start(context.Background(), "req")
	logger.InfoContext(ctx, "hello", "k", "v")
	span.End()

	if !strings.Contains(buf.String(), `"msg":"hello"`) || !strings.Contains(buf.String(), `"k":"v"`) {
		t.Fatalf("stdout JSON = %q", buf.String())
	}
	recs := exp.Records()
	if len(recs) != 1 {
		t.Fatalf("bridge records = %d, want 1", len(recs))
	}
	if recs[0].TraceID() != span.SpanContext().TraceID() || recs[0].SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("record trace/span = %s/%s, want %s", recs[0].TraceID(), recs[0].SpanID(), span.SpanContext().TraceID())
	}
	if recs[0].Body().AsString() != "hello" {
		t.Fatalf("body = %v", recs[0].Body())
	}
}

func TestTeeHandler_EnabledIfAnyEnabled(t *testing.T) {
	info := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
	errOnly := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})
	h := newTeeHandler(errOnly, info)
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("warn must be enabled when any handler accepts it")
	}
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("debug must be disabled when no handler accepts it")
	}
}

func TestTeeHandler_WithAttrsReachesEveryHandler(t *testing.T) {
	var a, b bytes.Buffer
	h := newTeeHandler(slog.NewJSONHandler(&a, nil), slog.NewJSONHandler(&b, nil)).WithAttrs([]slog.Attr{slog.String("svc", "x")})
	slog.New(h).Info("m")
	for _, buf := range []*bytes.Buffer{&a, &b} {
		if !strings.Contains(buf.String(), `"svc":"x"`) {
			t.Fatalf("missing attr in %q", buf.String())
		}
	}
}
```

Append to `internal/instrument/telemetry_test.go`:

```go
func TestInitTelemetry_BuildsLoggerProvider(t *testing.T) {
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
```

Existing `TestInitTelemetry_*` tests in this file change `shutdown, err := initTelemetry(...)` to `tel, err := initTelemetry(...)` and `shutdown(` to `tel.Shutdown(`.

Imports to add: `bytes`, `io`, `log/slog`, `strings`, `sync`, `otellog "go.opentelemetry.io/otel/log"`, `go.opentelemetry.io/otel/log/global`, `go.opentelemetry.io/otel/log/logtest`, `sdklog "go.opentelemetry.io/otel/sdk/log"`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument/ -run 'TestNewLogger|TestTeeHandler|TestInitTelemetry'`
Expected: `undefined: NewLogger`, `undefined: newTeeHandler`, `tel.Shutdown undefined`; `FAIL`.

- [ ] **Step 4: Implement `internal/instrument/logging.go` and the `Telemetry` return**

`logging.go` holds the signatures from Interfaces. In `telemetry.go`: `initTelemetry` returns `(*Telemetry, error)`; add `loggerProvider otellog.LoggerProvider` and `logger *slog.Logger` to `telemetryConfig`; add `newLogExporter`; build and register the `sdklog.LoggerProvider` on the SDK path; BYO behaviour as described in Interfaces; `Telemetry.Logger = cfg.logger` or `NewLogger(os.Stdout, lp)`. `InitTelemetry` threads `LoggerProvider`, `Logger`, `Host`.

- [ ] **Step 5: Consume `Telemetry` in both hosts and the integration test**

`serve.go`: add to `Config`, after `TracerProvider` (`:68-72`):

```go
// LoggerProvider, if set, receives the SDK's log records (and the decision
// events) instead of the OTLP log exporter autoexport would build. Ignored
// unless TracerProvider is also set: a host that owns one signal owns both.
LoggerProvider otellog.LoggerProvider
```

Change the `Logger` doc (`:74-75`) to: `// Logger receives SDK logs. Default: a JSON handler on stdout teed into the OTel logs bridge, so every *Context call carries trace_id and span_id.` Delete the `logger := cfg.Logger … slog.Default()` default (`:121-124`); after `InitTelemetry` (`:137-145`, now `tel, err := instrument.InitTelemetry(ctx, instrument.TelemetryConfig{…, TracerProvider: cfg.TracerProvider, LoggerProvider: cfg.LoggerProvider, Logger: cfg.Logger, Host: instrument.HostInProcess})`) set `logger := tel.Logger` and `otelShutdown := tel.Shutdown`; the rest of the function is unchanged.

`internal/atroposd/daemon.go`: `Run` keeps its signature; a nil `logger` means "the SDK default": before `InitTelemetry` use `boot := logger; if boot == nil { boot = slog.New(slog.NewJSONHandler(os.Stdout, nil)) }` for config errors (none are logged today, so this is only a guard for future lines), pass `Logger: logger` (nil allowed) in the `TelemetryConfig`, then `if logger == nil { logger = tel.Logger }` and `otelShutdown := tel.Shutdown`. `cmd/atroposd/main.go:48`: `atroposd.Run(ctx, cfg, nil)`; keep main's stderr text logger for the load-config error only. `integration_test.go:25`: `tel, err := …; shutdown := tel.Shutdown`.

- [ ] **Step 6: Tidy, then run the tests to verify they pass**

Run: `go mod tidy && grep '^go ' go.mod && go test -count=1 ./internal/instrument/ -run 'TestNewLogger|TestTeeHandler|TestInitTelemetry' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `go 1.25.5`; `--- PASS` for every listed test; `ok`.

- [ ] **Step 7: Full verification**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/... && grep '^go ' go.mod`
Expected: no gofmt output; all `ok` (`serve_test.go` and `daemon_test.go` keep passing: BYO tracer path, discard loggers); `go 1.25.5`.

- [ ] **Step 8: Changelog and commit**

Add under the Task 2 heading: logs are an OTel signal (`OTEL_LOGS_EXPORTER`), `Config.Logger` defaults to stdout JSON teed into the `otelslog` bridge, new `Config.LoggerProvider`.

```bash
git add go.mod go.sum internal/instrument/logging.go internal/instrument/logging_test.go internal/instrument/telemetry.go internal/instrument/telemetry_test.go serve.go internal/atroposd/daemon.go cmd/atroposd/main.go integration_test.go CHANGELOG.md
git commit -m "feat(telemetry): logs as a signal: LoggerProvider, otelslog bridge, teed default logger

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Request-scoped SDK logging (replace `log.Printf`)

**Files:**
- Modify: `internal/interceptor/interceptor.go:16-51` (option + field), `:60-75` (`New` default)
- Modify: `internal/interceptor/middleware.go:5` (drop `"log"`), `:37-40`, `:83-86`
- Modify: `grpc/interceptor.go:7`, `:18-26`
- Modify: `internal/instrument/core.go:113-117` (pass `interceptor.WithLogger(logger)`)
- Test: `internal/interceptor/interceptor_test.go`, `grpc/interceptor_test.go`

**Interfaces:**
- Consumes: Task 4's teed logger (any `*slog.Logger` works; the bridge is what attaches trace ids).
- Produces: `func WithLogger(l *slog.Logger) Option`; `func (i *Interceptor) Logger() *slog.Logger` (never nil; default `slog.Default()`).

Rule: every log call that has a request context uses `ErrorContext`/`WarnContext`/`InfoContext` with that context. The three sites are `middleware.go:39` (ingress check error), `middleware.go:85` (egress check error), `grpc/interceptor.go:21` (grpc check error). `decode.go:107` and `register.go:214` keep `slog.Default()` — they run at rule-apply time with no request context (known gap: they bypass the host logger; out of scope).

- [ ] **Step 1: Write the failing tests**

`internal/interceptor/interceptor_test.go`:

```go
// ctxCapturingHandler records the context and message of every record.
type ctxCapturingHandler struct {
	mu   sync.Mutex
	ctxs []context.Context
	msgs []string
}

func (h *ctxCapturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *ctxCapturingHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ctxs = append(h.ctxs, ctx)
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *ctxCapturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *ctxCapturingHandler) WithGroup(string) slog.Handler      { return h }

func sampledContext(t *testing.T) (context.Context, oteltrace.SpanContext) {
	t.Helper()
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: oteltrace.FlagsSampled,
	})
	return oteltrace.ContextWithSpanContext(context.Background(), sc), sc
}

func TestIngressMiddleware_CheckErrorLogsWithRequestContext(t *testing.T) {
	h := &ctxCapturingHandler{}
	eval := &mockEvaluator{decision: &evaluator.Decision{
		Fault: &inline.Latency{Delay: 0}, // invalid: Validate fails, Check returns an error
		Mode:  evaluator.Inline, Reason: "test",
	}}
	i := New(eval, trace.Noop(), WithLogger(slog.New(h)))
	ctx, sc := sampledContext(t)
	req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
	i.IngressMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(httptest.NewRecorder(), req)

	if len(h.msgs) != 1 || h.msgs[0] != "atropos: ingress check error" {
		t.Fatalf("messages = %q", h.msgs)
	}
	if got := oteltrace.SpanContextFromContext(h.ctxs[0]); got.TraceID() != sc.TraceID() {
		t.Fatalf("log context trace id = %s, want %s", got.TraceID(), sc.TraceID())
	}
}

func TestNew_LoggerDefaultsToSlogDefault(t *testing.T) {
	if New(nil, nil).Logger() != slog.Default() {
		t.Fatal("Logger() must fall back to slog.Default()")
	}
}
```

`grpc/interceptor_test.go` (same capturing handler pattern, package `atroposgrpc`): `TestUnaryServerInterceptor_CheckErrorLogsWithContext` builds `interceptor.New(evalReturningInvalidLatency, nil, interceptor.WithLogger(slog.New(h)))`, calls `UnaryServerInterceptor(i)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/svc/M"}, handler)` with the sampled ctx, and asserts the message `atropos: grpc check error` and the same trace id. (The evaluator fake: a type with `Evaluate` returning `&evaluator.Decision{Fault: &inline.Latency{Delay: 0}, Mode: evaluator.Inline}`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/interceptor/ ./grpc/ -run 'CheckErrorLogs|LoggerDefaults'`
Expected: `undefined: WithLogger`, `i.Logger undefined`; `FAIL`.

- [ ] **Step 3: Implement**

`interceptor.go`: field `logger *slog.Logger`; `WithLogger`; `Logger()`; `New` sets `slog.Default()` when nil. `middleware.go:39` → `i.logger.ErrorContext(r.Context(), "atropos: ingress check error", "error", err)`; `:85` → `i.logger.ErrorContext(r.Context(), "atropos: egress check error", "error", err)`; drop the `log` import. `grpc/interceptor.go:21` → `i.Logger().ErrorContext(ctx, "atropos: grpc check error", "error", err)`; drop `log`. `core.go:113-117` adds `interceptor.WithLogger(logger)`.

- [ ] **Step 4: Run the tests to verify they pass; confirm no `log.Printf` remains**

Run: `go test -count=1 ./internal/interceptor/ ./grpc/ -run 'CheckErrorLogs|LoggerDefaults' -v 2>&1 | grep -E '^(--- |ok|FAIL)'; grep -rn 'log\.Printf' $(git ls-files '*.go') | grep -v _test.go`
Expected: `--- PASS` ×3, `ok` ×2; the grep prints nothing.

- [ ] **Step 5: Full verification, commit**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`
Expected: no gofmt output; all `ok`.

```bash
git add internal/interceptor/interceptor.go internal/interceptor/middleware.go internal/interceptor/interceptor_test.go grpc/interceptor.go grpc/interceptor_test.go internal/instrument/core.go
git commit -m "refactor(interceptor): request-scoped slog logging replaces log.Printf

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Span topology — run ids, links to triggering and recent requests, join accounting

**Files:**
- Modify: `internal/trace/trace.go:17-29` (interfaces), `:41-46` (`Start`), `:48-85` (`otelSpan`); `internal/trace/noop.go:16-24`; `internal/trace/attrs.go:4-12` (fault attrs), `:57-65` (names)
- Create: `internal/interceptor/ring.go`, `internal/interceptor/ring_test.go`
- Modify: `internal/interceptor/registry.go:34-41`, `:53-104`, `:110-120`, `:124-138`; `internal/interceptor/registry_test.go` (append)
- Modify: `internal/interceptor/interceptor.go:88-99` (`CheckResult`, `Evaluate`), `:146-176` (`startDirect`), `:181-224` (`startRegistered`); `internal/interceptor/hook.go:44`
- Modify: `internal/interceptor/interceptor_test.go:343-377` (fakes gain the new methods; append tests)

**Interfaces:**
- Consumes: Task 5's interceptor shape.
- Produces:

```go
// internal/trace/trace.go
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, Span)
	StartLinked(ctx context.Context, name string, links []oteltrace.Link, attrs ...attribute.KeyValue) (context.Context, Span)
}
type Span interface {
	SetAttributes(attrs ...attribute.KeyValue)
	AddEvent(name string, attrs ...attribute.KeyValue)
	AddLink(l oteltrace.Link)
	SpanContext() oteltrace.SpanContext
	RecordResult(r fault.Result)
	EndWithError(err error)
	End()
}
// internal/trace/attrs.go
AttrFaultID = "atropos.fault.id"; AttrFaultJoined = "atropos.fault.joined"; AttrLinkKind = "atropos.link.kind"
LinkKindTrigger = "trigger"; LinkKindRecent = "recent"; LinkKindFault = "fault"
// internal/interceptor/ring.go
const ringSize = 256; const ringLinks = 16
type recentRequest struct { Trace oteltrace.TraceID; Span oteltrace.SpanID; Labels map[string]string; At time.Time }
type requestRing struct { mu sync.Mutex; buf []recentRequest; next, n int }
func newRequestRing(size int) *requestRing
func (r *requestRing) Record(sc oteltrace.SpanContext, labels map[string]string) // no-op unless sc.IsValid() && sc.IsSampled(); labels cloned
func (r *requestRing) Links(max int, exclude oteltrace.SpanID) []oteltrace.Link  // newest first, AttrLinkKind=recent, skips exclude
func (r *requestRing) TraceIDs() []string                                       // newest first, hex, every entry
// internal/interceptor/registry.go
type FaultRun struct { ID, Key string; Handle *fault.Handle; Span trace.Span; joined atomic.Int64 }
func (r *FaultRun) Join() int64 // joined++ and SetAttributes(AttrFaultJoined, n) on Span when non-nil; returns n
func NewRunID() string          // 8 random bytes, hex (crypto/rand); panics only if the OS entropy source fails
func (r *FaultRegistry) StartRun(key string, policy evaluator.StartPolicy,
	startFn func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error)) (run *FaultRun, joined bool, err error)
// StartOrJoin keeps its signature and semantics (nil handle + deduped=true on a join) and delegates to StartRun with a nil Span.
// internal/interceptor/interceptor.go
type CheckResult struct { Handle *fault.Handle; Decision *evaluator.Decision; RunID string; Joined bool }
```

Behaviour:
- `Evaluate` first calls `i.recent.Record(oteltrace.SpanContextFromContext(ctx), req.Labels)`, then evaluates. `New` builds `recent: newRequestRing(ringSize)`.
- `startDirect`: `runID := NewRunID()`; stamp `oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(trace.AttrFaultID, runID))` on the request span before starting the child fault span; the fault span gets `AttrFaultID` too; result carries `RunID`.
- `startRegistered`: `run, joined, err := i.registry.StartRun(key, policy, func(regCtx, runID) {...})`; inside, `links := append([]oteltrace.Link{oteltrace.LinkFromContext(ctx, attribute.String(trace.AttrLinkKind, trace.LinkKindTrigger))}, i.recent.Links(ringLinks, oteltrace.SpanContextFromContext(ctx).SpanID())...)` and `i.tracer.StartLinked(regCtx, trace.SpanFaultInject, links, attrs..., attribute.String(trace.AttrFaultID, runID))`; an invalid trigger context is dropped by the SDK (`WithLinks` ignores invalid span contexts). After a start the request span is stamped with `AttrFaultID`. On `joined`: request span gets `AttrFaultID` and `AddLink(oteltrace.Link{SpanContext: run.Span.SpanContext(), Attributes: []attribute.KeyValue{attribute.String(trace.AttrLinkKind, trace.LinkKindFault)}})` (only when `run.Span != nil`), `run.Join()`, and the result is `CheckResult{Decision: &d, RunID: run.ID, Joined: true}` (Handle nil, so callers never wait on it).
- `hook.go:44`: the injected/skipped event condition becomes `cr.Handle != nil || cr.Joined`.
- `otelSpan.SpanContext()` returns the wrapped span's; `noopSpan.SpanContext()` returns the zero value; `noopTracer.StartLinked` returns `ctx, &noopSpan{}`.
- Registry internals: `active map[string][]*FaultRun`; `Stop` stops `run.Handle`; `Close` unchanged apart from the type.

- [ ] **Step 1: Write the failing tests**

`internal/interceptor/ring_test.go`:

```go
func sampledSC(i byte) oteltrace.SpanContext {
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{i, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, SpanID: oteltrace.SpanID{i, 2, 2, 2, 2, 2, 2, 2}, TraceFlags: oteltrace.FlagsSampled,
	})
}

func TestRequestRing_WrapsAndOrdersNewestFirst(t *testing.T) {
	r := newRequestRing(4)
	for i := byte(1); i <= 6; i++ {
		r.Record(sampledSC(i), map[string]string{"n": string(rune('0' + i))})
	}
	ids := r.TraceIDs()
	if len(ids) != 4 || ids[0] != sampledSC(6).TraceID().String() || ids[3] != sampledSC(3).TraceID().String() {
		t.Fatalf("ids = %v", ids)
	}
}

// Review Focus 2: never link to a trace that was never stored.
func TestRequestRing_IgnoresUnsampledAndInvalid(t *testing.T) {
	r := newRequestRing(4)
	r.Record(oteltrace.SpanContext{}, nil)
	r.Record(sampledSC(1).WithTraceFlags(0), nil)
	if got := r.TraceIDs(); len(got) != 0 {
		t.Fatalf("ids = %v, want none", got)
	}
}

func TestRequestRing_LinksCapAndExclude(t *testing.T) {
	r := newRequestRing(8)
	for i := byte(1); i <= 5; i++ {
		r.Record(sampledSC(i), nil)
	}
	links := r.Links(3, sampledSC(5).SpanID())
	if len(links) != 3 || links[0].SpanContext.SpanID() != sampledSC(4).SpanID() {
		t.Fatalf("links = %+v", links)
	}
	for _, l := range links {
		if len(l.Attributes) != 1 || l.Attributes[0].Key != trace.AttrLinkKind || l.Attributes[0].Value.AsString() != trace.LinkKindRecent {
			t.Fatalf("link attrs = %v", l.Attributes)
		}
	}
}

func TestRequestRing_ConcurrentRecord(t *testing.T) {
	r := newRequestRing(ringSize)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g byte) {
			defer wg.Done()
			for k := 0; k < 100; k++ {
				r.Record(sampledSC(g+1), map[string]string{"k": "v"})
			}
		}(byte(g))
	}
	wg.Wait()
	if len(r.TraceIDs()) != ringSize {
		t.Fatalf("len = %d", len(r.TraceIDs()))
	}
}
```

`internal/interceptor/registry_test.go` (append):

```go
func TestRegistry_StartRun_JoinReturnsRunAndCounts(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	rec := &recordingSpan{}
	startFn := func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error) {
		if len(runID) != 16 {
			t.Fatalf("runID = %q, want 16 hex chars", runID)
		}
		h, err := newStubFault(300 * time.Millisecond).Start(ctx)
		return h, rec, err
	}
	run, joined, err := r.StartRun("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil || joined || run == nil || run.Span != trace.Span(rec) {
		t.Fatalf("first: run=%v joined=%v err=%v", run, joined, err)
	}
	run2, joined2, err := r.StartRun("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil || !joined2 || run2 != run {
		t.Fatalf("second: run2=%p run=%p joined=%v err=%v", run2, run, joined2, err)
	}
	if n := run2.Join(); n != 1 {
		t.Fatalf("Join = %d, want 1", n)
	}
	if !hasAttr(rec.attrs, trace.AttrFaultJoined, "1") {
		t.Fatalf("fault span attrs = %v, want atropos.fault.joined=1", rec.attrs)
	}
}

// Review Focus 5: the StartOrJoin wrapper leaves Span nil; Join must not panic.
func TestRegistry_StartRun_JoinWithNilSpan(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	startFn := func(ctx context.Context) (*fault.Handle, error) { return newStubFault(200 * time.Millisecond).Start(ctx) }
	if _, _, err := r.StartOrJoin("rule-b", evaluator.DeduplicateByRule, startFn); err != nil {
		t.Fatal(err)
	}
	run, joined, err := r.StartRun("rule-b", evaluator.DeduplicateByRule, func(ctx context.Context, _ string) (*fault.Handle, trace.Span, error) {
		h, err := newStubFault(time.Millisecond).Start(ctx)
		return h, nil, err
	})
	if err != nil || !joined || run.Span != nil {
		t.Fatalf("run=%+v joined=%v err=%v", run, joined, err)
	}
	if n := run.Join(); n != 1 {
		t.Fatalf("Join = %d", n)
	}
}
```

`hasAttr(attrs []attribute.KeyValue, key, value string) bool` is a small test helper (string form of the value via `Value.Emit()`); add it to `interceptor_test.go`.

`internal/interceptor/interceptor_test.go`: extend the fakes at `:343-377`: `recordingTracer.StartLinked` records `links` into a new `links []oteltrace.Link` field and otherwise behaves like `Start`; `recordingSpan.AddLink` appends to `links`; `recordingSpan.SpanContext()` returns `oteltrace.SpanContext{}`. Then append:

```go
// installRecordingProvider makes the global tracer provider export
// synchronously to an in-memory exporter for the test's lifetime.
func installRecordingProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample())))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return exp
}

func findSpan(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no span %q in %d spans", name, len(spans))
	return tracetest.SpanStub{}
}

func linksOfKind(links []sdktrace.Link, kind string) []sdktrace.Link {
	var out []sdktrace.Link
	for _, l := range links {
		for _, a := range l.Attributes {
			if a.Key == trace.AttrLinkKind && a.Value.AsString() == kind {
				out = append(out, l)
			}
		}
	}
	return out
}

func backgroundDecision(name string) *evaluator.Decision {
	return &evaluator.Decision{Name: name, Fault: newStubFault(60 * time.Millisecond), Reason: "test", Mode: evaluator.Background}
}

func TestStartFault_BackgroundSpanLinksTriggerAndRecent(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	tr := otel.Tracer("test")

	var recent []oteltrace.SpanContext
	for n := 0; n < 3; n++ { // three earlier requests, evaluated with no fault
		ctx, s := tr.Start(context.Background(), "earlier")
		i.Evaluate(ctx, evaluator.Request{Point: evaluator.Ingress, Labels: map[string]string{"n": "x"}})
		recent = append(recent, s.SpanContext())
		s.End()
	}
	ctx, reqSpan := tr.Start(context.Background(), "trigger")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil || cr.RunID == "" || cr.Joined {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	reqSpan.End()

	spans := exp.GetSpans()
	fs := findSpan(t, spans, trace.SpanFaultInject)
	if !hasAttr(fs.Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatalf("fault span attrs = %v, want %s=%s", fs.Attributes, trace.AttrFaultID, cr.RunID)
	}
	if fs.Parent.IsValid() {
		t.Fatal("background fault span must stay a root (detached from the request)")
	}
	trig := linksOfKind(fs.Links, trace.LinkKindTrigger)
	if len(trig) != 1 || trig[0].SpanContext.SpanID() != reqSpan.SpanContext().SpanID() {
		t.Fatalf("trigger links = %+v", trig)
	}
	rl := linksOfKind(fs.Links, trace.LinkKindRecent)
	if len(rl) != 3 {
		t.Fatalf("recent links = %d, want 3 (trigger excluded)", len(rl))
	}
	if rl[0].SpanContext.SpanID() != recent[2].SpanID() {
		t.Fatal("recent links must be newest first")
	}
	rs := findSpan(t, spans, "trigger")
	if !hasAttr(rs.Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatalf("request span must carry atropos.fault.id, got %v", rs.Attributes)
	}
}

func TestStartFault_JoinLinksRequestToFaultSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	tr := otel.Tracer("test")

	ctx1, s1 := tr.Start(context.Background(), "first")
	first, err := i.Check(ctx1, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || first.Handle == nil {
		t.Fatalf("first: %+v %v", first, err)
	}
	s1.End()
	ctx2, s2 := tr.Start(context.Background(), "joiner")
	second, err := i.Check(ctx2, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || !second.Joined || second.Handle != nil || second.RunID != first.RunID {
		t.Fatalf("second: %+v %v (first run %s)", second, err, first.RunID)
	}
	s2.End()
	<-first.Handle.Done()

	spans := exp.GetSpans()
	fs := findSpan(t, spans, trace.SpanFaultInject)
	js := findSpan(t, spans, "joiner")
	fl := linksOfKind(js.Links, trace.LinkKindFault)
	if len(fl) != 1 || fl[0].SpanContext.SpanID() != fs.SpanContext.SpanID() {
		t.Fatalf("joiner links = %+v, want one to the fault span", js.Links)
	}
	if !hasAttr(js.Attributes, trace.AttrFaultID, first.RunID) || !hasAttr(fs.Attributes, trace.AttrFaultJoined, "1") {
		t.Fatalf("joiner attrs %v / fault attrs %v", js.Attributes, fs.Attributes)
	}
}

// Review Focus 5: Core.Arm starts from context.Background().
func TestStartFault_ArmWithoutRequestSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	cr, err := i.StartFault(context.Background(), evaluator.Request{Point: evaluator.Egress, Labels: map[string]string{}}, backgroundDecision("bg"))
	if err != nil || cr.Handle == nil {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	fs := findSpan(t, exp.GetSpans(), trace.SpanFaultInject)
	if len(linksOfKind(fs.Links, trace.LinkKindTrigger)) != 0 {
		t.Fatalf("no trigger link expected without a request span, got %+v", fs.Links)
	}
}

func TestStartFault_InlineStampsRunIDOnRequestSpan(t *testing.T) {
	exp := installRecordingProvider(t)
	i := New(&mockEvaluator{decision: &evaluator.Decision{Fault: &inline.Latency{Delay: 5 * time.Millisecond}, Mode: evaluator.Inline, Reason: "test"}}, trace.NewOTelTracer())
	ctx, s := otel.Tracer("test").Start(context.Background(), "req")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.RunID == "" {
		t.Fatalf("cr=%+v err=%v", cr, err)
	}
	<-cr.Handle.Done()
	s.End()
	spans := exp.GetSpans()
	if !hasAttr(findSpan(t, spans, "req").Attributes, trace.AttrFaultID, cr.RunID) || !hasAttr(findSpan(t, spans, trace.SpanFaultInject).Attributes, trace.AttrFaultID, cr.RunID) {
		t.Fatal("both the request span and the inline fault span must carry atropos.fault.id")
	}
}
```

Imports to add in the interceptor tests: `sync`, `go.opentelemetry.io/otel`, `sdktrace "go.opentelemetry.io/otel/sdk/trace"`, `go.opentelemetry.io/otel/sdk/trace/tracetest`, `oteltrace "go.opentelemetry.io/otel/trace"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/interceptor/ -run 'TestRequestRing|TestRegistry_StartRun|TestStartFault'`
Expected: `undefined: newRequestRing`, `r.StartRun undefined`, missing methods on the fakes; `FAIL`.

- [ ] **Step 3: Implement**

`internal/trace`: extend the interfaces and both implementations (`OTelTracer.StartLinked` passes `oteltrace.WithLinks(links...)` and `WithAttributes`); add the attribute and link-kind constants. `ring.go`: the ring per Interfaces (a fixed slice, `next`/`n`, mutex; `Record` clones labels with `maps.Clone`). `registry.go`: `FaultRun`, `NewRunID`, `StartRun` (same lock discipline as today's `StartOrJoin`; the completion callback removes the run), `StartOrJoin` wrapper. `interceptor.go`: `recent` field, `Evaluate` records, `CheckResult` fields, `startDirect`/`startRegistered` per Behaviour. `hook.go:44`: `if cr.Handle != nil || cr.Joined`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/interceptor/ -run 'TestRequestRing|TestRegistry|TestStartFault|TestCheck|TestHook' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: every listed test `--- PASS`; `ok`.

- [ ] **Step 5: Full verification, commit**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`
Expected: no gofmt output; all `ok` (the `-race` run covers the ring and the registry).

```bash
git add internal/trace internal/interceptor
git commit -m "feat(trace): fault run ids, span links to triggering and recent requests, join accounting

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Decision events through the OTel Logs API

**Files:**
- Create: `internal/trace/decision.go`, `internal/trace/decision_test.go`
- Modify: `internal/trace/attrs.go` (decision attribute and event names)
- Modify: `internal/interceptor/interceptor.go` (`startDirect`, `startRegistered`: emit on start and on join), `internal/interceptor/cachebox.go:42-66` (emit once the key strategy is resolved)
- Modify: `internal/interceptor/interceptor_test.go`, `internal/interceptor/cachebox_test.go` (append)

**Interfaces:**
- Consumes: Task 6's `CheckResult.RunID`/`Joined`, `FaultRun.ID`, `requestRing.TraceIDs()`; Task 4's global `LoggerProvider`.
- Produces:

```go
// internal/trace/attrs.go
EventDecision = "atropos.decision"
AttrRuleName = "atropos.rule.name"; AttrReason = "atropos.reason"; AttrInjectionPoint = "atropos.injection_point"
AttrExperimentID = "atropos.experiment.id"; AttrPhaseID = "atropos.phase.id"; AttrRecentTraceIDs = "atropos.recent.trace_ids"
// internal/trace/decision.go
const DecisionLoggerName = "git.ucsc.edu/microfaults/atropos-go/decision"
type Decision struct {
	RuleName, Reason, FaultID, InjectionPoint, ExperimentID, PhaseID string
	Action         string   // "fault.start" | "fault.join" | "cachebox.<CacheBoxAction.String()>"
	RecentTraceIDs []string // newest first; omitted from the record when empty
}
func EmitDecision(ctx context.Context, d Decision)
```

`EmitDecision` uses a package-level `decisionLogger = global.Logger(DecisionLoggerName)` (the global delegates to whatever provider is registered later, so it is safe at init) and emits one `otellog.Record`: `SetTimestamp(time.Now())`, `SetEventName(EventDecision)`, `SetSeverity(otellog.SeverityInfo)`, `SetSeverityText("INFO")`, `SetBody(attribute.StringValue("atropos decision: " + d.Action))`, `AddAttributes(attribute.String(AttrRuleName, d.RuleName), attribute.String(AttrReason, d.Reason), attribute.String(AttrFaultID, d.FaultID), attribute.String(AttrInjectionPoint, d.InjectionPoint), attribute.String(AttrExperimentID, d.ExperimentID), attribute.String(AttrPhaseID, d.PhaseID), attribute.String("atropos.action", d.Action))` plus `attribute.StringSlice(AttrRecentTraceIDs, d.RecentTraceIDs)` when non-empty. The record is emitted with the request `ctx`, which is how the SDK attaches `trace_id`/`span_id` (`sdk/log` reads `trace.SpanContextFromContext(ctx)` at `Emit`). Experiment and phase come from `decision.CacheBoxContext` when present (Task 8 adds the baggage fallback).

Emission points: `startDirect` after a successful `Start` (`Action: "fault.start"`, `FaultID: runID`); `startRegistered` after a successful start (`fault.start`) and on join (`fault.join`, `FaultID: run.ID`); `handleCacheBox` right after `span.SetAttributes(AttrCacheBoxKey…)` (`Action: "cachebox." + decision.CacheBox.String()`, `FaultID: ""`). `RecentTraceIDs` = `i.recent.TraceIDs()` on every emission. `Core.Arm` goes through `StartFault`, so armed faults emit too (`InjectionPoint` from the request point).

- [ ] **Step 1: Write the failing tests**

`internal/trace/decision_test.go` (package `trace`):

```go
func TestEmitDecision_RecordShape(t *testing.T) {
	rec := logtest.NewRecorder()
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(rec)
	t.Cleanup(func() { global.SetLoggerProvider(prev) })

	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID{9}, SpanID: oteltrace.SpanID{8}, TraceFlags: oteltrace.FlagsSampled})
	ctx := oteltrace.ContextWithSpanContext(context.Background(), sc)
	EmitDecision(ctx, Decision{RuleName: "r1", Reason: "compiled", FaultID: "abc", InjectionPoint: "ingress",
		ExperimentID: "exp-1", PhaseID: "ph-1", Action: "fault.start", RecentTraceIDs: []string{"t1", "t2"}})

	recs := rec.Result()[logtest.Scope{Name: DecisionLoggerName}]
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1 (%v)", len(recs), rec.Result())
	}
	r := recs[0]
	if r.EventName != EventDecision || r.Severity != otellog.SeverityInfo {
		t.Fatalf("event=%q severity=%v", r.EventName, r.Severity)
	}
	if oteltrace.SpanContextFromContext(r.Context).TraceID() != sc.TraceID() {
		t.Fatal("record context must carry the request span context")
	}
	want := map[attribute.Key]string{AttrRuleName: "r1", AttrReason: "compiled", AttrFaultID: "abc", AttrInjectionPoint: "ingress", AttrExperimentID: "exp-1", AttrPhaseID: "ph-1"}
	for _, a := range r.Attributes {
		if w, ok := want[a.Key]; ok && a.Value.AsString() == w {
			delete(want, a.Key)
		}
		if a.Key == AttrRecentTraceIDs && len(a.Value.AsStringSlice()) != 2 {
			t.Fatalf("recent trace ids = %v", a.Value.AsStringSlice())
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing attributes: %v", want)
	}
}

// The SDK path: trace ids land on the exported record.
func TestEmitDecision_TraceIDThroughSDK(t *testing.T) {
	exp := &recordingLogExporter{} // same shape as the instrument test helper; duplicated here (different package)
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(lp)
	t.Cleanup(func() { global.SetLoggerProvider(prev); _ = lp.Shutdown(context.Background()) })

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	ctx, span := tp.Tracer("t").Start(context.Background(), "req")
	EmitDecision(ctx, Decision{RuleName: "r", Action: "fault.start"})
	span.End()

	recs := exp.Records()
	if len(recs) != 1 || recs[0].TraceID() != span.SpanContext().TraceID() || recs[0].EventName() != EventDecision {
		t.Fatalf("records = %+v", recs)
	}
}
```

`internal/interceptor/interceptor_test.go` (append; uses the Task 6 helpers):

```go
func installDecisionRecorder(t *testing.T) *logtest.Recorder {
	t.Helper()
	rec := logtest.NewRecorder()
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(rec)
	t.Cleanup(func() { global.SetLoggerProvider(prev) })
	return rec
}

func decisionRecords(rec *logtest.Recorder) []logtest.Record {
	return rec.Result()[logtest.Scope{Name: trace.DecisionLoggerName}]
}

func attrString(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

func TestStartFault_EmitsDecisionEvent(t *testing.T) {
	installRecordingProvider(t)
	rec := installDecisionRecorder(t)
	i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "compiled"}}, trace.NewOTelTracer())
	ctx, span := otel.Tracer("test").Start(context.Background(), "req")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil {
		t.Fatal(err)
	}
	<-cr.Handle.Done()
	span.End()

	recs := decisionRecords(rec)
	if len(recs) != 1 || recs[0].EventName != trace.EventDecision {
		t.Fatalf("records = %+v", recs)
	}
	a := recs[0].Attributes
	if attrString(a, trace.AttrRuleName) != "lat" || attrString(a, trace.AttrReason) != "compiled" || attrString(a, trace.AttrFaultID) != cr.RunID || attrString(a, trace.AttrInjectionPoint) != "ingress" || attrString(a, "atropos.action") != "fault.start" {
		t.Fatalf("attributes = %v", a)
	}
	if oteltrace.SpanContextFromContext(recs[0].Context).TraceID() != span.SpanContext().TraceID() {
		t.Fatal("decision event must be emitted with the request context")
	}
}

func TestStartFault_JoinEmitsDecisionEvent(t *testing.T) {
	installRecordingProvider(t)
	rec := installDecisionRecorder(t)
	reg := NewFaultRegistry()
	defer reg.Close()
	i := New(&mockEvaluator{decision: backgroundDecision("bg")}, trace.NewOTelTracer(), WithRegistry(reg))
	first, _ := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	second, _ := i.Check(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	<-first.Handle.Done()
	recs := decisionRecords(rec)
	if len(recs) != 2 || attrString(recs[1].Attributes, "atropos.action") != "fault.join" || attrString(recs[1].Attributes, trace.AttrFaultID) != second.RunID {
		t.Fatalf("records = %+v", recs)
	}
}
```

`internal/interceptor/cachebox_test.go` (append; `newTestInterceptor`/`cacheBoxRuleWithPhase` exist at `:43-80`):

```go
func TestHandleCacheBox_EmitsDecisionEventWithExperiment(t *testing.T) {
	rec := installDecisionRecorder(t)
	i, _ := newTestInterceptor(t, cacheBoxRuleWithPhase("dep", evaluator.CacheBoxPassthrough, "exp-1", "ph-1"))
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	resp, err := i.EgressTransport(base).RoundTrip(httptest.NewRequest("GET", "http://dep/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	recs := decisionRecords(rec)
	if len(recs) != 1 {
		t.Fatalf("records = %d", len(recs))
	}
	a := recs[0].Attributes
	if attrString(a, "atropos.action") != "cachebox.passthrough" || attrString(a, trace.AttrExperimentID) != "exp-1" || attrString(a, trace.AttrPhaseID) != "ph-1" || attrString(a, trace.AttrInjectionPoint) != "egress" {
		t.Fatalf("attributes = %v", a)
	}
}
```

Imports to add: `go.opentelemetry.io/otel/attribute` (already in interceptor tests), `otellog "go.opentelemetry.io/otel/log"`, `go.opentelemetry.io/otel/log/global`, `go.opentelemetry.io/otel/log/logtest`, `sdklog "go.opentelemetry.io/otel/sdk/log"`, `sdktrace`, `oteltrace`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/trace/ ./internal/interceptor/ -run 'TestEmitDecision|EmitsDecisionEvent'`
Expected: `undefined: EmitDecision`, `undefined: trace.DecisionLoggerName`; `FAIL`.

- [ ] **Step 3: Implement `internal/trace/decision.go`, the attribute names, and the three emission points**

Per Interfaces and the emission list above. In `startRegistered` the start-path emission happens after `StartRun` returns (outside the registry lock) so a slow log processor never serializes fault starts.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/trace/ ./internal/interceptor/ -run 'TestEmitDecision|EmitsDecisionEvent' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS` ×5; `ok` ×2.

- [ ] **Step 5: Full verification, commit**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`
Expected: no gofmt output; all `ok`.

```bash
git add internal/trace internal/interceptor
git commit -m "feat(trace): emit atropos.decision log events for every applied decision

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Experiment baggage on egress through an allowlisted propagator

**Files:**
- Create: `internal/trace/baggage.go`, `internal/trace/baggage_test.go`
- Modify: `internal/instrument/telemetry.go:39-42` (propagator), `TelemetryConfig` (+`PropagateBaggage []string`)
- Modify: `serve.go` (`Config.PropagateBaggage []string`, threaded into `TelemetryConfig`), `internal/atroposd/config.go:62-96` (+`PropagateBaggage []string \`yaml:"propagate_baggage"\``), `:126-145` (`ATROPOSD_PROPAGATE_BAGGAGE`, comma-separated), `:105-110` (doc), `internal/atroposd/daemon.go` (thread it)
- Modify: `internal/interceptor/middleware.go:25` (delete `workflowBaggageKey`, use `trace.BaggageWorkflow` at `:146`), `:61-98` (inject members at egress), `internal/interceptor/interceptor.go` (decision event experiment fallback)
- Modify: `internal/instrument/middleware_test.go` or `internal/interceptor/cachebox_test.go` (append egress tests), `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 7's `EmitDecision`; Task 2's propagator registration site.
- Produces:

```go
// internal/trace/baggage.go
const (
	BaggageWorkflow     = "atropos.workflow"
	BaggageExperimentID = "atropos.experiment_id"
	BaggagePhaseID      = "atropos.phase_id"
)
func DefaultBaggageAllowlist() []string // the three keys above
// BaggagePropagator is propagation.Baggage whose Inject writes only allowlisted members.
type BaggagePropagator struct { allow map[string]struct{}; inner propagation.Baggage }
func NewBaggagePropagator(extra ...string) BaggagePropagator
func (p BaggagePropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier)
func (p BaggagePropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context // = inner.Extract
func (p BaggagePropagator) Fields() []string                                                               // = inner.Fields
func WithExperimentBaggage(ctx context.Context, experimentID, phaseID string) context.Context // sets/overwrites the two members
func ExperimentFromContext(ctx context.Context) (experimentID, phaseID string)                 // reads them back
```

Behaviour:
- `initTelemetry` registers `propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, trace.NewBaggagePropagator(cfg.propagateBaggage...))`. `Inject` builds a new `baggage.Baggage` from the context's members whose keys are allowlisted and delegates; members stay in the context in full, so an app can still read everything with `baggage.FromContext`.
- Interceptor egress (`middleware.go:71` onward): when `decision != nil && decision.CacheBoxContext != nil && decision.CacheBoxContext.ExperimentID != ""`: `ctx := trace.WithExperimentBaggage(r.Context(), cbCtx.ExperimentID, cbCtx.PhaseID)`; `r = r.Clone(ctx)` (RoundTripper contract: never mutate the caller's request); `otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(r.Header))`. The otelhttp transport already injected `traceparent`/`baggage` from the pre-decision context one layer up; re-injecting here rewrites `baggage` with the members added, and `traceparent` with the same client span.
- Sidecar: `newIngressHandler` (`internal/proxy/ingress.go:26`) and the egress listener already inject through the global propagator, so the allowlist applies to proxied traffic without changes.
- Decision events: `ExperimentID`/`PhaseID` fall back to `trace.ExperimentFromContext(ctx)` when the decision has no `CacheBoxContext`.

- [ ] **Step 1: Write the failing tests**

`internal/trace/baggage_test.go`:

```go
// Review Focus 3.
func TestBaggagePropagator_FiltersForeignMembers(t *testing.T) {
	ctx := WithExperimentBaggage(context.Background(), "exp-1", "ph-1")
	m, _ := baggage.NewMember("tenant", "acme")
	b, _ := baggage.FromContext(ctx).SetMember(m)
	ctx = baggage.ContextWithBaggage(ctx, b)

	h := http.Header{}
	NewBaggagePropagator().Inject(ctx, propagation.HeaderCarrier(h))
	got := h.Get("baggage")
	for _, want := range []string{"atropos.experiment_id=exp-1", "atropos.phase_id=ph-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("baggage %q lacks %q", got, want)
		}
	}
	if strings.Contains(got, "tenant") {
		t.Fatalf("foreign member leaked: %q", got)
	}

	h = http.Header{}
	NewBaggagePropagator("tenant").Inject(ctx, propagation.HeaderCarrier(h))
	if !strings.Contains(h.Get("baggage"), "tenant=acme") {
		t.Fatalf("allowlisted member missing: %q", h.Get("baggage"))
	}
}

func TestBaggagePropagator_ExtractIsUnfiltered(t *testing.T) {
	h := http.Header{"Baggage": {"tenant=acme,atropos.workflow=checkout"}}
	ctx := NewBaggagePropagator().Extract(context.Background(), propagation.HeaderCarrier(h))
	if baggage.FromContext(ctx).Member("tenant").Value() != "acme" {
		t.Fatal("extract must keep every member available in-process")
	}
}

func TestExperimentFromContext_RoundTrip(t *testing.T) {
	e, p := ExperimentFromContext(WithExperimentBaggage(context.Background(), "exp-2", "ph-9"))
	if e != "exp-2" || p != "ph-9" {
		t.Fatalf("got %q/%q", e, p)
	}
	if e, p := ExperimentFromContext(context.Background()); e != "" || p != "" {
		t.Fatalf("empty context yielded %q/%q", e, p)
	}
}
```

`internal/interceptor/cachebox_test.go` (append):

```go
func installAllowlistPropagator(t *testing.T, extra ...string) {
	t.Helper()
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, trace.NewBaggagePropagator(extra...)))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
}

func TestEgressTransport_InjectsExperimentBaggage(t *testing.T) {
	installAllowlistPropagator(t)
	i, _ := newTestInterceptor(t, cacheBoxRuleWithPhase("dep", evaluator.CacheBoxPassthrough, "exp-1", "ph-1"))
	var seen string
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.Header.Get("baggage")
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	req := httptest.NewRequest("GET", "http://dep/x", nil)
	resp, err := i.EgressTransport(base).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !strings.Contains(seen, "atropos.experiment_id=exp-1") || !strings.Contains(seen, "atropos.phase_id=ph-1") {
		t.Fatalf("upstream baggage = %q", seen)
	}
	if req.Header.Get("baggage") != "" {
		t.Fatal("the caller's request must not be mutated")
	}
}

// Review Focus 3, end to end through the transport.
func TestEgressTransport_ForeignBaggageDropped(t *testing.T) {
	installAllowlistPropagator(t)
	i, _ := newTestInterceptor(t, cacheBoxRuleWithPhase("dep", evaluator.CacheBoxPassthrough, "exp-1", "ph-1"))
	var seen string
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.Header.Get("baggage")
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	m, _ := baggage.NewMember("tenant", "acme")
	b, _ := baggage.New(m)
	req := httptest.NewRequest("GET", "http://dep/x", nil).WithContext(baggage.ContextWithBaggage(context.Background(), b))
	resp, err := i.EgressTransport(base).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if strings.Contains(seen, "tenant") || !strings.Contains(seen, "atropos.experiment_id=exp-1") {
		t.Fatalf("upstream baggage = %q", seen)
	}
}

func TestEgressTransport_FaultDecisionEventFallsBackToBaggage(t *testing.T) {
	rec := installDecisionRecorder(t)
	i := New(&mockEvaluator{decision: &evaluator.Decision{Name: "lat", Fault: &inline.Latency{Delay: time.Millisecond}, Mode: evaluator.Inline, Reason: "compiled"}}, trace.Noop())
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: http.NoBody}, nil
	})
	req := httptest.NewRequest("GET", "http://dep/x", nil).WithContext(trace.WithExperimentBaggage(context.Background(), "exp-7", "ph-3"))
	if _, err := i.EgressTransport(base).RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	recs := decisionRecords(rec)
	if len(recs) != 1 || attrString(recs[0].Attributes, trace.AttrExperimentID) != "exp-7" || attrString(recs[0].Attributes, trace.AttrPhaseID) != "ph-3" {
		t.Fatalf("records = %+v", recs)
	}
}
```

`roundTripperFunc` exists in `internal/interceptor/middleware.go:122`. Imports: `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/baggage`, `go.opentelemetry.io/otel/propagation`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/trace/ ./internal/interceptor/ -run 'TestBaggagePropagator|TestExperimentFromContext|TestEgressTransport_(Injects|Foreign|FaultDecision)'`
Expected: `undefined: NewBaggagePropagator`, `undefined: WithExperimentBaggage`; `FAIL`.

- [ ] **Step 3: Implement**

`baggage.go` per Interfaces (`Inject`: `baggage.FromContext(ctx)`, keep members whose `Key()` is allowlisted, `baggage.New(kept...)`, `p.inner.Inject(baggage.ContextWithBaggage(ctx, filtered), carrier)`). `telemetry.go`: the composite propagator with `trace.NewBaggagePropagator(cfg.propagateBaggage...)`; `TelemetryConfig.PropagateBaggage`. `serve.go`: `Config.PropagateBaggage []string` with the doc: `// PropagateBaggage lists baggage keys the SDK forwards on outbound calls in addition to atropos.workflow, atropos.experiment_id and atropos.phase_id. Everything else stays in-process: baggage rides on EVERY outbound call, including third-party APIs.` `atroposd/config.go`: field + `ATROPOSD_PROPAGATE_BAGGAGE` (split on commas, trimmed) + doc line; `daemon.go` threads it. Interceptor egress and the decision-event fallback per Behaviour; delete `workflowBaggageKey` and use `trace.BaggageWorkflow`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/trace/ ./internal/interceptor/ ./internal/instrument/ ./internal/atroposd/ -run 'TestBaggagePropagator|TestExperimentFromContext|TestEgressTransport|TestInitTelemetry_Propagator|TestLoad' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: every listed test `--- PASS` (the existing `TestInitTelemetry_PropagatorHasTraceContextAndBaggage` still sees `traceparent`, `tracestate`, `baggage` in `Fields()`); `ok` ×4.

- [ ] **Step 5: Full verification, changelog, commit**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`
Expected: no gofmt output; all `ok`.

Changelog bullet (Task 2 heading): experiment/phase baggage on egress, allowlisted propagator, `Config.PropagateBaggage` / `ATROPOSD_PROPAGATE_BAGGAGE`.

```bash
git add internal/trace internal/interceptor internal/instrument/telemetry.go serve.go internal/atroposd CHANGELOG.md
git commit -m "feat(trace): propagate experiment baggage on egress through an allowlisted propagator

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Metrics — `service` label, trace exemplars, OpenMetrics

**Files:**
- Modify: `internal/metrics/metrics.go:41-54` (cache-box counters), `:61-64` (fault counter), `:105-108` (hook), `:110-117` (handler)
- Modify: `internal/instrument/middleware.go:20-34` (ingress), `:44-92` (egress)
- Modify: `internal/interceptor/interceptor.go:35-40` (`InjectionHook` with ctx), `:166-168`, `:217-219` (hook calls), + `WithService`/`Service()`
- Modify: `internal/instrument/core.go:29-30`, `:85-88`, `:113-117`; `atropos.go:47-49`, `:72-75`
- Modify: `metrics_test.go:151-200` (labels), `internal/instrument/middleware_test.go` (append), `CHANGELOG.md`, `README.md:271-275`

**Interfaces:**
- Consumes: Task 5's `Interceptor` options; Task 6's `CheckResult`.
- Produces:

```go
// internal/metrics/metrics.go
CacheBoxHitsTotal, CacheBoxMissesTotal, CacheBoxRecordsTotal *prometheus.CounterVec // labels: service
FaultInjectionsTotal *prometheus.CounterVec                                         // labels: fault_type, injection_point, service
func FaultInjectionHook(service string) func(ctx context.Context, faultType, injectionPoint string)
func Exemplar(ctx context.Context) prometheus.Labels // {"trace_id","span_id"} when the span context is valid AND sampled; nil otherwise
func ObserveWithExemplar(o prometheus.Observer, v float64, ex prometheus.Labels) // ExemplarObserver when available, else Observe
func IncWithExemplar(c prometheus.Counter, ex prometheus.Labels)                  // ExemplarAdder(1) when available, else Inc
func Handler() http.Handler // promhttp.HandlerOpts{EnableOpenMetrics: true}
// internal/interceptor/interceptor.go
type InjectionHook func(ctx context.Context, faultType, injectionPoint string)
func WithService(name string) Option
func (i *Interceptor) Service() string
```

Behaviour:
- Exemplar labels are only attached when `sc.IsSampled()` (Review Focus 2); a nil `prometheus.Labels` leaves the existing exemplar in place (client_golang contract) and never panics; the two labels total 63 runes, under `prometheus.ExemplarMaxRunes` (128).
- `IngressMiddleware` reorders to `otelhttp.NewHandler(measured, serviceName)` where `measured` wraps `i.IngressMiddleware(next)` and reads `metrics.Exemplar(r.Context())` — inside the otelhttp span the request context carries the server span. `EgressTransport` reorders the same way: `otelhttp.NewTransport(measuredRT)`, with `measuredRT` wrapping `faulted`; the cache-box classification (`:79-88`) uses `resolve().Service()` for the label. The proxy's `observe` wrapper (`internal/proxy/overhead.go:47-62`) stays outside and unlabelled.
- Hook: the interceptor calls `i.onInject(ctx, faultType, point)` with the request context; `metrics.FaultInjectionHook(service)` adds the exemplar. `core.go` defaults the hook to `metrics.FaultInjectionHook(cfg.Service)` and passes `interceptor.WithService(cfg.Service)`; `atropos.go` uses `metrics.FaultInjectionHook("")` (service unknown before `Serve`).

- [ ] **Step 1: Write the failing tests**

`internal/instrument/middleware_test.go` (append):

```go
func installSampledProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample())))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return exp
}

func histogramExemplarLabel(t *testing.T, name string, labels map[string]string, key string) string {
	t.Helper()
	families, _ := metrics.Registry.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matchLabels(m.GetLabel(), labels) {
				continue
			}
			for _, b := range m.GetHistogram().GetBucket() {
				for _, l := range b.GetExemplar().GetLabel() {
					if l.GetName() == key {
						return l.GetValue()
					}
				}
			}
		}
	}
	return ""
}

func TestIngressMiddleware_ExemplarCarriesTraceID(t *testing.T) {
	exp := installSampledProvider(t)
	ic := interceptor.New(nil, nil)
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), "exemplar-svc", ic)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want the server span", len(spans))
	}
	got := histogramExemplarLabel(t, "http_server_request_duration_seconds", map[string]string{"method": "GET", "status_code": "204", "service": "exemplar-svc"}, "trace_id")
	if got != spans[0].SpanContext.TraceID().String() {
		t.Fatalf("exemplar trace_id = %q, want %s", got, spans[0].SpanContext.TraceID())
	}
}

// Review Focus 2.
func TestExemplar_NilWhenUnsampled(t *testing.T) {
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID{1}, SpanID: oteltrace.SpanID{1}})
	if ex := metrics.Exemplar(oteltrace.ContextWithSpanContext(context.Background(), sc)); ex != nil {
		t.Fatalf("exemplar = %v for an unsampled span", ex)
	}
	if ex := metrics.Exemplar(context.Background()); ex != nil {
		t.Fatalf("exemplar = %v for no span", ex)
	}
	sampled := sc.WithTraceFlags(oteltrace.FlagsSampled)
	if ex := metrics.Exemplar(oteltrace.ContextWithSpanContext(context.Background(), sampled)); ex["trace_id"] != sampled.TraceID().String() || ex["span_id"] != sampled.SpanID().String() {
		t.Fatalf("exemplar = %v", ex)
	}
}

func TestMetricsHandler_OpenMetricsExposesExemplars(t *testing.T) {
	installSampledProvider(t)
	ic := interceptor.New(nil, nil)
	IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), "om-svc", ic).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/openmetrics-text") {
		t.Fatalf("content-type = %q", rec.Header().Get("Content-Type"))
	}
	// client_golang sorts exemplar labels, so span_id renders before trace_id.
	if !strings.Contains(body, `# {span_id="`) || !strings.Contains(body, `trace_id="`) || !strings.Contains(body, "# EOF") {
		t.Fatalf("OpenMetrics body lacks exemplars/EOF:\n%s", body)
	}
}

func TestFaultInjectionHook_LabelsServiceAndExemplar(t *testing.T) {
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID{7}, SpanID: oteltrace.SpanID{7}, TraceFlags: oteltrace.FlagsSampled})
	metrics.FaultInjectionHook("hook-svc")(oteltrace.ContextWithSpanContext(context.Background(), sc), "*inline.Latency", "ingress")
	families, _ := metrics.Registry.Gather()
	for _, f := range families {
		if f.GetName() != "atropos_fault_injections_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m.GetLabel(), map[string]string{"fault_type": "*inline.Latency", "injection_point": "ingress", "service": "hook-svc"}) {
				if m.GetCounter().GetValue() != 1 || m.GetCounter().GetExemplar() == nil {
					t.Fatalf("counter = %v", m.GetCounter())
				}
				return
			}
		}
	}
	t.Fatal("atropos_fault_injections_total{service=hook-svc} not found")
}
```

`matchLabels` exists in `metrics_test.go:46` (package `atropos`); copy it into `internal/instrument/middleware_test.go` as a test helper. `metrics_test.go:183` `gatherCounter(c, nil)` becomes `gatherCounter(c, map[string]string{"service": ""})` (the root test goes through `EgressTransport` whose baseline interceptor has no service name).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument/ -run 'Exemplar|OpenMetrics|FaultInjectionHook'`
Expected: `undefined: metrics.Exemplar`, `undefined: metrics.FaultInjectionHook`; `FAIL`.

- [ ] **Step 3: Implement**

`metrics.go` per Interfaces (`NewCounterVec` for the three cache-box counters with `[]string{"service"}`; the fault counter adds `service`; `Exemplar` via `oteltrace.SpanContextFromContext`). `interceptor.go`: `InjectionHook` signature, `service` field, `WithService`, `Service()`, hook calls with `ctx`. `instrument/middleware.go` per Behaviour. `core.go` and `atropos.go` per Behaviour.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/instrument/ . -run 'Exemplar|OpenMetrics|FaultInjectionHook|Metrics' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: the four new tests and the root `TestIngressMetrics`/`TestEgressMetrics*` `--- PASS`; `ok` ×2.

- [ ] **Step 5: Full verification, docs, commit**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/...`
Expected: no gofmt output; all `ok`.

`README.md:271-275`: the cache-box and fault counters carry `service`; the request-duration histograms and the fault/cache-box counters carry `trace_id`/`span_id` exemplars; `/metrics` negotiates OpenMetrics (`Accept: application/openmetrics-text`), which is what exposes exemplars. Changelog bullet under a `### Changed — Metrics` heading: label and exemplar changes (dashboards that `sum by (service)` the cache-box counters now work; fixed in a separate plan).

```bash
git add internal/metrics internal/instrument internal/interceptor atropos.go metrics_test.go README.md CHANGELOG.md
git commit -m "feat(metrics): service label, trace exemplars and OpenMetrics on /metrics

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: Documentation

**Files:**
- Modify: `README.md:160-166` (telemetry paragraph), `:175-188` (Config table), `:399-401` (env list); `AGENTS.md` (new `### Telemetry` subsection under `## SDK Bootstrap`); `CHANGELOG.md` (consolidate the bullets added by Tasks 2, 4, 8, 9)

- [ ] **Step 1: README**

Telemetry paragraph (`:160-166`) becomes: `Serve` bootstraps OTel for traces and logs — exporters from `OTEL_TRACES_EXPORTER` / `OTEL_LOGS_EXPORTER` (`otlp` default, `console`, `none`) and `OTEL_EXPORTER_OTLP_*`; `COLLECTOR_SERVICE_ADDR` or a schemeless endpoint still mean "gRPC collector"; sampler from `OTEL_TRACES_SAMPLER` (default parent-based always-on); resource `service.name`, `service.version`, `deployment.environment.name`, `service.instance.id` and `k8s.pod.name` (`ATROPOS_POD_NAME`, else `HOSTNAME`), `atropos.host`; W3C TraceContext plus an allowlisted Baggage propagator. Config table: `TracerProvider` row text "BYO provider; also disables the SDK's log exporter", new rows `LoggerProvider` (`nil` / BYO logs, only with `TracerProvider`), `Logger` (`stdout JSON + OTel bridge` / SDK logging with `trace_id`/`span_id`), `PropagateBaggage` (`nil` / extra baggage keys forwarded on egress). Env list (`:399-401`): add the OTel variables above and `ATROPOS_POD_NAME`. Add a short "Decision events and correlation" paragraph after the cache-box section: every applied decision is one `atropos.decision` log record (attributes `atropos.rule.name`, `atropos.reason`, `atropos.fault.id`, `atropos.injection_point`, `atropos.experiment.id`, `atropos.phase.id`, `atropos.recent.trace_ids`) with the request's trace id; background fault spans link to the triggering request (`atropos.link.kind=trigger`) and the last 16 sampled requests (`recent`); joiners link to the fault span (`fault`), carry `atropos.fault.id`, and bump `atropos.fault.joined`; TraceQL `{ span.atropos.fault.id = "…" }` answers "which requests met this fault". Baggage note: `atropos.experiment_id` / `atropos.phase_id` ride on every outbound call once a cache-box decision applies, including calls to third parties — opaque ids, strip them at an egress gateway if that matters; non-allowlisted members never leave the process through the SDK.

- [ ] **Step 2: AGENTS.md**

Under `## SDK Bootstrap` add `### Telemetry` with: the env list (one line), "logs are a signal: `Config.Logger` tees stdout JSON and the OTel bridge; use `*Context` log methods", the decision event name and attributes, the exemplar labels, and "metrics stay on client_golang; `/metrics` serves OpenMetrics".

- [ ] **Step 3: Verify and commit**

Run: `go vet ./... && go test -count=1 ./... && git diff --stat`
Expected: all `ok`; only `README.md`, `AGENTS.md`, `CHANGELOG.md` changed.

```bash
git add README.md AGENTS.md CHANGELOG.md
git commit -m "docs: telemetry configuration, decision events, exemplars and baggage

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11 (optional, last): Declarative configuration via `OTEL_CONFIG_FILE` (otelconf)

**Files:**
- Create: `internal/instrument/otelconf.go`, `internal/instrument/otelconf_test.go`
- Modify: `internal/instrument/telemetry.go` (branch at the top of the SDK path), `README.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 4's `Telemetry`, `NewLogger`; Task 8's propagator.
- Produces: `func declarativeConfig(ctx context.Context) (sdk *otelconf.SDK, ok bool, err error)` — `ok=false` when `OTEL_CONFIG_FILE` is unset; `err` when `OTEL_EXPERIMENTAL_CONFIG_FILE` is set (otelconf v0.26.0 rejects that name: `config.go:75`, "no longer supported, use OTEL_CONFIG_FILE instead"), when the file cannot be read or parsed (`otelconf.ParseYAML`), or when `otelconf.NewSDK(otelconf.WithOpenTelemetryConfiguration(*cfg))` fails.

Behaviour: when `ok`, `initTelemetry` registers `sdk.TracerProvider()` and `global.SetLoggerProvider(sdk.LoggerProvider())`, keeps the SDK's own propagator (the file's `propagator:` section is ignored and logged once), builds `Telemetry{Logger: NewLogger(os.Stdout, sdk.LoggerProvider()), LoggerProvider: sdk.LoggerProvider(), Shutdown: sdk.Shutdown}`, and skips the env fallback, autoexport and `newResource` — the file owns the resource, so a file must carry `service.name` itself. The dependency cost is real: otelconf v0.26.0 adds aws/azure/gcp detectors, `autoprop` and the b3/jaeger/ot/aws propagators (about nine modules); that is why this task is last and optional.

- [ ] **Step 1: Add the module, pinned**

```bash
go get go.opentelemetry.io/contrib/otelconf@v0.26.0
```

- [ ] **Step 2: Write the failing tests in `internal/instrument/otelconf_test.go`**

```go
const minimalConfig = `file_format: "1.0-rc.2"
resource:
  attributes:
    - name: service.name
      value: from-file
tracer_provider:
  processors: []
logger_provider:
  processors: []
`

func TestInitTelemetry_DeclarativeConfigWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "otel.yaml")
	if err := os.WriteFile(path, []byte(minimalConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_CONFIG_FILE", path)
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "")
	t.Setenv("OTEL_TRACES_EXPORTER", "console") // must be ignored: the file wins
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "from-config"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	if v, ok := resourceOf(t).Set().Value("service.name"); !ok || v.AsString() != "from-file" {
		t.Fatalf("service.name = %v, want from-file (the file owns the resource)", v)
	}
	if _, ok := global.GetLoggerProvider().(*sdklog.LoggerProvider); !ok {
		t.Fatalf("logger provider = %T, want the file's SDK provider", global.GetLoggerProvider())
	}
}

func TestDeclarativeConfig_RejectsDeprecatedVariable(t *testing.T) {
	t.Setenv("OTEL_CONFIG_FILE", "")
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "/nonexistent.yaml")
	if _, _, err := declarativeConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "OTEL_CONFIG_FILE") {
		t.Fatalf("err = %v, want a pointer to OTEL_CONFIG_FILE", err)
	}
}

func TestDeclarativeConfig_UnsetIsNotOK(t *testing.T) {
	t.Setenv("OTEL_CONFIG_FILE", "")
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "")
	if _, ok, err := declarativeConfig(context.Background()); ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}
```

`resourceOf` comes from Task 3's tests. The file's `tracer_provider` is an SDK provider even with no processors (`otelconf/trace.go:27-55` builds `sdktrace.NewTracerProvider` whenever the section is present), so the probe span is a `ReadOnlySpan`. If `otelconf/log.go` turns out to wrap its provider in a non-`*sdklog.LoggerProvider` type, replace that assertion with `global.GetLoggerProvider() == tel.LoggerProvider`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument/ -run 'Declarative'`
Expected: `undefined: declarativeConfig`; `FAIL`.

- [ ] **Step 4: Implement `otelconf.go` and the branch, then tidy**

Run after implementing: `go mod tidy && grep '^go ' go.mod`
Expected: `go 1.25.5`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/instrument/ -run 'Declarative|TestInitTelemetry' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: all `--- PASS`; `ok`.

- [ ] **Step 6: Full verification, docs, commit**

Run: `gofmt -l $(git ls-files '*.go'); go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/instrument/... ./internal/interceptor/... && grep '^go ' go.mod`
Expected: no gofmt output; all `ok`; `go 1.25.5`.

README (telemetry paragraph): "`OTEL_CONFIG_FILE=/etc/atropos/otel.yaml` switches to the OpenTelemetry declarative configuration file (format `1.0-rc.2`); the file then owns exporters, sampler and resource, and the `OTEL_*` exporter variables are ignored (`OTEL_EXPERIMENTAL_CONFIG_FILE` is rejected)". Changelog bullet accordingly.

```bash
git add go.mod go.sum internal/instrument/otelconf.go internal/instrument/otelconf_test.go internal/instrument/telemetry.go README.md CHANGELOG.md
git commit -m "feat(telemetry): declarative OpenTelemetry configuration via OTEL_CONFIG_FILE (otelconf)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review

1. **Spec coverage.** ADR-3 item 1 (logs as a signal, `*Context` logging, `log.Printf` gone): Tasks 4–5. Item 2 (decision events with the six attributes): Task 7. Item 3 (links to the trigger, `atropos.fault.id` on the request span, joins with `atropos.fault.joined`, 256-entry ring with 16 links and the full list in the event): Task 6 + Task 7. Item 4 (experiment/phase baggage, allowlist, leak note): Task 8 + Task 10. Item 5 (no forced `AlwaysSample`, env sampler): Task 3. Item 8 (autoexport, `COLLECTOR_SERVICE_ADDR` fallback, declarative file): Tasks 2 and 11. Item 7 as simplified (client_golang, exemplars, `service` label, OpenMetrics): Task 9. Item 9 (one semconv, stable otelhttp names): Tasks 1 and 3. Design §5.7 resource (`service.instance.id`, `atropos.host`): Task 3. The scope's test list (in-memory log exporter, span-link assertions with `tracetest`, exemplars in OpenMetrics output, `none`/`console` exporter selection): Tasks 4/7, 6, 9, 2. Items 6 and 10 (collector pipeline, bed Loki, filelog, profiles) are bed work and intentionally absent.
2. **Deviations from the brief, all forced by verified facts.** The module set is otel v1.46.0 / contrib v0.71.0 / log v0.22.0 rather than the latest (v1.47.0 / v0.72.0 require `go 1.26.0`); `client_golang` moves to v1.24.1 because autoexport requires it; the declarative file variable is `OTEL_CONFIG_FILE` because otelconf rejects `OTEL_EXPERIMENTAL_CONFIG_FILE`; the Logs API at v0.22.0 already uses `attribute.KeyValue`/`attribute.Value` (no `log.KeyValue`), and the logger globals live in `go.opentelemetry.io/otel/log/global` at this version.
3. **Type consistency.** `Telemetry`/`TelemetryConfig` (Task 4) are what Tasks 8 and 11 extend; `CheckResult{Handle, Decision, RunID, Joined}` (Task 6) is what Task 7 reads; `trace.Span.SpanContext()` (Task 6) is what joins use; `InjectionHook` with `ctx` (Task 9) is the only signature `core.go`/`atropos.go` reference; `installRecordingProvider`, `installDecisionRecorder`, `hasAttr`, `attrString`, `findSpan`, `linksOfKind`, `backgroundDecision` are shared test helpers in package `interceptor`; `resourceOf` and `recordingLogExporter` live in package `instrument` tests (the trace package duplicates the exporter helper).
4. **Review Focus.** Each of the five lines names its pinning test and task.
5. **Known gaps left on purpose.** `decode.go:107` and `register.go:214` still log through `slog.Default()` (no request context, no host logger); the proxy overhead histogram carries no exemplar; the sidecar's app-side stdout logs are collector work; the Grafana dashboards are fixed in a separate plan.
