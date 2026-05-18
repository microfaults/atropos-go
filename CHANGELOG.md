# Changelog

All notable changes to atropos-go are documented in this file.

## Unreleased

### Added — Long-running faults & SDK liveness

- **Multi-slot `DemoEvaluator`.** Active faults are now stored as a map keyed by ID rather than a single decision. New APIs: `Set(decision, req)`, `ClearSlot(id)`, `ActiveIDs()`, `Active()` (returns all `FaultRequest`s), `Confirm(id)` (heartbeat), `StaleSlots(maxAge)`. Per-call `Evaluate` returns the first match in `inline > network > resource` priority, with deterministic lex order within a category. Limitation: still a single decision per `Evaluate` — see `docs/plans/2026-05-17-concurrent-multi-fault-execution.md`.
- **`MultiEvaluator` composition primitive** (`internal/evaluator/multi.go`, re-exported as `atropos.MultiEvaluator` / `NewMultiEvaluator`). Chains evaluators and returns the first non-nil `Decision`. Intended for wiring a `StaticEvaluator` + `DemoEvaluator` behind one `Evaluator` interface.
- **Fault watchdog** (`StartFaultWatchdog` in `register.go`). 1 s ticker that reaps slots whose `lastConfirmedAt` is older than `max(3 * pollInterval, 30s)`. Auto-started by `ConnectManteion` whenever `ApplyTargets.DemoEval` is set. Independent of poll cadence so it still runs when the poll loop is wedged.
- **Poll-interval-in-register.** `RegisterRequest.PollIntervalMs` lets Manteion size its liveness threshold to each SDK's configured cadence.
- **OTLP HTTP exporter support.** `WithHTTP(bool)` option (`options.go`) selects `otlptracehttp` over the default gRPC exporter. Default port `4318` when HTTP is on. Endpoint resolution is scheme-aware — `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318` uses `WithEndpointURL`; a bare `host:port` uses `WithEndpoint`. Batch timeout reduced to 100 ms for faster trace egress.
- **`Apply` reconciliation.** When `ApplyTargets.DemoEval` is set, `Apply` reconciles `resp.ActiveFaults` against currently-armed slots: drops slot IDs not in the response, applies the rest, and `Confirm`s them. Pairs with the watchdog: a confirmed slot survives until the server's lifetime ends or contact is lost long enough that the watchdog steps in. Manteion is authoritative when connected — see the doc on `FaultAdminHandlerWith`.
- **Per-slot admin DELETE.** `DELETE /admin/fault/{id}` clears one slot; `DELETE /admin/fault` still clears all.

### Changed — Fault wire format (multi-fault)

- **`RegisterResponse.ActiveFault` → `ActiveFaults []FaultRequest`**; **`FaultStatus.Fault` → `Faults []*FaultRequest`**. Manteion-side and admin-status payloads carry arrays now.
- **`FaultRequest` shape.** New fields: `id` (optional, defaults to category), `duration_ms` (lifecycle hint; `0 = infinite, whitelist enforced server-side`). The old top-level `delay`/`jitter`/`duration`/`status_code`/`message` fields are removed — inline configs now live inside `config` (`{"type":"latency","config":{"delay":"100ms"}}`, `{"type":"hang","config":{"duration":"2s"}}`, etc.), matching the compiled-rule envelope.
- **Manteion poll body** (`GET /api/v1/sdk/rules`) now carries `active_faults` and `freeze_cfg` alongside `rules`. `fetchRules` routes them through a single `Apply` pass so the configured `NetworkResolver` is wired uniformly (previously rules were decoded twice and briefly published without the resolver — fixed).
- **Manteion offline mode** logs a warning at `ConnectManteion` time when `MANTEION_URL` is unset or `WithOfflineMode()` is in effect, instead of silently degrading.

### Fixed

- **Network rules briefly resolver-less on poll** — `manteion_client.fetchRules` no longer double-decodes/double-`SetRules`; `Apply` is the single source of truth and `ruleVersion` only advances after it succeeds.
- **`DemoEvaluator.Evaluate` / `Active` map-iteration non-determinism** — slot IDs are sorted lex order so the chosen decision is stable across calls.

### Docs

- `docs/plans/2026-05-17-concurrent-multi-fault-execution.md` — design note covering the current single-decision-per-`Evaluate` limitation and the three changes needed for true concurrent multi-fault execution (eager arming, per-slot lifecycle, slot-aware registry keys). Referenced from `DemoEvaluator.Evaluate` doc comment.

### Changed — fault wire format and rename

- **Renamed `network.Loss` → `network.RetransmitDelay`.** The toxic never dropped bytes (userspace TCP can't); it simulated the observable effect via per-chunk delay. The new name reflects what it actually does. Wire-format fault_type renamed `loss` → `retransmit_delay`; config field `retransmit_delay` → `delay`. RST reason string `packet_loss_threshold` → `consecutive_retransmit_threshold`. For true packet drop semantics, use kernel-level `tc netem`.
- **`CompiledFault` wire format restructured.** The opaque `Config json.RawMessage` blob is split into `Network *NetworkEnvelope` (host/target/direction/scope, only meaningful for network category) + `Params json.RawMessage` (toxic-specific params). Envelope is rejected on non-network categories; required on network.
- **Network host discriminator.** `NetworkEnvelope.Host` selects `"proxy"` (TCP sidecar; current behaviour) or `"inline"` (per-request response shaping via RoundTripper; v6 — schema lands now, execution deferred).
- **Composition rules now error with v6 marker** rather than a generic "not supported" message.

### Added — Cache-Box Stage 1 (egress, HTTP)
- `internal/cachebox` package: stdlib-only cache-box engine.
  - `Store` interface + `Entry` with `StatusCode`/`Header`/`Body`/`ObservedLatency`/`RecordedAt`/`HitCount`.
  - `MemStore`: LRU-bounded in-memory store using `container/list` + map for O(1) lookup and eviction. Lazy TTL check on `Get`. Byte-accounting + hit/miss/eviction counters.
  - `KeyFunc` + three `KeyStrategy` values: `exact` (method+path+normalized query), `exact_with_host`, `exact_with_body` (adds FNV-1a hash of body). Query params normalized by sorting alphabetically.
  - `Recorder`: bounded-channel async drain goroutine. Drops on backpressure rather than blocking the hot path. `PushFunc` hook for Stage 3 manteion forwarding (always nil in Stage 1).
  - `DelaySource`: `ObservedDelaySource` replays entry latency verbatim; `DistributionDelaySource` scaffolds lognormal sampling via Box-Muller + PCG for Stage 3.
  - `CacheBox` coordinator wiring store + recorder + delay source + key function together. `BufferRequestBody` helper for `exact_with_body`. `DefaultMaxBodyBytes = 1 MiB` soft cap.
- `internal/evaluator/static.go`: `StaticEvaluator` — simple list-of-rules evaluator with AND-semantics label matching, used to exercise cache-box without a full policy engine. Atomic rule replacement via `SetRules`.
- `CacheBoxAction` added to `evaluator.Decision` with four values: `CacheBoxNone`, `CacheBoxPassthrough`, `CacheBoxReplay`, `CacheBoxReplayDelay`. Mutually exclusive with `Decision.Fault` by contract.
- `internal/interceptor/cachebox.go`: `handleCacheBox` dispatcher for the three cache-box modes.
  - Passthrough: forwards to the real downstream, buffers the response body up to the cap, enqueues an async cache record, returns the body to the caller with `X-Atropos-Cache-Key` + `X-Atropos-Cache-Latency-Us` headers.
  - Replay: serves from cache with `X-Atropos-Cache-Mode: replay`. Miss falls through to passthrough so subsequent requests hit.
  - Replay-with-delay: sleeps for `DelaySource.Sample(entry)` before serving. Respects `context.Done()` to abort cancelled requests.
  - Oversized responses (> `MaxBodyBytes`) skip caching via `io.MultiReader` patch — body streams through unchanged, no truncation.
- `atropos.Configure` is now variadic: `Configure(WithEvaluator(e), WithCacheBoxCoordinator(cb))`. Old positional signature removed (pre-1.0, internal breakage acceptable).
- Re-exports in `types.go`: `CacheBox`, `CacheBoxConfig`, `CacheBoxEntry`, `CacheBoxStore`, `KeyStrategy*`, `NewCacheBox`, `NewCacheBoxMemStore`, `NewStaticEvaluator`, and the four `CacheBoxAction` constants.
- `trace/attrs.go`: cache-box span/event/attribute constants (`SpanCacheBoxCheck`, `EventCacheBoxRecord|Replay|Miss|Oversize|Error`, `AttrCacheBoxMode|Key|Hit|LatencyUs|ResponseSize|ResponseBody|Workflow|Injection|Reason`), plus `AttrHTTPQuery`.
- `extractHTTPLabels`: now emits the raw query string and the `atropos.workflow` label pulled from W3C Baggage (placeholder until workflow taxonomy lands).
- Docs: `VISION.md` updated with implemented architecture and ingress cache-box sketch for modeling service-internal cache states (warm/cold tiers). `ambiguities.md` added with 14 flagged engineering decisions for later review.

### Changed
- `Interceptor.Check()` split into `Evaluate()` (side-effect-free rule match) + `StartFault()` (creates span, validates, starts fault). The old `Check()` remains as a fault-only convenience that silently skips cache-box decisions so existing fault-only callers don't need to change.
- `EgressTransport` now branches on the decision: cache-box actions route through `handleCacheBox`, faults through `StartFault`. `IngressMiddleware` and gRPC interceptors are unchanged (fault-only).
- Refactored gRPC interceptors: extracted shared `checkAndWait` helper, reduced boilerplate across all four interceptors.
- Refactored network proxy `conn.go`: extracted `checkHijackers()` and `streamWorker()` helpers from `handleAffected`.
- Consolidated duplicate test cases into table-driven subtests (`config_file_test.go`, `grpc/interceptor_test.go`, `init_test.go`, `metrics_test.go`).

### Fixed
- Race in `fault.Handle.SetOnResult`/`Send`: swapped to `atomic.Pointer[func(Result)]`. This was pre-existing (unrelated to cache-box) but was blocking race-detector runs of the new cache-box interceptor tests.
- Missing `internal/fault/inline` import in `faults.go` — pre-existing build breakage on `main`.

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
