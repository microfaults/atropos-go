# Changelog

All notable changes to atropos-go are documented in this file.

## Unreleased

### Added — Node-agent netem (packet loss, kernel latency)

- **`packet_loss` network fault** (`params: {"loss", "correlation"}`, percent) and **`mechanism: "netem"` on network `latency`**: both run as `tc netem` in the pod's network namespace, through the node agent, from the atroposd sidecar only. `packet_loss` is always netem; latency without the mechanism stays the proxy toxic.
- **Agent.** The netem qdisc goes on the pod's egress interface: under a `prio` root with one `u32` filter per IPv4 address of `network.target` (and its port, when named), or as the root itself when no target is given (then the sidecar's own traffic is shaped too). `network.scope` multiplies `loss` and must lie in (0,1] (0 means 1); latency needs scope 1; egress only (`direction` empty or `upstream`). One netem fault per pod network namespace (`409` for a second, `400` for an invalid spec). Each fault is journaled (`-netem-journal`, default `/var/lib/atropos-agent/netem`) before its first `tc` command and removed on stop, on sidecar disconnect and at agent boot; a failed removal keeps the entry and the namespace reserved so a later stop or the next boot retries it. Known residual: a crash between the root `qdisc add` and the journal re-save leaves the qdisc for manual removal (`deploy/agent/README.md`).
- **`GET /v1/capabilities`** on the agent lists its mechanisms (`burn`, `quota`, and `netem` when `tc` and `nsenter` are on its PATH). The sidecar probes it at start and advertises the `netem` capability only when listed (an agent answering 404 is read as `burn` and `quota`). The in-process host, and a sidecar without a netem-capable agent, skip netem rules per rule (`atropos_rules_unsupported_total{category="network"}`); on a netem-capable sidecar an invalid netem spec is a decode error (`422` on `/admin/rules`).
- **Images.** The agent image installs `iproute2` and `util-linux`; the Go build stages of the agent and sidecar Dockerfiles run on the build platform and cross-compile (`FROM --platform=$BUILDPLATFORM`), since emulating the Go compiler under amd64 on an arm64 Mac segfaults it. The DaemonSet passes `-netem-journal`.

### Changed — Telemetry (ADR-3)

Both hosts (`Serve` and atroposd) bootstrap OTel through one `InitTelemetry`, configured by the standard OTel environment.

- **Exporters.** Spans and logs are exported as contrib `autoexport` selects: `OTEL_TRACES_EXPORTER` and `OTEL_LOGS_EXPORTER` (`otlp` default, `console`, `none`) and, for `otlp`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` and the other `OTEL_EXPORTER_OTLP_*` variables. `COLLECTOR_SERVICE_ADDR` and a schemeless `OTEL_EXPORTER_OTLP_ENDPOINT` (`otel-collector:4317`) still work and mean a gRPC collector, unless `OTEL_EXPORTER_OTLP_PROTOCOL` names another protocol; an endpoint that is already a URL is used as it is, and a padded `OTEL_EXPORTER_OTLP_PROTOCOL` is trimmed before autoexport reads it. A malformed `OTEL_*` value fails startup. **Default moved:** with no endpoint configured the exporters follow the OTel default, http/protobuf to `localhost:4318`, instead of gRPC to `localhost:4317`; a bare local run against a gRPC-only collector sets `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`. The signal-specific `OTEL_EXPORTER_OTLP_TRACES_*` / `_LOGS_*` variables reach the exporters unchanged (the schemeless-endpoint fallback does not apply to them).
- **Sampler.** `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` choose it; the default is the SDK's parent-based always-on, so an incoming `traceparent` whose sampled flag is off suppresses the request's spans. Set `OTEL_TRACES_SAMPLER=always_on` to trace every request whatever its caller decided.
- **Resource identity.** `service.instance.id` is the registration identity (`Config.InstanceID` > `MANTEION_INSTANCE_ID` > hostname > service), so spans and logs join manteion's instance rows; `k8s.pod.name` is the pod name (`ATROPOS_POD_NAME`, else `HOSTNAME`; omitted when neither is set); `atropos.host` is `in-process` or `sidecar`. `service.version` and `deployment.environment.name` are stamped only when set, so `OTEL_RESOURCE_ATTRIBUTES` can supply them; `Config.Environment` no longer defaults to `"development"`. A BYO `TracerProvider` keeps its own sampler and resource.
- **Declarative configuration.** `OTEL_CONFIG_FILE` names an OpenTelemetry declarative configuration file (format `1.0-rc.2`, through contrib `otelconf` v0.26.0); the file then owns exporters, sampler and resource, and the `OTEL_*` exporter variables are ignored. `OTEL_EXPERIMENTAL_CONFIG_FILE` is rejected at startup (use `OTEL_CONFIG_FILE`). The SDK adds nothing to the file's resource: `service.name`, `service.instance.id`, `service.version`, `k8s.pod.name` and `atropos.host` are present only if the file sets them. The file's `propagator` section is ignored (logged at WARN): the propagators stay W3C TraceContext plus Baggage. Its `meter_provider` section is ignored too (logged at WARN): metrics stay on client_golang. The path is read trimmed. A BYO `TracerProvider` still wins over the file. A file that cannot be read, parsed or built fails startup.
- **Logs are a signal.** Both hosts build an SDK `LoggerProvider` with the span resource and register it as the global logger provider; the shutdown flushes spans, then logs. `Config.Logger` now defaults to JSON on stdout (Info and above) teed into the `otelslog` bridge (scope `git.ucsc.edu/microfaults/atropos-go`, every level, Debug included), so `*Context` log calls carry `trace_id` and `span_id`. New `Config.LoggerProvider`, honoured only together with `Config.TracerProvider`; a BYO `TracerProvider` builds no exporter, for traces or logs. `atroposd` logs through the same default. **Migration for embedders:** the default was `slog.Default()`; pass `Config.Logger: slog.Default()` to keep it (a BYO logger is used as is and bypasses the bridge), and set `OTEL_LOGS_EXPORTER=none` to keep the logs out of the collector.
- **Request-scoped logging.** The SDK's own request-scoped error lines (a failed ingress, egress, gRPC or Transient fault check, and a recovered panic in a Transient start) go through that logger with the request's context instead of the standard `log` package, so they carry `trace_id` and `span_id`; the text is the message and the error is the `error` attribute (`panic` for the recovered panic). The warnings for rules and active-fault slots a host skips at apply time (`rule skipped: …`, `active fault skipped: …`) use the host's logger instead of `slog.Default()`.
- **Span topology.** Every fault run started through the interceptor has an id, `atropos.fault.id` (16 hex characters), on its fault span and on the span of each request that started or joined it (`MustFault`'s direct fallback, which bypasses the evaluator, still starts an untracked fault without one). A background fault span links the request that triggered it (`atropos.link.kind=trigger`) and up to 16 of the last 256 sampled requests the interceptor evaluated (`recent`, newest first); an unsampled request is never a link target. A Transient fault span, a child of its request's span, links that request too (it starts after the response, so the request's span has usually ended and does not get the id); an eagerly armed fault (the sidecar) has only `recent` links. A request that joins a running background fault is counted on its fault span as `atropos.fault.joined`, and links that span (`fault`) only when the fault span is sampled. A request is any call the interceptor is not told is an eager arm: one without a span context (a gRPC call without otelgrpc) is counted too, it has no span to stamp or link. `Core.Arm` marks its calls (`interceptor.ArmedContext`), so a re-arm that meets the running fault (the sidecar re-arms on every delivery) is not counted, and an armed start has no trigger link and stamps no span, whatever span its context carries. A deduplicated start now returns the decision with `Joined` and the run's `RunID` (still no handle) instead of an empty `CheckResult`, and `Hook` reports such a join as `atropos.fault.injected`. TraceQL `{ span.atropos.fault.id = "<id>" }` finds the requests that met a fault.
- **Decision events.** Every decision the interceptor applies is an `atropos.decision` log event (INFO), emitted through the OTel Logs API on the global logger provider (scope `git.ucsc.edu/microfaults/atropos-go/decision`; not through `Config.Logger`) with the context of the request it was applied to, so the record carries that request's `trace_id` and `span_id` (a Transient start's record carries its request's server span, which has ended by then). `atropos.action` is `fault.start` when a fault run starts (an eager arm's start too, without a trace), `fault.join` when a request joins a running background fault (an eager re-arm emits none), or `cachebox.<action>` (`passthrough`, `replay`, `replay_with_delay`) when a cache-box decision dispatches. The other attributes: `atropos.rule.name`, `atropos.reason`, `atropos.fault.id` (the run id; empty for a cache-box action), `atropos.injection_point`, `atropos.experiment.id` and `atropos.phase.id` (from the matched rule's cache-box context; without one, from the request's `atropos.experiment_id` / `atropos.phase_id` baggage members; empty when neither names one), and `atropos.recent.trace_ids`: up to 16 distinct trace ids among the last 256 sampled requests the interceptor evaluated, newest first, left out when there are none. A fault that fails to start emits nothing, and nothing is assembled when the logger provider would drop the event. `MustFault`'s direct fallback, which bypasses the evaluator, emits none.
- **Baggage.** The propagator stays W3C TraceContext plus the standard, unfiltered W3C Baggage propagator: foreign baggage is never dropped, and every member a request arrived with (properties included) is forwarded on its outbound calls, in-process and through the atroposd sidecar alike; the workflow member keeps its name (`atropos.workflow`). Once a cache-box decision whose context names an experiment applies to an outbound HTTP call, that call carries the members `atropos.experiment_id` and `atropos.phase_id` (the phase member is removed when the context names none), set on a clone of the request and re-injected with `traceparent` through the global propagator (members of a `baggage` header the application set on the request itself are kept); from there they travel on every further hop through the downstream services' inbound baggage. They ride on the call whatever its destination, third-party APIs included; they are opaque ids, so strip them at an egress gateway if that matters.
- **Metrics: `service` label.** Metrics stay on client_golang. `atropos_cachebox_hits_total`, `atropos_cachebox_misses_total` and `atropos_cachebox_records_total` (unlabelled before) carry `service`; `atropos_fault_injections_total` is `{fault_type, injection_point, service}`; `atropos_sync_generation`, `atropos_replay_set_state{state}` and `atropos_rules_unsupported_total{category}` gain `service` too. Metric names are unchanged. Dashboards that `sum by (service)` the cache-box counters now work (the dashboards themselves are fixed in a separate plan). Before `Serve` runs, the in-process baseline labels its counters `service=""`.
- **Metrics: trace exemplars.** `http_server_request_duration_seconds`, `http_client_request_duration_seconds`, the fault counter and the cache-box counters carry `trace_id`/`span_id` exemplars when the span they are recorded under is sampled. The ingress and egress measurement now runs inside the otelhttp span, so the exemplar names the request's server or client span; a fault's exemplar is the span of the request that started it (an eager arm has none).
- **Metrics: OpenMetrics.** `/metrics` negotiates OpenMetrics (`Accept: application/openmetrics-text`), which is the format that exposes the exemplars; other scrapes still get the classic text format.
- **Server span name (deliberate rename).** The ingress server span (`Serve` and the atroposd ingress listener) takes the OpenTelemetry semconv name otelhttp produces, `METHOD /route` (`GET /product/{id}`, with `http.route=/product/{id}`) when the business handler is a `ServeMux` whose pattern matched, else the method alone (`GET`, no `http.route`), instead of the service name. `Serve`'s control mux, which mounts the handler at `/`, sets neither. **Migration:** queries and dashboards that selected the server span by `name = <service>` select it by `service.name` and `span.kind = server` instead (TraceQL `{ resource.service.name = "<service>" && kind = server }`).

### Fixed

- **`POST /admin/fault` validates the fault.** A fault that decodes but could never start (a latency with neither delay nor jitter, such as `delay: "0s"`; ramps that do not fit the duration) answers `400` and arms no slot, instead of being armed and failing every matching request's check.

### Changed — Desired-state protocol v1 (ADR-2)

The in-process host (`Serve`) and the atroposd sidecar converge on manteion's materialized desired state (contract: `docs/plans/2026-10-30-desired-state-protocol.md`). The wire additions are additive, and `version` keeps its name (it now carries the generation).

- **Long-poll with a conditional request.** The poll loop sends `GET /api/v1/sdk/rules?...&version=<generation held>&wait=30` with `If-None-Match: "<etag held>"` (only when an ETag is held) and polls again as soon as the answer is handled, never sooner than a second after the previous poll started. `RuleSync` gains `etag` and `replay_set`, both `omitempty`. The SDK treats the ETag as opaque: it is the content hash alone, and the generation travels in `X-Atropos-Generation` and the body's `version`.
- **`304` adoption.** A `304` applies nothing and adopts the generation (and ETag) manteion confirms. It also re-confirms the held fault slots, so a steady state of `304`s never lets the watchdog reap a desired slot; a late `304` (one answering a poll sent before the instance's record of its document last changed: an apply, a forgotten document, a reaped slot) adopts nothing, so the generation held never goes back, even when a newer generation of the same content keeps the ETag.
- **`StatusReport`** (`POST /api/v1/sdk/status`) after every apply and on every replay-set transition: a full snapshot of the generation and ETag held, the `unsupported` rules, the `errors`, and the desired replay set with its pull state (`fetching` and `failed` name a set not yet held). A report that fails to send is re-sent before the next poll and by its own retry (2 s, doubling up to the poll wait).
- **One scoped `apply()`.** Register, poll and `POST /admin/rules` go through the same function. The push applies only rules (it never clears admin fault slots, the delay source or the replay set), now observes the drain tracker like any other rule delivery, and is serialized with the sync's applies. A push in which some rules do not decode applies the rest and answers `422` with the per-rule messages in `{"error": ...}` (it was all-or-nothing `400`); the pushed set less the skipped rules replaces the installed one, so a push in which no rule decodes installs an empty set. `400` is malformed JSON and `500` a host that cannot hold rules. Connected, a push is a short-lived override like an admin fault slot: it clears the ETag the instance holds, so the next poll (at most one hold plus the apply later) re-applies manteion's desired state; it sends no status report.
- **Per-rule skipping.** Rules this host cannot execute (a category outside its capabilities, `quota` in-process, network `host: inline` outside the sidecar or `host: proxy` without a resolver) and composition rules are skipped, counted in `atropos_rules_unsupported_total{category}` (compositions under `category="composition"`) and reported, instead of failing the sync; a rule that does not decode is skipped and reported under `errors`.
- **`freeze_cfg.context` honoured.** The delay source is installed scoped to the context's `(experiment_id, phase_id)` and shown in the `GET /admin/cachebox` stats (`Delay`); replays under another pair keep the entry's observed latency. A document whose `freeze_cfg` names the source already installed (the same `mu`, `sigma`, `seed` and scope) keeps it, so the sample sequence is not restarted by every document of a frozen phase. `POST /admin/cachebox/delay` takes the same `context`. An absent `freeze_cfg` clears the delay source and an absent `replay_set` the installed replay set (`[]` clears `rules` and `active_faults`).
- **Replay-set pull.** The instance fetches the `replay_set` itself: `GET /api/v1/cache/artifacts/{id}` in `Range` chunks of at most 4 MiB, staged through the preload path and committed once the entry count and the W5 checksum match; its `fetching|committed|failed` state is reported. A chunk is retried from its offset on a transport error, a request that cannot be built, a 5xx, 408 or 429. A failed pull is pulled again on the instance's own timer while the set stays desired (5 s, doubling up to 60 s, ±20 % jitter), reported `failed` → `fetching` → `committed`, so an instance that restarts mid-phase converges even while manteion answers `304`; a document that names the failed set again has it fetched at once, the backoff starting over. A set no pull can fetch (no pair scopes it, an unsupported key strategy, no artifact URL) gets no timer. `/cachebox/preload/*` stays mounted for manual use.
- **Ingest `epoch`.** Cache-push batches carry the instance's start time (unix nanoseconds) as `epoch`, extending manteion's dedupe key across restarts. `batch_seq` no longer restarts when the tracked `(experiment, phase)` pair changes: it is monotonic per epoch, so entries for one pair, then another, then the first again no longer reuse a number manteion's dedupe has seen, which dropped a real batch while the client counted it pushed.
- **Metrics.** `atropos_sync_generation` (the generation held) and `atropos_replay_set_state{state}` (the pull state; all zero when no replay set is desired).
- **Liveness cadence.** Registration advertises `poll_interval_ms = max(poll interval, hold)` (30 s with the defaults: a 10 s interval, a 30 s hold), and the fault watchdog's grace is `max(3 * max(poll interval, hold), 30s)` (90 s by default).
- **No-document register.** A register answer with neither an `etag` nor `rules` (manteion could not materialize the desired state) is ignored: the instance keeps its rules, slots, delay source and replay set and keeps polling.
- **Registration lost.** A status report or a poll answered `404` (manteion no longer knows the instance: a database reset, an instance-id collision) makes the instance drop the generation and ETag it holds and register again at once, applying the answer, instead of re-sending the report forever. Once per loss: until a report lands again, a further `404` does not register again; a poll answered `404` while the re-register is under way waits for it, re-polled after the floor and not counted as a failure.

### Added — Route publication for the workflow-builder catalog

- **`atropos.RegisterRoutes(routes ...Route)`** records the service's HTTP route inventory; `ManteionClient` includes it in every register payload (`RegisterRequest.Routes`, wire key `routes`). Manteion aggregates routes across live SDK instances into `GET /api/v1/catalog/endpoints`, which feeds the manteion-ui workflow-builder endpoint picker. Call at startup before `ConnectManteion`; calling later triggers a best-effort background re-register so the catalog converges. Each call replaces the inventory; gRPC-only services skip it.
- **`Route` type** — `{method, path, description?, depends_on?}`. `Path` is the literal serve template (e.g. `/product/{id}`). `DependsOn` names prerequisite routes as `"METHOD /path"` (same service) or `"service METHOD /path"` (fully qualified) for workflow ordering.

### Added — Long-running faults & SDK liveness

- **Multi-slot `DemoEvaluator`.** Active faults are now stored as a map keyed by ID rather than a single decision. New APIs: `Set(decision, req)`, `ClearSlot(id)`, `ActiveIDs()`, `Active()` (returns all `FaultRequest`s), `Confirm(id)` (heartbeat), `StaleSlots(maxAge)`. Per-call `Evaluate` returns the first match in `inline > network > resource` priority, with deterministic lex order within a category. Limitation: still a single decision per `Evaluate` — see `docs/plans/2026-05-17-concurrent-multi-fault-execution.md`.
- **`MultiEvaluator` composition primitive** (`internal/evaluator/multi.go`, re-exported as `atropos.MultiEvaluator` / `NewMultiEvaluator`). Chains evaluators and returns the first non-nil `Decision`. Intended for wiring a `StaticEvaluator` + `DemoEvaluator` behind one `Evaluator` interface.
- **Fault watchdog** (`watchSlots` in `manteion_client.go`). 1 s ticker that reaps slots whose `lastConfirmedAt` is older than `max(3 * max(poll interval, hold), 30s)` (90 s with the default 10 s interval and 30 s long-poll hold; the hold counts because a held poll is answered, and the slots re-confirmed, about once per hold). Auto-started by `ConnectManteion` whenever the apply targets carry the demo evaluator. Independent of poll cadence so it still runs when the poll loop is wedged. Every answer that vouches for the held slots (a `200` that names them, a `304`, a no-document register answer) re-confirms them; `/admin/fault` slots are never confirmed and age out. Reaping a slot the last document confirmed also clears the held ETag, so the next poll is an unconditional full pull that re-applies the slot if it is still desired.
- **Poll-interval-in-register.** `RegisterRequest.PollIntervalMs` lets Manteion size its liveness threshold to each SDK's configured cadence.
- **`apply` reconciliation.** When `applyTargets.DemoEval` is set, `apply` reconciles the document's `active_faults` against currently-armed slots: drops slot IDs not in the document, applies the rest, and `Confirm`s them. Pairs with the watchdog: a confirmed slot survives until the server's lifetime ends or contact is lost long enough that the watchdog steps in. Manteion is authoritative when connected — see the doc on `faultAdminHandler`.
- **Per-slot admin DELETE.** `DELETE /admin/fault/{id}` clears one slot; `DELETE /admin/fault` still clears all.

### Changed — Fault wire format (multi-fault)

- **`RegisterResponse.ActiveFault` → `ActiveFaults []FaultRequest`**; **`FaultStatus.Fault` → `Faults []*FaultRequest`**. Manteion-side and admin-status payloads carry arrays now.
- **`FaultRequest` shape.** New fields: `id` (optional, defaults to category), `duration_ms` (lifecycle hint; `0 = infinite, whitelist enforced server-side`). The old top-level `delay`/`jitter`/`duration`/`status_code`/`message` fields are removed — inline configs now live inside `config` (`{"type":"latency","config":{"delay":"100ms"}}`, `{"type":"hang","config":{"duration":"2s"}}`, etc.), matching the compiled-rule envelope.
- **Manteion poll body** (`GET /api/v1/sdk/rules`) now carries `active_faults` and `freeze_cfg` alongside `rules`. `fetchRules` routes them through a single `apply` pass so the configured `NetworkResolver` is wired uniformly (previously rules were decoded twice and briefly published without the resolver — fixed).
- **Manteion offline mode** logs a warning at `ConnectManteion` time when `MANTEION_URL` is unset or `WithOfflineMode()` is in effect, instead of silently degrading.

### Fixed

- **Network rules briefly resolver-less on poll** — `manteion_client.fetchRules` no longer double-decodes/double-`SetRules`; `apply` is the single source of truth and `ruleVersion` only advances after it succeeds.
- **`DemoEvaluator.Evaluate` / `Active` map-iteration non-determinism** — slot IDs are sorted lex order so the chosen decision is stable across calls.
- **`internal/fault/resource/memory`: data races in thrashing mode** — workers now own disjoint chunk partitions (`{workerID, workerID+stride, ...}`) instead of all cycling through the full slice, eliminating concurrent `chunk[off] ^= 0xAA` on shared bytes. Workers are also stopped (`workerCancel(); thrashWg.Wait()`) *before* the ramp-down release loop mutates the slice, removing the read/write race on `chunks[i]`. Adds `TestStress_ThrashingWithRampDown` to lock in the ramp-down path under `-race`.

### Docs

- OpenAPI (`docs/swagger.{yaml,json}`) regenerated: `POST /admin/rules` documents its `204`/`400`/`422`/`500` answers, `GET /admin/cachebox` the delay state, and a `manteion-protocol` tag lists the desired-state poll (`GET /api/v1/sdk/rules`) and status report (`POST /api/v1/sdk/status`) the SDK calls (manteion serves them). Regenerate with `GOTOOLCHAIN=go1.25.5 make openapi`: swag v2 cannot parse the Go 1.27 standard library.
- `docs/plans/2026-05-17-concurrent-multi-fault-execution.md` — design note covering the current single-decision-per-`Evaluate` limitation and the three changes needed for true concurrent multi-fault execution (eager arming, per-slot lifecycle, slot-aware registry keys). Referenced from `DemoEvaluator.Evaluate` doc comment.

### Changed — fault wire format and rename

- **Renamed `network.Loss` → `network.RetransmitDelay`.** The toxic never dropped bytes (userspace TCP can't); it simulated the observable effect via per-chunk delay. The new name reflects what it actually does. Wire-format fault_type renamed `loss` → `retransmit_delay`; config field `retransmit_delay` → `delay`. RST reason string `packet_loss_threshold` → `consecutive_retransmit_threshold`. For true packet drop semantics, use kernel-level `tc netem`.
- **`CompiledFault` wire format restructured.** The opaque `Config json.RawMessage` blob is split into `Network *NetworkEnvelope` (host/target/direction/scope, only meaningful for network category) + `Params json.RawMessage` (toxic-specific params). Envelope is rejected on non-network categories; required on network.
- **Network host discriminator.** `NetworkEnvelope.Host` selects `"proxy"` (TCP proxy; needs a `NetworkResolver`) or `"inline"` (an armed policy on the atroposd sidecar's proxied upstream calls; the in-process host has no proxied connection and skips such rules per rule). An empty host means `inline` on the sidecar and `proxy` in-process.
- **Composition rules are skipped per rule** (logged at WARN, counted under `atropos_rules_unsupported_total{category="composition"}`) rather than failing the whole sync; the SDK has no composition evaluator yet.

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
