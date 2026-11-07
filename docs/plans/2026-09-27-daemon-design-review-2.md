# atroposd design review, second pass: open gaps and better alternatives

> **Status:** proposed, 2026-09-27. Companion to `2026-09-08-atropos-daemon-design.md` (accepted, M0/M1/M2/M4
> built on `feat/atroposd`). File:line references below are to `develop` (atropos-go `f2066ed`, manteion-go
> `a85ee9e`) unless marked *branch*; the daemon branch moved the SDK files under `internal/` without changing
> the mechanisms described.
>
> **Scope asked for:** the gaps left open from the original SDK design (`AGENTS.md` taxonomy, `VISION.md`
> policy loop), and better alternatives for the daemon on three specific questions: how an OPA policy engine
> would integrate; pull versus push for rule updates and cache-box sync; modern OpenTelemetry practice for
> trace-correlated logs.

## 0. Answer first

1. **Policy evaluation.** The original design promised two things the code never got: per-request matching
   over the JSON payload tree (`AGENTS.md`) and metric-conditioned policies (`VISION.md`, OPA bundles carrying
   `data.system.*`). Today the evaluator is an exact-label, first-match scan. Recommendation (ADR-1): add a
   compiled **CEL predicate** per rule at the edge for request-shaped conditions, keep **metric-conditioned
   automation central** in manteion (where the frozen `internal/policy` loop already lives and where
   ADR-2 makes its output converge to every instance), and adopt two OPA ideas without adopting OPA:
   bundle-style pull distribution and per-decision **decision logs**. OPA remains a drop-in `Evaluator`
   implementation if Rego authoring or a polyglot policy target becomes a goal; §2.4 says exactly how it
   would integrate so the choice can be reversed later.
2. **Distribution.** Rule delivery has three code paths (register response, poll snapshot, admin push) fed by
   two sources of truth (the DB plus an in-memory intent tracker) and they disagree in documented ways.
   Recommendation (ADR-2): one **materialized, per-instance desired state** with a monotonic generation,
   delivered by **long-poll pull** and acknowledged by a **status report** carrying the applied generation
   and the unsupported rules. Push survives only as a nudge. Cache-box preload becomes a pull of an
   immutable, checksummed artifact; ingest and drain stay edge-initiated. This subsumes M3: the capability
   filter runs once, in the materializer.
3. **Telemetry.** Traces exist; logs and metrics are not on OTel at all, background fault spans are root
   traces with no link to the request that triggered them, the collector has no processors and no logs
   pipeline, and there is no Loki. Recommendation (ADR-3): slog through the OTel logs bridge, every
   injection decision emitted as a trace-correlated event, span links from faults to the requests that
   caused and joined them, a collector pipeline with tail sampling that never drops a faulted trace, and
   exemplars on the Prometheus histograms. This is also the cheapest real implementation of the
   "request correlation" item in the original design.

## 1. Where the original design stands (facts, refreshed today)

| Original item (`AGENTS.md` §1–5, `VISION.md`) | State in code |
|---|---|
| JSON payload tree evaluated against a rule graph | Absent. Rules match exact label equality, first match wins (`internal/evaluator/static.go:42-57`); the only inputs are `atropos.http.{method,path,host,query,user_agent}` and the `atropos.workflow` baggage key (`internal/interceptor/middleware.go:90-110`). Bodies are buffered only for cache keys. |
| Metric-conditioned policies, OPA bundles (`VISION.md` §Policy loop) | Absent in the SDK (`README.md:49`). manteion's `internal/policy` is a PromQL threshold loop that pushes rule sets, frozen since 2026-06 (`docs/decisions/2026-06-policy-engine-freeze.md`). |
| Injection points inbound / outbound / transient / custom | Inbound, outbound, custom implemented. **Transient** parses but has no call site (`compiled_rule.go:190-191`). An empty `injection_point` decodes to ingress on the SDK while manteion documents it as "any point" (`model/rule.go:134`). |
| Fault types: CPU, memory, disk, I/O, GPU; latency, packet drops; duration | Resource faults and duration shipped. **GPU** absent by test. **Packet drops** absent: `loss` became `retransmit_delay` because it never dropped bytes (`CHANGELOG.md:41`). |
| OTel integration | Traces only. No MeterProvider, no LoggerProvider (`telemetry.go:37-143`). The inline `error` fault had no HTTP effect on `develop` (`internal/fault/inline/error.go`); fixed on the branch. |
| Observability via env or config file, to Prometheus + Loki + Tempo | Env only, and only `OTEL_EXPORTER_OTLP_ENDPOINT` / `COLLECTOR_SERVICE_ADDR`. Prometheus pull, no exemplars. No log export. No Loki in the bed. |
| Request correlation ("the few requests that led up to the fault") | Absent. No span links, no ring buffer, no correlation id. A background fault span is a root trace that carries the first request's labels only (`internal/interceptor/interceptor.go:181-216`). |

Defects found on the way, each already a gap the daemon inherits (fixes are folded into the ADRs or listed
in §5):

- The global `rule_version` is bumped by rule CRUD, fault-config fire/cancel and cache-box phases, but **not
  by fault-only phases** (`orchestrator/runner.go:153-158, 354-359`). Pollers keep getting 304, so those
  rules arrive only through the unacknowledged push.
- SSE carries a `rules_changed` nudge only from API mutations, never from the orchestrator; the broker drops
  events when its 16-slot buffer is full; there are no event ids and no resume (`api/sse_broker.go:59-68`).
- Push is one attempt, 2 s timeout, no retry, unordered (`atrocontrol/fanout.go:16-62`); the in-memory
  intent is lost on manteion restart and each command **replaces** the whole per-service intent
  (`atrocontrol/intent.go:11-50`). A phase push contains only that phase's rules; the poll snapshot contains
  all matching rules plus the synthesized cache-box rule: two different truths for the same instance.
- `POST /admin/rules` bypasses `apply()`: no drain observation, no version change (`rules_admin.go:55-72`).
- A composition rule makes the **whole** rule-set decode fail on the SDK (`compiled_rule.go:166-173`); an
  unsupported category is skipped per rule (*branch*). The two should behave the same.
- User-authored cache-box rules compile without a `Context`, so the SDK never records under them
  (`ruleconv.go:126-130`; `internal/interceptor/cachebox.go:160-165`).
- A pod restarted during a frozen phase gets no preload (preload is a one-shot push at phase entry,
  `orchestrator/runner.go:811-869`), so INV-1 turns every call into a fail-closed 503 for the rest of the
  phase. Rolling deploys are covered for rules (`docs/sdk-liveness.md`) but not for replay sets.
- Cache ingest dedupes on `(pair, instance, batch_seq)` with a `batch_seq` that restarts at 1 per process
  (`cache_push.go:195-197`): a restarted instance collides with its own earlier batches.
- Dashboards sum `atropos_cachebox_*` and `atropos_fault_injections_total` `by (service)`, but those series
  carry no `service` label (`service-beds/.../grafana-dashboards.yaml:693-705`); Grafana's `tracesToLogs`
  points at the Prometheus datasource.
- `AGENTS.md` still says `/admin/rules` takes `[]StaticRule` and `RegisterResponse` has `ActiveFault`; the
  code takes `[]CompiledRule` and returns `ActiveFaults` in a `RuleSync`.

## 2. ADR-1: where policy is evaluated, and how OPA would fit

**Context.** Two distinct needs hide under "policy engine": (a) per-request conditions richer than label
equality (payload fields, header sets, path patterns, percentages, time windows); (b) conditions on system
state ("when browse RPS > 500 and CPU > 70 %") that turn a hypothesis into a rule change. The daemon makes
(a) an out-of-process concern for the first time (the sidecar sees the bytes) and changes the economics of
(b): it is now one process per pod that already long-polls manteion.

The frozen `internal/policy` engine was the central form of (b). `VISION.md` proposed the edge form:
metrics shipped as bundle data to every SDK, each re-evaluating the same global condition with 5–30 s
staleness. The 2026-06 freeze decision already leans central ("belongs to the orchestrator FSM").

### 2.1 Options

| | A. Labels only, richer extraction | B. CEL predicate per rule (edge) | C. Embedded OPA / Rego (edge) | D. OPA as a separate sidecar (ext_authz style) |
|---|---|---|---|---|
| Expressiveness | Equality on precomputed labels; JSON pointers become labels | Full expressions over request, parsed body (lazy), baggage, local instance state | Full Rego, sets, partial eval, data documents | Same as C |
| Per-request cost | ns | µs (compiled program, no allocation on the hot path) | 10–100 µs typical; body must be decoded into `input` | + one local RPC per request (0.2–1 ms) |
| Binary / memory | none | ~5 MB, small | ~25–35 MB, tens of MB heap per bundle | separate container per pod |
| Authoring | manteion UI labels | UI gets one expression field; same `model.Rule` | Rego text, or manteion compiles rules to data under a fixed policy | Rego |
| Testing | none | manteion "evaluate against sample request" endpoint (cheap to add) | `opa test`, coverage, partial evaluation | same |
| Distribution | manteion's | manteion's (ADR-2) | OPA bundle protocol (ETag, long-poll, status, decision logs) | same |
| Polyglot | n/a | cel-cpp / cel-java exist; policies are strings | Rego → WASM for any host | any host over gRPC |
| Fit with G-proxy gate | trivially | yes | needs measuring; body decode dominates | no (extra hop) |

### 2.2 Recommendation

**B now, C reversible later, D never.** Concretely:

- `model.Rule.Match` gains `expr` (CEL, optional). manteion compiles it with `cel-go` at save time to
  reject bad expressions early and ships the source on the wire (`CompiledRule.match.expr`). The SDK
  compiles at apply time, once per rule, into a `cel.Program`; equality labels stay as the fast path and
  gate the expression (labels first, then `expr`). Activation: `request{method,path,host,query,headers}`,
  `body` (lazily parsed JSON, size-capped, only when the expression references it), `baggage`, `point`,
  `service`, `instance{id, host, capabilities, inflight, cpu_1m}`. Deterministic sampling is
  `hash(trace_id) % 100 < 10`, never `rand`, so a decision is reproducible from the trace.
- Metric-conditioned automation stays in manteion, rebuilt on the orchestrator FSM as the freeze decision
  says, but its **output is a desired-state change** (ADR-2), not a fanout push. The edge never sees global
  metrics; the only "system state" it evaluates is its own (`instance.inflight`, cgroup CPU), which needs no
  bundle and is exactly the state a global metric lags on.
- Every `Decision` is emitted as a **decision log** (ADR-3): one OTel log record per applied decision with
  rule name, reason, fault id, trace and span ids. This is the OPA feature worth having regardless of
  engine.
- The "graph built from rules" in `AGENTS.md` becomes an indexed matcher compiled by manteion: injection
  point → exact-label hash → priority-ordered predicate list. At tens of rules the linear scan is fine; the
  index matters only if rule sets grow to hundreds. Compositions (sequential / parallel members) are a
  dependency graph and belong to the orchestrator as phases, not to the per-request evaluator; the SDK
  should skip them per rule rather than fail the sync (§5).

### 2.3 Why not OPA now

Rego is a second rule language next to the manteion UI's model; the bed is Go-only so the WASM story buys
nothing yet; the binary and heap costs land in every service pod for the in-process host; and the metric
data documents `VISION.md` wanted in the bundle are the wrong place for global conditions once ADR-2 gives
manteion an acknowledged way to change the edge within a long-poll interval. What OPA does uniquely well
(policy testing, partial evaluation, decision logs, bundle protocol) is either reproducible cheaply
(decision logs, ETag long-poll) or not needed at this scale.

### 2.4 How OPA would integrate, if chosen (reversibility record)

- **Evaluator.** `OPAEvaluator` implements `Evaluate(ctx, *Request) *Decision` with a prepared query
  `data.atropos.decision` (`rego.PrepareForEval`, Rego v1 syntax, OPA ≥ 1.0). `input` = the activation
  above as JSON; `data.atropos.rules` = manteion's compiled rules; the policy shipped with the SDK is a
  fixed interpreter of that data (users do not write Rego) with an escape hatch action `rego` whose body is
  a user policy fragment.
- **Bundle server.** manteion serves `GET /api/v1/sdk/bundle?service&instance` as a tar.gz bundle with
  `.manifest` (revision = generation), honoring `If-None-Match`/long-poll; the sidecar's OPA plugin config
  points at it. Instance capabilities go into `data.instance` so the policy itself filters unsupported
  faults (M3 becomes data). Status API (`POST /api/v1/sdk/status`) carries the active revision, which is the
  acknowledgement ADR-2 asks for in any case.
- **Decision logs.** OPA's decision-log plugin posts each decision to manteion or, better, to the OTel
  collector through a small adapter, so decisions land next to spans.
- **Placement.** Sidecar only, behind the capability `policy=rego`; the in-process host keeps CEL. Budget:
  measure `atropos_proxy_overhead_seconds` p99 with and without body decoding before enabling on a bed.

### 2.5 Consequences

Easier: hypotheses that mention payload fields; deterministic per-request sampling; testable rules.
Harder: a body-referencing expression forces body buffering on egress (already done for cache keys) and
now on ingress; document the size cap. Revisit: OPA when a non-Go bed is targeted (§7 of the design doc
already defers Thrift/gRPC) or when experimenters want to author policies as text.

## 3. ADR-2: pull versus push for rules and cache-box sync

**Context (measured today).** Register returns the in-memory intent; poll returns a DB-derived snapshot
keyed on one global version that misses fault-only phases; SSE is a lossy nudge; push is best-effort and
unacknowledged, and the orchestrator has no per-instance observed state (it waits on drain reports and
pulls fidelity, nothing else). M3 has to apply the same capability filter in three places, which is the
symptom of the duplication.

### 3.1 Options

**A. Keep the hybrid, patch it.** Bump the version on every phase transition, add retries to push, dedupe
the filter in one helper. Cheapest; leaves two truths and no acknowledgement.

**B. Desired state, long-poll pull, status report (recommended).**
- manteion materializes `DesiredState(service, instance)` = f(enabled rules, running phases, active fault
  configs, freeze config, replay-set artifact, instance capabilities) with a per-service **generation**
  that increases on any input change (rule CRUD, phase FSM transitions, fault config, freeze) and an
  ETag = hash of the materialized document. This replaces the intent tracker; it is derivable from the DB,
  so a manteion restart is invisible.
- The instance long-polls `GET /api/v1/sdk/state?service&instance_id` with `If-None-Match`; manteion holds
  the request up to 30 s and answers on change (Consul blocking query / Kubernetes watch / OPA bundle
  semantics). Latency of a change is the wake-up, not the poll interval. `RuleSync` keeps its shape; it
  gains `generation` and an `unsupported`-free guarantee because the filter ran centrally.
- Each poll (or a `POST /api/v1/sdk/status`) carries **observed state**: `applied_generation`, `unsupported
  [{rule, category}]`, `errors`, cache-box counters. manteion stores it on `sdk_instances`; the UI shows
  which instances have converged; the orchestrator's phase-entry barrier becomes "every live instance
  reports `applied_generation >= target` within T", replacing the unacknowledged push and giving the M3
  exit criterion a server-side witness.
- Push is reduced to the SSE nudge (kept, now emitted by the materializer on every generation change) plus
  the kill switch, which also just bumps the generation.
- Cache-box: **preload becomes pull.** The replay set is an immutable artifact `{id, key_strategy, count,
  sha256, size}` referenced from desired state; the instance fetches it in idempotent chunks with `Range`
  and commits when the W5 checksum matches. Retries, parallelism across instances and a restarted pod
  mid-phase all fall out (it simply pulls the same artifact). **Ingest stays push** from the edge, batched,
  but keyed `(instance, epoch, seq)` where `epoch` is the registration time, so restarts cannot collide.
  **Drain stays edge-initiated**; the barrier waits on generation plus drain reports as today. The freeze
  delay source travels inside desired state instead of a separate `/admin/cachebox/delay` push, and its
  `context` stops being ignored.

**C. Bidirectional stream (xDS-like).** gRPC or WebSocket per instance, versioned resources, nonce and
ACK/NACK, deltas. The Envoy control-plane model; strongest semantics; a new transport and a delta protocol
for a fleet of tens of instances. Over-engineered here; B gives the same convergence guarantee with HTTP
and no new dependency.

### 3.2 Trade-offs

Pull-with-ack costs one held connection per instance and a materializer in manteion (a pure function of
DB state, easy to unit test). It removes the manteion→pod connectivity requirement, which is what breaks
docker-compose and any NAT'd or multi-cluster bed, and it removes the class of "push failed silently" bugs.
It does not by itself make the change atomic across instances; the barrier does that at phase entry, which
is the only place atomicity matters for experiments.

### 3.3 Action items

1. [ ] Materializer + generation column in manteion (`sdk_instances.observed_generation`,
   `observed_unsupported JSONB`); register response = materialized state. M3 lands inside this.
2. [ ] Long-poll on `/api/v1/sdk/rules` (keep the path; add `If-None-Match` and a hold), SSE emitted by the
   materializer.
3. [ ] SDK: report status on each poll; apply through the single `apply()` path so `/admin/rules` also
   observes drain.
4. [ ] Replay-set artifact endpoint + pull preload in the SDK; retire `PreloadService` push.
5. [ ] Ingest batch key `(instance, epoch, seq)`.

## 4. ADR-3: modern OpenTelemetry practice for trace-correlated logs

**Context.** `telemetry.go` sets a TracerProvider with OTLP/gRPC, `AlwaysSample`, and `service.name`,
`service.version`, `deployment.environment.name` on the resource (semconv 1.40). Logs are slog to stdout
without trace ids and `log.Printf` in the interceptors; metrics are hand-registered Prometheus series with
no exemplars and no resource attributes. The collector (contrib 0.100.0, May 2024) has no processors and no
logs pipeline; Tempo 2.6, Prometheus 2.53 with static targets, Grafana 11.6, no Loki.

### 4.1 Recommendations

1. **Logs as a signal.** Route slog through the OTel logs bridge
   (`go.opentelemetry.io/contrib/bridges/otelslog`) into a `LoggerProvider` (`otel/sdk/log`) with an OTLP
   log exporter; keep a stdout JSON handler as fallback. Every log call in request scope uses
   `InfoContext(ctx, …)` so `trace_id` and `span_id` are attached by the bridge. Replace the `log.Printf`
   calls in `internal/interceptor` and `grpc/`. Collector: add a `logs` pipeline to Loki through Loki's
   native OTLP endpoint (Loki 3.x), with `service.name`, `k8s.pod.name`, `deployment.environment.name`
   as stream labels and everything else as structured metadata.
2. **Decisions as events.** Each applied `Decision` becomes one log record with `event.name =
   atropos.decision` and attributes `atropos.rule.name`, `atropos.reason`, `atropos.fault.id`,
   `atropos.injection_point`, `atropos.experiment.id`, `atropos.phase.id`. Fault start, ramp and stop stay
   span events. This is the decision-log idea from OPA and the primary input to request correlation.
3. **Span topology.** A background fault's span currently starts from `context.Background()` and is a root
   trace. Start it with a **span link** to the triggering request span (`trace.WithLinks(trace.LinkFromContext
   (reqCtx))`) and stamp `atropos.fault.id` on the request span; when a later request joins the running
   fault, add a link on that request's span to the fault span and bump `atropos.fault.joined`. Then
   "which requests led up to the fault" is a TraceQL query (`{ span.atropos.fault.id = "…" }`) rather than a
   feature. For the last N requests before a fault fired, keep a 256-entry ring of `(trace_id, span_id,
   labels, ts)` per instance and emit up to 16 of them as links on the fault span; the rest go into the
   decision event as a list. Cheap, local, no new system.
4. **Baggage for experiment scope.** Propagate `atropos.experiment_id` and `atropos.phase_id` as W3C
   baggage from the injection point onward (the SDK already reads `atropos.workflow`); the collector copies
   them to span attributes (`baggage` → attribute via the `transform` processor) so every downstream span of
   an experiment is queryable. Allowlist the keys so baggage does not leak to external calls.
5. **Sampling.** Keep `AlwaysSample` in the SDK for the bed, but read the standard `OTEL_TRACES_SAMPLER` /
   `OTEL_TRACES_SAMPLER_ARG`; in the collector add `tail_sampling` with an always-keep policy for traces
   carrying `atropos.fault.*` or `status = error`, probabilistic for the rest, so fault evidence survives
   load-generator volume.
6. **Collector hygiene.** `memory_limiter` and `batch` processors; `k8sattributes` for pod, namespace and
   node on every signal (the sidecar's `service.instance.id` = pod name then agrees with the resource);
   `resourcedetection`; `servicegraph` next to `spanmetrics`; upgrade the contrib image (0.100.0 is two years
   old). Run it as a DaemonSet agent; a gateway is unnecessary at bed scale.
7. **Metrics.** Move the SDK series to the OTel `MeterProvider` with the Prometheus exporter so resource
   attributes ride along and exemplars come from the recording context; add `service` (or rely on the
   scrape `job`) to the cache-box and fault counters, and fix the dashboards that sum `by (service)`.
   Prometheus 3.x accepts OTLP natively if push is preferred later; pull stays the default.
8. **Configuration.** Replace the hand-rolled endpoint precedence with contrib's `autoexport` (honours
   `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER`, `OTEL_EXPORTER_OTLP_PROTOCOL`,
   `_INSECURE`, `_HEADERS`), and accept the standard OTel **declarative configuration file** through
   contrib's `otelconf` so a bed can ship one YAML. This closes the "env or config file" item in `AGENTS.md`
   with a standard rather than an atropos-specific format.
9. **Semantic conventions.** otelhttp v0.67 emits the stable HTTP conventions (`http.request.method`,
   `http.response.status_code`, `url.path`, `server.address`); make sure TraceQL queries and dashboards use
   those names, and pin the `semconv` import to one version across the SDK and the sidecar.
10. **Sidecar-specific.** The app's own stdout logs are collected by the collector's `filelog` receiver
    with `k8sattributes`, so a polyglot app whose logs already print the `traceparent` the sidecar injected
    correlates without SDK changes. The profiling signal (OTLP profiles from the OpenTelemetry eBPF
    profiler, Grafana Pyroscope) is the modern way to see a CPU `burn` or `quota` land in the right
    container; optional, mark as emerging.

### 4.2 Consequences

Easier: one query answers "what happened around this fault" (logs, decision events, linked spans, all
sharing the trace id); fault evidence survives sampling; the sidecar and the in-process host produce the
same log shape. Harder: one more exporter and pipeline to run; the bed manifests need Loki (or Grafana
Cloud) and a collector upgrade. Revisit: OTLP push for metrics if Prometheus static targets become a
maintenance problem.

## 5. Remaining gaps and their disposition

| Gap | Disposition |
|---|---|
| Transient injection point | Define it as "after the response is flushed": the ingress middleware wraps the writer, runs the fault after `WriteHeader`+body completion with a detached context, span linked to the request. Small; fits both hosts. |
| Packet drops, true latency at L3 | Done (`2026-10-30-agent-netem.md`); egress only, ifb for ingress deferred; `loss` and `delay` only (no `corrupt`), applied inside the pod's namespace on its egress interface rather than on the host-side veth. The node agent (M4) already has hostPID and privileges: add a `netem` mechanism that enters the pod's network namespace and applies `tc qdisc netem loss/delay/corrupt` on the pod's veth, journaled and restored like quotas. This is the first honest "packet drop" the platform can offer; the L4/L7 toxics stay for shaping without privileges. |
| GPU | Out of scope unless a bed has GPUs; NVML-based burn would be a node-agent mechanism. |
| Compositions rejected by the SDK, failing the whole sync | Skip per rule with a counted reason (same path as unsupported categories) until compositions are expanded by manteion into phases. |
| Version not bumped on fault-only phases; lossy SSE | Subsumed by ADR-2 (generation from the materializer). |
| `/admin/rules` bypassing `apply()` | Route through `apply()`; one-line fix, do it now. |
| Pod restart during a frozen phase | Subsumed by ADR-2 (pull preload of an immutable artifact). |
| Ingest `batch_seq` collisions | `(instance, epoch, seq)`; do it now. |
| User cache-box rules without context | Either compile a context from the rule's phase or reject at save; do it now. |
| Dashboards `by (service)`; `tracesToLogs` datasource | Fix with ADR-3 item 7 and the Loki datasource. |
| `AGENTS.md` drift (`[]StaticRule`, `ActiveFault`) | Update with the M3 docs. |
| Explicit `Serve` knobs for sampler/secure/HTTP exporter | Subsumed by ADR-3 item 8. |

## 6. Suggested order

1. Quick fixes from §5 (apply path, batch key, cache-box rule context, dashboards, docs).
2. ADR-2 in manteion with M3 inside it (materializer, generation, status). The exit criterion becomes
   observable in the UI instead of on a metric.
3. ADR-3 items 1–3 and 6 (logs bridge, decision events, span links, collector pipeline): the correlation
   feature the original design asked for, at the cost of instrumentation only.
4. ADR-1 CEL predicates once ADR-2 delivers rules with a generation, so a bad expression can be reported
   back as `unsupported` rather than failing a sync.
5. Node-agent `netem`; transient injection point.
