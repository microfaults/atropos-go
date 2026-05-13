# Atropos: Interventional Performance Attribution for Microservice Workflows

UCSC Faults Lab

---

## The Problem

Microservice workflows overlap on shared services. A checkout workflow and a browse workflow both hit `productcatalogservice` and `currencyservice`. When checkout tail latency degrades, the observed latency conflates three causally distinct phenomena: (1) intrinsic per-service processing cost, (2) application-level contention from other workflows competing for shared queues and connection pools, and (3) infrastructure-level coupling — CFS throttling, LLC evictions, memory bandwidth contention, veth bridge queueing — where co-located containers compete for physical resources regardless of workflow membership. These three sources interact nonlinearly. Shared services are queueing systems: the latency effect of concurrent workflows is strictly greater than the sum of their isolated effects. Higher application utilization increases CFS throttle probability for neighbors, which slows their responses, which increases upstream queueing. Cross-workflow interference and infrastructure coupling are entangled through shared utilization.

Observational methods cannot separate these. Distributed tracing (Jaeger, Tempo, Zipkin) records what happened but cannot attribute *why*. A trace shows that `productcatalogservice` took 45ms, but cannot distinguish whether that 45ms reflects the service's intrinsic cost, contention from concurrent browse requests, or CFS scheduling delays from a co-located batch job. Observational causal models (Sage, CIRCA) generate statistical counterfactuals over metrics and traces but cannot detect interference that is persistently present in the training distribution — if browse and checkout always run together, there is no observational contrast to learn from. Critical path analysis (CRISP, Mystery Machine) identifies bottleneck services from trace DAGs but cannot decompose a span into its causal constituents. All of these report on the system as-is. None can answer the counterfactual: "what would checkout latency be if browse traffic were absent?"

Existing interventional tools also miss the mark. Chaos engineering (Gremlin, LitmusChaos, Chaos Mesh) tests binary correctness under failure — does the system recover from a crashed pod, a network partition, a full disk? It does not test performance decomposition. Fault injection (Filibuster, LDFI, 3MileBeach) is interventional and uses the service-boundary interposition points we target, but tests correctness rather than continuous performance. Causal profiling (Coz, BCOZ) is the closest methodological analog — attributing performance via controlled intervention — but cannot cross the process boundary because virtual speedup requires direct control over thread scheduling. Load testing (k6, Vegeta, Locust) measures aggregate throughput and latency but treats the system as a black box. No existing method combines targeted traffic generation, per-workflow service isolation, and causal latency attribution in a single framework.

---

## The Cache-Box Primitive

The core research contribution is a new testing primitive called the **cache-box**. A cache-box is a service that has been switched from live operation to cached-response replay. The atropos SDK already sits at every service boundary (ingress and egress middleware). Extending it with a modal passthrough/replay toggle turns the same SDK that observes traffic into a mechanism that can "freeze" a service — replacing live computation with deterministic cached responses.

### Three Modes

- **Passthrough** (default): requests flow through normally. The SDK records request/response pairs in a keyed cache (method + path + normalized body hash).
- **Replay**: the SDK intercepts requests and returns the cached response without forwarding to the actual service. The service appears to function normally from the caller's perspective, but imposes zero actual load on the real implementation.
- **Replay-with-delay**: same as replay, but the SDK adds a configurable synthetic delay (e.g., the observed p50 or p99 of the real service). This separates "is the service slow because of contention?" from "is the service slow because it's intrinsically expensive?"

### Selective Call-Graph Freezing

```
Workflow: Checkout under load, with shared services frozen

  [frontend] ──► [productcatalog] ──► [currencyservice]
       │              ▲ FROZEN              ▲ FROZEN
       │              │ (cache-box)         │ (cache-box)
       │              │                     │
       ├──► [cartservice] ──────────────────┘
       │         (live)
       │
       ├──► [checkoutservice] ──► [paymentservice]
       │         (live)               (live)
       │
       └──► [shippingservice]
                 (live)

  (live)         Service processes requests normally under full
                 contention from all concurrent workflows.

  FROZEN         Service returns cached responses. No CPU, no
  (cache-box)    queueing, no contention contribution. Isolates
                 the workflow from this service's effect on
                 tail latency.
```

By freezing `productcatalog` and `currencyservice`, checkout traffic no longer contends with browse traffic on those shared services. The measured checkout latency now reflects only the live services. The difference between the all-live baseline and the partially-frozen experiment is the frozen services' contribution to checkout tail latency under that specific load mix.

### What Makes This Novel

No existing system combines these four capabilities:

1. **SDK-at-the-boundary with modal operation.** The same instrumentation point that records OTel spans and injects faults also controls the cache-box toggle. Filibuster comes closest (instrumenting HTTP clients to inject faults at boundaries) but does not do response replay. WireMock and Hoverfly do response replay but operate as external proxies without trace context awareness.

2. **Workflow-aware service graph topology.** Zeus-go knows which services each workflow touches (declared in flow JSON). This enables selective freezing of the services that participate in a specific workflow, guided by the call-graph DAG rather than manual configuration.

3. **Selective call-graph freezing during load.** Freeze `productcatalogservice` (replay cached responses at 50ms) while stress-testing `checkoutservice` at 1000 RPS. Chaos engineering can kill a service; service virtualization can stub a service; neither can freeze a service at its current observed behavior while the rest of the system runs under realistic load.

4. **Trace-correlated latency attribution under controlled isolation.** Every request carries a `meta-trace-id` via W3C Baggage. Cache-box mode transitions are recorded as span events. The trace itself becomes the evidence for the attribution claim — you can query for "all checkout traces where productcatalog was frozen" and compare their latency distribution against the all-live baseline.

### Relationship to LDFI

The cache-box is methodologically inspired by Lineage-Driven Fault Injection. LDFI asks "which faults could prevent success?" by reasoning backwards from successful executions — it identifies minimal fault sets that break correctness invariants. Cache-box asks "which services cause latency?" by reasoning backwards from end-to-end performance — it freezes services one at a time (or in combinations) to isolate their contributions to tail latency. LDFI removes components to find correctness bottlenecks. Cache-box removes contention to find performance bottlenecks. Both are interventional and use the service graph as the unit of analysis. The formal connection is looser than LDFI's — performance is a distribution, not a binary predicate, and the cache-box search space lacks LDFI's completeness guarantees under CALM. The analogy is structural, not formal.

### Interference Hierarchy and Experimental Controls

A span's observed latency is shaped by effects at four levels. The cache-box controls some directly, addresses others through experimental design, and is transparent to the rest.

| Level | Effect | Source | Cache-box control |
|-------|--------|--------|-------------------|
| L0 | Intrinsic processing cost | Service's own compute for the request | Removed by `replay`. Preserved by `replay-with-delay` (synthetic latency from observed distribution). |
| L1 | Within-service application contention | Goroutine scheduling, connection pool exhaustion, internal queueing from concurrent requests to the same service instance | Removed. Frozen service processes zero requests, eliminating all internal queueing. |
| L2 | Cross-service application contention | Backpressure propagation, timeout cascades, retry storms caused by one workflow's requests degrading a shared dependency | Removed for the frozen service's contribution. Downstream services no longer wait on real computation from the frozen service. |
| L3 | Infrastructure coupling (container) | CFS throttling, cgroup CPU quota exhaustion, memory pressure from co-located containers on the same node | **Confound.** Freezing a service reduces its resource footprint, which indirectly benefits co-located neighbors. This is a side effect of the intervention, not a controlled variable. |
| L4 | Infrastructure coupling (hardware) | LLC evictions, memory bandwidth contention, NUMA effects, NIC queue sharing, veth bridge queueing | **Not addressable.** These depend on physical co-location, not on application behavior. Cache-box operates above this level. |

**What cache-box measures.** The delta between an all-live baseline and a frozen-service experiment captures the combined effect of L0+L1+L2, with L3 as a confound and L4 as noise.

**Separating L0 from L1+L2.** Run each isolation experiment twice: once with `replay` (removes L0+L1+L2), once with `replay-with-delay` (preserves L0, removes L1+L2). The difference between the two deltas is the intrinsic processing cost (L0). The `replay-with-delay` delta is the contention-only contribution (L1+L2).

**Addressing the L3 confound.** Randomize service-to-node placement (pod anti-affinity or random scheduler) across repeated experiment runs. Effects consistent across random placements are attributable to the service graph (L1+L2). Effects that vary with placement are infrastructure-coupled noise (L3+L4). This follows the standard experimental design principle of randomizing nuisance variables. The `ExperimentRun` records each run's `NodePlacement` (service-to-node mapping) so the analysis layer can test for placement-dependent variation.

**What we do not claim.** We do not decompose infrastructure coupling (L3+L4) into per-service contributions. We treat it as noise to be averaged out, not signal to be attributed. The research question is about application-level contention (L1+L2) — which shared services are bottlenecks, and how do their contributions interact under load.

---

## Current State

### atropos-go — Instrumentation SDK

Go library that embeds into each service. Provides:

- OTel bootstrap (`Init`) with OTLP gRPC exporter, W3C TraceContext + Baggage propagation
- HTTP middleware (`IngressMiddleware`, `EgressTransport`) and gRPC interceptors (unary + streaming, separate `grpc/` subpackage)
- Always-on spans (`Span`) and fault-checked spans (`SpanWithFault`) — continuous trace coverage regardless of whether faults are active
- Rule evaluation engine (`Evaluator` interface) with label-based predicate matching
- Fault taxonomy: inline (latency, error, hang), network TCP proxy (RST, blackhole, retransmit_delay, latency, throttle, drip), resource stress (CPU with cgroup-aware detection, I/O with token-bucket rate control)
- Linear ramp-up/ramp-down phases on all faults for realistic degradation modeling
- `atropos.*` attribute namespace for clean coexistence with OTel semantic conventions
- **Cache-box engine (egress, HTTP)**: stdlib-only `internal/cachebox` package with LRU-bounded in-memory store, async recorder, three modes (`passthrough`/`replay`/`replay_with_delay`), three key strategies (`exact`/`exact_with_host`/`exact_with_body`), pluggable delay source (`ObservedDelaySource` + `DistributionDelaySource` lognormal scaffold for Stage 3). Dispatched via `EgressTransport` when the evaluator returns a `CacheBoxAction`. `StaticEvaluator` provides a label-matching rule list for use without a central controller.

### zeus-go — Load Generation and Attack Orchestration

Combines k6 sidecars with an Archer Go service:

- k6 runs declarative JSON flows (browse, checkout) with persona-based behavior profiles and step DAGs
- Archer launches targeted Vegeta attacks against specific endpoints with dedup bypass
- Policy engine evaluates rules on intervals, auto-triggers attacks when conditions are met
- All requests carry `meta-trace-id` via W3C Baggage for end-to-end trace correlation

### service-beds — Target Testbed

Online Boutique microservices ported to Go: frontend, productcatalog, currency, cart, checkout, payment, shipping, email, ad, recommendation. Deployed via Skaffold/Kubernetes.

---

## Where It's Headed

### Phase 1: Cache-Box Core — *egress shipped*

Egress HTTP cache-box is implemented in `internal/cachebox` and dispatched from `EgressTransport`.

- **Cache store.** In-memory LRU (stdlib `container/list` + map) with byte accounting, hit/miss/eviction counters, lazy TTL. Pluggable via `cachebox.Store` interface for future backends (BoltDB, badger, shared via manteion).
- **Modal middleware.** `EgressTransport` branches on `Decision.CacheBoxAction` returned by the evaluator. Three actions: `CacheBoxPassthrough` (forward + record), `CacheBoxReplay` (serve cached + fall through on miss), `CacheBoxReplayDelay` (replay + sleep for the sampled delay). `Interceptor.Check()` is split into `Evaluate()` + `StartFault()` so middleware layers can dispatch to fault or cache-box paths cleanly.
- **Request matching.** Three `KeyStrategy` values: `exact` (method+path+normalized query), `exact_with_host`, `exact_with_body` (FNV-1a of body). Query params are sorted alphabetically so `a=1&b=2` and `b=2&a=1` collide. Body hashing is opt-in and only buffers the body up to the configured cap.
- **Mutation safety.** The SDK does not restrict which HTTP methods can be replayed — the rule author is responsible for scoping cache-box rules to safe endpoints. Mutation-safety enforcement is a Stage 3 policy-engine concern.
- **Trace integration.** `atropos.cachebox.check` span wraps each cache-box dispatch with events: `atropos.cachebox.record` (on passthrough), `atropos.cachebox.replay` (on hit), `atropos.cachebox.miss` (on replay miss), `atropos.cachebox.oversize` (response exceeded the cap). Cache key surfaces as the `X-Atropos-Cache-Key` response header and the `atropos.cachebox.key` span attribute so traces can be joined against cache-entry telemetry.
- **Limits.** Ingress cache-box, gRPC cache-box, and manteion push of cache entries are intentionally deferred. See the Phase 1.5 and Phase 2 subsections below.

### Phase 1.5: Ingress Cache-Box for Cache-State Modeling

Egress cache-box freezes *dependencies* from a caller's perspective. Ingress cache-box freezes *the service's own compute* from its handlers' perspective — intercepting at the request boundary *before* the handler runs and returning a cached response. This is methodologically distinct and unlocks a different class of experiments.

**Why both.** The two positions along the interference hierarchy are duals:

| Position | What it freezes | What it parameterizes with `replay_with_delay` |
|----------|-----------------|-------------------------------------------------|
| Egress   | A dependency as seen by one caller. The dependency still runs for other callers. | The *dependency's observed latency* from this caller's side. |
| Ingress  | The service itself and its entire downstream fan-out. The handler never runs, dependencies never get called. | The *service's own internal cost*, including the warmth of its in-process caches, Redis hit ratio, JIT state, connection pool readiness. |

**Cache-state modeling via ingress replay_with_delay.** A service's end-to-end latency is often dominated by its internal cache state, not its own compute. Cold internal caches → misses → slower tier fetches → higher latency. Warm caches → hits → fast. Running a real cold-cache experiment requires warming up *nothing* on the service side while also warming up everything upstream (connection pools, JIT, page cache) — hard to do in practice without bespoke controls.

Ingress cache-box gives a clean counterfactual. By skipping the handler entirely and returning a recorded response after sleeping for a chosen latency, the experimenter can pin the service at an arbitrary point on its cache-warmth curve:

| Ingress delay | Models |
|---|---|
| 0 | Perfectly warm: every internal lookup hits |
| p25 of observed | Hot-path operation |
| p50 | Typical steady-state |
| p99 | Cold cache, most internal lookups miss |
| Lognormal sample | Realistic warm+cold mix |

**Metastable failure studies.** The classical metastable cascade is "cold cache → slow responses → timeouts → retries → more load → even colder cache." Ingress `replay_with_delay` with a swept latency parameter lets the experimenter pin a service in simulated cold-cache state and watch whether the workflow recovers or spirals into the cascade — the continuous-latency analog of LDFI's discrete fault search.

**Combined egress + ingress.** Together the two positions span a 2D experimental matrix over the dependency's apparent state and the service's own apparent state. The diagonal (ingress replay + egress replay on all deps) gives the "ideal workflow latency" floor; the baseline minus that is the total interference budget.

**Implementation cost.** Egress cache-box wraps `http.RoundTripper` — trivially composable. Ingress cache-box requires a `ResponseRecorder` wrapping `http.ResponseWriter` in `IngressMiddleware`, buffering the handler output up to the cap before writing it back. Streaming handlers (SSE, chunked multi-MB) cannot be cached and must fall through. The replay and replay_with_delay paths are cheaper than egress because the handler is entirely skipped.

**When.** Phase 1.5 — after egress cache-box produces its first experimental results, before OPA integration (Phase 3).

### Phase 2: Orchestration Integration

Zeus-go orchestrates cache-box experiments using workflow DAG topology.

- **Experiment protocol.** (1) Baseline run with all services live. (2) N isolation runs, each freezing one service. (3) Combination runs freezing pairs to detect interaction effects.
- **Per-workflow freezing.** `meta-trace-id` Baggage carries workflow identity. Cache-box mode activates per-workflow — browse requests to `productcatalog` get cached responses while checkout requests to the same service are forwarded to the real implementation.
- **Manteion coordination.** The control plane (roadmap P0) pushes cache-box mode changes to SDK instances, enabling centralized experiment coordination across the service mesh.

### Phase 3: Analysis Integration

- **Latency decomposition.** Each service is frozen twice: once with `replay` (total delta, L0+L1+L2 removed), once with `replay-with-delay` (contention delta, L1+L2 removed, L0 preserved). Per-service contribution:
  - `Δ_total = latency_baseline − latency_replay` (total contribution including intrinsic cost)
  - `Δ_contention = latency_baseline − latency_replay_with_delay` (contention-only contribution)
  - `intrinsic_cost = Δ_total − Δ_contention` (the service's inherent processing time under zero contention)
- **Interaction detection.** Compare sum-of-isolated-contributions to actual baseline. The difference quantifies superadditive interference — the nonlinear coupling that no observational method can detect.
- **Automated experiment planning.** Given a workflow DAG, generate the minimal set of freezing combinations needed to decompose all pairwise interactions.

### First Experiment

**Scope constraint.** The initial experiments freeze only stateless read-path services (productcatalog, currency, recommendation). Stateful services (cart, checkout) are analyzed indirectly — by freezing their stateless dependencies and measuring how checkout latency changes. Replaying cached responses for stateful services would produce stale state (e.g., a frozen cart returns items that were never added during the experiment), corrupting the latency measurements. Extending cache-box to stateful services requires session-aware caching, which is future work.

**Protocol.**

1. Deploy Online Boutique on Kubernetes with atropos SDK in all services. Use pod anti-affinity or random scheduler assignment to vary service-to-node placement across runs.
2. Run concurrent browse (increasing RPS: 100 → 2000) + checkout (constant 50 RPS) via zeus-go. **Baseline.** Record `NodePlacement` for this run.
3. Freeze `productcatalogservice` in **replay** mode. Re-run with fresh pod placement. **Isolation-1a** (total delta).
4. Freeze `productcatalogservice` in **replay-with-delay** mode (delay = observed p50 from baseline). Re-run. **Isolation-1b** (contention delta).
5. Repeat steps 3-4 for `currencyservice`. **Isolation-2a, 2b.**
6. Freeze both in replay mode. Re-run. **Combination-1.**
7. Repeat each isolation 3-5 times with randomized pod placement. Effects consistent across placements are service-graph attributable. Effects that vary are infrastructure noise.

**Compute:**
- `Δ_total(productcatalog) = baseline_p99 − isolation-1a_p99`
- `Δ_contention(productcatalog) = baseline_p99 − isolation-1b_p99`
- `intrinsic(productcatalog) = Δ_total − Δ_contention`
- `interaction = Δ_total(both) − (Δ_total(productcatalog) + Δ_total(currency))`

If `interaction > 0`, the services' contributions are superadditive — confirming that additive decomposition is wrong. If results are consistent across pod placements, the effect is application-level (L1+L2), not infrastructure-coupled (L3+L4).

---

## Policy-Driven Experiment Loop

### Why a Policy Engine

The cache-box primitive needs a mode toggle, not a rule engine — "freeze productcatalog for browse traffic" is a binary decision. But feedback-driven experiments need something richer. After running baseline and isolation experiments, the analysis layer (manteion) identifies patterns in exemplar traces: "checkout requests with >3 cart items when browse RPS exceeds 500 show 5x p99." That pattern needs to become a testable hypothesis — a policy that says "inject 200ms latency on egress calls matching these conditions, when system metrics confirm this state." The current evaluator (`Evaluate(ctx, Request) → *Decision`) cannot do this because it sees only per-request labels, not system-wide metrics.

### OPA as Embedded Policy Engine

Each atropos SDK embeds OPA via the Go library (`github.com/open-policy-agent/opa/rego`). The `Evaluator` interface does not change — a new `OPAEvaluator` implementation queries in-process OPA instead of matching labels against static predicates.

**Policy distribution.** Manteion publishes a bundle (Rego policies + JSON metrics data) to a single HTTP endpoint. Each SDK's embedded OPA polls that endpoint on a background goroutine. Manteion writes once, all SDKs pull. No direct manteion-to-SDK communication needed.

```
manteion ──publish──► bundle endpoint (HTTP)
                            ▲ poll every 5-30s (background, off request path)
SDK₁ (embedded OPA) ────────┘
SDK₂ (embedded OPA) ────────┘
SDKₙ (embedded OPA) ────────┘
```

**Evaluation cost.** `rego.PrepareForEval()` pre-compiles policies on bundle load. Per-request evaluation runs against the compiled representation: ~10-100us for simple match policies. System metrics (`data.system.*`) are loaded into memory from the bundle — no network call during evaluation. This is two orders of magnitude faster than a shared OPA service over the network (1-5ms per query), and negligible relative to inter-service call latency (1-50ms).

**Staleness.** Metrics in the bundle lag by one polling interval (configurable, e.g., 5-30s). This is acceptable — adversarial policies target sustained state transitions (CPU > 70% for multiple seconds), not instantaneous spikes.

### Three-Phase Experiment Loop

**Phase 1 — Baseline.** No active OPA policies. Atropos SDK traces all requests, records Prometheus metrics. Manteion collects per-service latency distributions, exemplar traces, resource utilization. Define what to collect: span attributes, histogram buckets, cgroup resource metrics.

**Phase 2 — Cache-box isolation.** Manteion publishes cache-box policies to the bundle:

```rego
package atropos.cachebox

freeze {
    input.service == "productcatalogservice"
    input.labels.workflow == "browse"
}
```

Each SDK queries OPA at ingress/egress. If `freeze` evaluates to true, the SDK returns a cached response instead of forwarding to the real service. Results: per-service latency contribution under controlled isolation.

**Phase 3 — Hypothesis-driven fault injection.** Manteion analyzes exemplar traces from phases 1-2. Identifies patterns and generates Rego policies encoding hypotheses:

```rego
package atropos.faults

import data.system   # live metrics pushed by manteion via bundle

inject_latency {
    input.point == "egress"
    input.labels.workflow == "checkout"
    to_number(input.labels.cart_items) > 3
    data.system.browse_rps > 500
}

fault_config := {
    "type": "latency",
    "duration_ms": 200,
    "jitter_ms": 50,
}
```

The `data.system.browse_rps` reference is the key differentiator: OPA conditions the decision on live system state, not just request labels. Manteion pushes updated metrics to the bundle periodically. The SDK's `OPAEvaluator` constructs a `Fault` from the returned config using existing factory functions (`NewLatencyFault`, etc.).

Observe whether degradation matches prediction. If it does, the hypothesis is confirmed — that request pattern under that system state causes the observed tail latency. If not, revise the policy and re-run.

### Adversarial Policy Search

Policies that react to system state create feedback loops. Example: "when CPU > 70% AND queue_depth > 100, inject latency on egress calls."

1. High application load drives CPU past the threshold.
2. OPA policy triggers latency injection on egress calls.
3. Callers time out and retry, increasing load further.
4. CPU rises further, triggering more faults.
5. Cascade.

This discovers failure modes that static chaos testing misses. The OPA policy is a replayable test case — re-run the same workload with the same policy bundle, get the same cascade. Safety is enforced via guard clauses in Rego: `data.system.error_rate < 0.5` prevents the policy from firing when the system is already in a failure state.

The analogy is fuzzing for distributed systems: the "input" is a policy + workload combination, and the "crash" is a cascading performance failure. The policy search space can be explored systematically — vary thresholds, swap fault types, target different services — with each run producing trace-correlated evidence.

### Manteion's Role

With OPA handling policy evaluation and distribution, manteion shifts from rule distributor to analysis and policy generator:

- Consume traces from the OTel collector.
- Compute per-service latency decomposition (baseline vs. isolation Deltas).
- Identify exemplar trace patterns that correlate with tail latency.
- Generate Rego policies from patterns (template-based or LLM-assisted).
- Publish bundles (policies + system metrics) to a single HTTP endpoint.
- Orchestrate experiment phases via zeus-go.

---

## Key References

- Alvaro, Rosen, Hellerstein. "Lineage-driven Fault Injection." SIGMOD '15. *The interventional testing methodology that cache-box dualizes.*
- Meiklejohn et al. "Service-Level Fault Injection Testing." SoCC '21. *Boundary instrumentation for fault injection; closest architectural precedent.*
- Zhang et al. "3MileBeach: A Tracer with Teeth." SoCC '21. *Combined tracing + request-level fault injection using baggage propagation.*
- Curtsinger & Berger. "Coz: Finding Code that Counts with Causal Profiling." SOSP '15. *Attributing performance via controlled intervention; limited to single-process.*
- Ahn et al. "BCOZ: Causal Profiling for Off-CPU Events." OSDI '24. *Extends causal profiling to off-CPU events but still cannot cross process boundaries.*
- Gan et al. "An Open-Source Benchmark Suite for Microservices." ASPLOS '19. *DeathStarBench; demonstrates cross-service QoS cascading.*
- Sigelman et al. "Dapper, a Large-Scale Distributed Systems Tracing Infrastructure." Google TR '10. *Foundation for distributed tracing.*
- Chow et al. "The Mystery Machine." OSDI '14. *End-to-end critical path analysis from traces.*
- ShapleyIQ. "Influence Quantification by Shapley Values for Performance Debugging of Microservices." ASPLOS '23. *Game-theoretic attribution; estimates characteristic function observationally rather than interventionally.*
- Open Policy Agent. "Policy-based control for cloud native environments." CNCF Graduated Project. *Embedded Go library for in-process policy evaluation with bundle-based distribution.*
