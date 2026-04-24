# atropos-go

Go SDK for fault injection, observability instrumentation, and request correlation in distributed systems.

Atropos embeds into your services as a library. It provides OpenTelemetry instrumentation out of the box, evaluates developer-defined rules to decide when and where to inject faults, and emits rich trace data so the effects of those faults are observable. The SDK is useful for pure observability even without fault injection configured.

```go
// Bootstrap OTel for the entire service (replaces manual TracerProvider boilerplate).
shutdown, _ := atropos.Init(ctx, atropos.WithServiceName("checkout"))
defer shutdown(context.Background())

// Instrument a code block with a span + fault injection check.
ctx, span, cr, _ := atropos.SpanWithFault(ctx, "process-payment",
    map[string]string{"tenant": "acme"},
    attribute.String("customer_id", id),
)
defer span.End()
if cr.Handle != nil { <-cr.Handle.Done() }
```

## Current State

A snapshot of what's shipped on `main`. For the research vision (cache-box primitive, interventional performance attribution, policy-driven experiments), see `VISION.md`.

| Area | Status |
|---|---|
| OTel bootstrap + HTTP/gRPC middleware | Shipped |
| Inline faults (latency, error, hang) | Shipped |
| Network faults (TCP proxy: RST, blackhole, loss, latency, throttle, drip) | Shipped |
| Resource faults (CPU, I/O, disk, memory) | Shipped |
| Cache-box (egress HTTP) — passthrough / replay / replay-with-delay | Shipped (Stage 1) |
| `FaultAdminHandler` — `/admin/fault` runtime fault control | Shipped |
| `CacheBoxAdminHandler` — `/admin/cachebox` stats, delay source, clear | Shipped |
| `RulesAdminHandler` — `/admin/rules` atomic rule swap | Shipped |
| `StaticEvaluator` with versioned atomic rule swap (`SetRules`) | Shipped |
| Prometheus metrics for HTTP ingress/egress | Shipped |
| SDK → manteion `Register` / `Apply` wire types | On `feat/admin-endpoints`, not yet merged |
| `CompiledRule` decoder for manteion wire format | On `feat/admin-endpoints`, not yet merged |
| Cache-box on ingress + gRPC | Not yet implemented |
| OPA-backed evaluator | Not yet implemented |
| Polyglot bindings (DeathStarBench target) | Design only (branch `chore/polyglot-bindings`) |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Service code                                                │
│                                                              │
│   atropos.Init()          -- OTel bootstrap                  │
│   atropos.Span()          -- always-on spans                 │
│   atropos.SpanWithFault() -- spans + fault check             │
│   atropos.IngressMiddleware()  -- HTTP server                │
│   atropos.EgressTransport()    -- HTTP client (+ cache-box)  │
│   atroposgrpc.UnaryServerInterceptor() -- gRPC               │
│   atropos.FaultAdminHandler()     -- /admin/fault            │
│   atropos.CacheBoxAdminHandler()  -- /admin/cachebox         │
│   atropos.RulesAdminHandler()     -- /admin/rules            │
│   atropos.MetricsHandler()        -- /metrics (Prometheus)   │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Interceptor layer          (internal/interceptor)           │
│                                                              │
│   Evaluator ─► Decision ─► Fault.Start() ─► Handle           │
│       │            │             │                           │
│       │       OTel span     EventAware                       │
│       │       + labels      emitter                          │
│       ▼                                                      │
│   No match? ──► hook span with fault.skipped event           │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Cache-box                   (internal/cachebox)             │
│                                                              │
│   passthrough  ──► forward to real upstream, record          │
│   replay       ──► serve cached response, zero upstream load │
│   replay_delay ──► serve cached response + synthetic latency │
│                                                              │
│   MemStore (LRU + TTL) · Recorder (async) · DelaySource      │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Fault taxonomy             (internal/fault)                 │
│                                                              │
│   inline/     Latency, Error, Hang                           │
│   network/    TCP proxy with toxics (RST, loss,              │
│               blackhole, drip, throttle, latency)            │
│   resource/   CPU stress, I/O stress, Disk, Memory stress    │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Trace layer                (internal/trace)                 │
│                                                              │
│   OTelTracer → global TracerProvider                         │
│   noopTracer → zero-cost when OTel is off                    │
│   atropos.* attribute namespace                              │
└──────────────────────────────────────────────────────────────┘
```

## Installation

```bash
go get atropos-go
```

The gRPC interceptors live in a separate subpackage so HTTP-only services never pull in `google.golang.org/grpc`:

```bash
go get atropos-go/grpc
```

## Quick Start

### 1. Bootstrap OTel

```go
import "atropos-go"

func main() {
    ctx := context.Background()
    shutdown, err := atropos.Init(ctx,
        atropos.WithServiceName("frontend"),
        atropos.WithServiceVersion("1.2.3"),
        atropos.WithEnvironment("staging"),
    )
    if err != nil { log.Fatal(err) }
    defer shutdown(ctx)

    // ...
}
```

`Init` sets up an OTLP gRPC exporter, W3C TraceContext + Baggage propagators, and a `TracerProvider`. Endpoint resolution: `WithEndpoint()` option > `OTEL_EXPORTER_OTLP_ENDPOINT` env > `COLLECTOR_SERVICE_ADDR` env > `localhost:4317`.

For tests or existing setups, bring your own provider:

```go
shutdown, _ := atropos.Init(ctx, atropos.WithTracerProvider(myTP))
```

### 2. HTTP Middleware

```go
mux := http.NewServeMux()
mux.HandleFunc("/api/checkout", checkoutHandler)

// Single middleware for both OTel request spans and fault injection.
handler := atropos.IngressMiddleware(mux, "checkout-service")
http.ListenAndServe(":8080", handler)
```

For outbound calls:

```go
client := &http.Client{
    Transport: atropos.EgressTransport(http.DefaultTransport),
}
resp, _ := client.Get("http://payment-service/charge")
```

### 3. gRPC Interceptors

```go
import atroposgrpc "atropos-go/grpc"

server := grpc.NewServer(
    grpc.UnaryInterceptor(atroposgrpc.UnaryServerInterceptor(atropos.DefaultInterceptor())),
    grpc.StreamInterceptor(atroposgrpc.StreamServerInterceptor(atropos.DefaultInterceptor())),
)
```

### 4. Custom Spans

For pure observability (no fault check):

```go
ctx, span := atropos.Span(ctx, "drain-queue",
    attribute.Int("queue_depth", len(q)),
    attribute.String("consumer_id", id),
)
defer span.End()

span.SetAttributes(attribute.Int("drained", count))
```

For spans with fault injection:

```go
ctx, span, cr, err := atropos.SpanWithFault(ctx, "process-payment",
    map[string]string{"tenant": "acme", "region": "us-east-1"},
    attribute.String("customer_id", id),
)
defer span.End()
if cr.Handle != nil {
    <-cr.Handle.Done() // wait for inline fault to finish
}
```

### 5. Enable Fault Injection

The SDK instruments spans by default. To activate faults, configure an evaluator:

```go
atropos.Configure(atropos.WithEvaluator(myEvaluator))
```

The `Evaluator` interface has a single method:

```go
type Evaluator interface {
    Evaluate(ctx context.Context, req Request) *Decision
}
```

If `Evaluate` returns nil, no fault fires. If it returns a `Decision`, the interceptor validates the fault, starts it, and records the result on the span.

For testing, demos, and manteion integration, use the built-in `StaticEvaluator` with atomic rule swap:

```go
eval := atropos.NewStaticEvaluator()
eval.SetRules([]atropos.StaticRule{{
    Name: "slow-checkout",
    Point: atropos.Ingress,
    Labels: map[string]string{"http.method": "POST"},
    Decision: atropos.Decision{ /* ... */ },
}})
atropos.Configure(atropos.WithEvaluator(eval))
```

### 6. Cache-Box (Egress)

Cache-box is a testing primitive that "freezes" a downstream dependency by replaying recorded responses instead of forwarding to the real service. Three modes, selectable per-request via `Decision.CacheBox`:

- **`CacheBoxPassthrough`** (default): forward to real upstream, record request/response pair asynchronously
- **`CacheBoxReplay`**: serve cached response immediately; zero upstream load
- **`CacheBoxReplayDelay`**: serve cached response after a synthetic delay (observed p50, or a fitted lognormal distribution)

```go
import "atropos-go/internal/cachebox"

cb := cachebox.New(
    cachebox.NewMemStore(cachebox.MemStoreConfig{MaxBytes: 64 << 20}),
    cachebox.WithKeyStrategy(cachebox.KeyStrategyExact),
    cachebox.WithDelaySource(cachebox.NewObservedDelaySource()),
)
atropos.Configure(atropos.WithCacheBoxCoordinator(cb))

client := &http.Client{
    Transport: atropos.EgressTransport(http.DefaultTransport),
}
```

Response headers on replay: `X-Atropos-Cache-Key`, `X-Atropos-Cache-Mode`, `X-Atropos-Cache-Latency-Us`. Cache misses fall through to passthrough. Responses larger than `DefaultMaxBodyBytes` (1 MiB) stream through unchanged without caching.

Stage 1 covers **egress HTTP only**. Ingress and gRPC are tracked in Ongoing Development.

### 7. Admin Handlers

Three `http.Handler` factories for runtime control. Mount on an internal admin mux, never on externally-exposed traffic.

#### `FaultAdminHandler()` — `/admin/fault`

Single-fault runtime control via a built-in `DemoEvaluator`. Suitable for demos and single-fault scenarios; not for rule libraries.

| Method | Body | Response |
|---|---|---|
| `GET` | — | 200 `{"active": bool, "fault": {...}}` |
| `POST` | fault request JSON | 201 `{"active": true, "fault": {...}}` |
| `DELETE` | — | 200 `{"active": false}` |

POST body fields by `type`:

| `type` | Required | Optional | Defaults |
|---|---|---|---|
| `latency` | `delay` (e.g. `"500ms"`) | `jitter` (duration) | — |
| `error`   | — | `status_code` (int), `message` (string) | 500, `"injected fault"` |
| `hang`    | `duration` | — | — |

```go
mux.Handle("/admin/fault", atropos.FaultAdminHandler())
```

```bash
curl -X POST localhost:8080/admin/fault -d '{"type":"latency","delay":"500ms"}'
curl localhost:8080/admin/fault
curl -X DELETE localhost:8080/admin/fault
```

#### `CacheBoxAdminHandler(cb)` — `/admin/cachebox`

Runtime cache-box control: read stats, replace the delay source with a fitted lognormal distribution, or clear the store.

| Method | Path | Body | Response |
|---|---|---|---|
| `GET` | `/admin/cachebox` | — | 200 `Stats` JSON |
| `POST` | `/admin/cachebox/delay` | `{"mu":float, "sigma":float, "seed"?:uint64}` | 204 |
| `DELETE` | `/admin/cachebox` | — | 204 (store cleared; lifetime counters preserved) |

```go
mux.Handle("/admin/cachebox", atropos.CacheBoxAdminHandler(cb))
mux.Handle("/admin/cachebox/", atropos.CacheBoxAdminHandler(cb))
```

#### `RulesAdminHandler(eval)` — `/admin/rules`

Runtime rule-set management backed by a `StaticEvaluator`. POST atomically replaces the entire rule list.

| Method | Body | Response |
|---|---|---|
| `GET` | — | 200 `[]StaticRule` JSON |
| `POST` | `[]StaticRule` JSON | 204 |

```go
eval := atropos.NewStaticEvaluator()
atropos.Configure(atropos.WithEvaluator(eval))
mux.Handle("/admin/rules", atropos.RulesAdminHandler(eval))
```

### 8. Prometheus Metrics

```go
mux.Handle("/metrics", atropos.MetricsHandler())
```

Exposes `http_server_requests_total`, `http_server_request_duration_seconds`, `http_client_requests_total`, `http_client_request_duration_seconds` — each labeled by method, path, status, and service.

## Design Decisions

### Always-on spans

Hook points (`Span`, `SpanWithFault`, middleware) always create OTel spans regardless of whether a fault fires. This gives continuous trace coverage during normal operation and makes the SDK useful for pure observability. When a fault does fire, its span nests as a child of the hook span, preserving parent-child relationships.

When no fault fires, a `fault.skipped` event is recorded on the hook span. When a fault fires, a `fault.injected` event is recorded instead. This makes it trivial to query for "all requests that were affected by faults" in your trace backend.

### Events over spans for network and resource faults

Network faults operate at the TCP level (proxy, RST, blackhole, packet loss). Creating child spans per-connection when the connection itself might be reset or blackholed produces misleading trace data: a span implies a successful lifecycle, but these faults deliberately break that assumption.

Instead, network and resource faults implement the `EventAware` interface. The interceptor injects an event emitter before starting the fault, and the fault emits timestamped events on the parent `fault.inject` span at key moments:

**Network events:** `conn.accepted`, `toxic.hijack`, `upstream.dial`, `conn.error`, `conn.closed`
**Resource events:** `ramp_up.start`, `ramp_up.complete`, `sustain.start`, `ramp_down.start`, `ramp_down.complete`

This gives precise timing correlation without implying a "successful" span lifecycle for something that is by definition breaking.

### Fault lifecycle: linear ramp phases

All faults share a base `FaultConfig` with `Duration`, `RampUp`, and `RampDown`. Resource faults (CPU, I/O, memory, disk) linearly scale intensity during ramp phases. This models real-world degradation patterns better than instant on/off, and lets you observe how services behave under gradually increasing pressure.

### Label threading

Labels passed to `SpanWithFault` or extracted by middleware (`http.method`, `grpc.method`, etc.) serve two purposes:

1. **Evaluator matching:** The rule engine matches predicates against labels to decide if a fault should fire.
2. **Span attributes:** The same labels are recorded as span attributes so they appear in your trace backend.

This avoids the common gap where rules match on context that never shows up in traces.

### Injection point taxonomy

Faults can fire at four points:

| Point | Where | Example |
|-------|-------|---------|
| **Ingress** | Inbound request hitting the service | HTTP middleware, gRPC server interceptor |
| **Egress** | Outbound call to a dependency | HTTP transport, gRPC client interceptor |
| **Transient** | After request completion (side effects) | Background jobs, queue consumers |
| **Custom** | Developer-annotated code block | `SpanWithFault("process-payment", ...)` |

### Execution modes

Faults run in one of two modes:

- **Inline:** Blocks the request until the fault completes. Used for latency, error, and hang faults where the caller should experience the delay.
- **Background:** Runs independently of the request lifecycle. Used for resource faults (CPU, I/O, memory, disk) and network proxies that outlive individual requests.

### Cache-box never blocks the hot path

The cache-box recorder uses a bounded channel with an async drain goroutine. When the channel is full (e.g., a burst of unique requests faster than the recorder can persist), new entries are dropped rather than blocking the request path. Replay reads are O(1) against the in-memory LRU. Oversized responses (> 1 MiB) stream through unchanged via `io.MultiReader` without caching.

### gRPC in a separate subpackage

The `grpc/` subpackage avoids forcing a `google.golang.org/grpc` dependency on HTTP-only services. Import `atropos-go/grpc` only when you need gRPC interceptors.

## Fault Types

### Inline

| Type | Behavior |
|------|----------|
| **Latency** | Sleeps for configured duration with optional jitter |
| **Error** | Returns immediately with configured HTTP status code and message |
| **Hang** | Blocks until context cancellation or duration expires (application-level blackhole) |

### Network (TCP Proxy)

The network proxy sits between client and upstream, applying toxic effects to the TCP stream:

| Toxic | Effect |
|-------|--------|
| **Blackhole** | Accepts connections, never responds |
| **RST** | Sends TCP reset |
| **Loss** | Drops packets at configured rate |
| **Latency** | Adds stream-level delay |
| **Throttle** | Rate-limits to configured bytes/sec |
| **Drip** | Writes data in tiny chunks with pauses |

### Resource

| Type | Mechanism |
|------|-----------|
| **CPU** | Duty-cycle spinning across goroutines pinned to OS threads. Detects CPU quota from cgroup (Docker/k8s) or `runtime.NumCPU()`. Maps to iBench SoI11/12/15 (integer, FP, vector pressure). |
| **I/O** | Creates random files and reads them at controlled rate using a shared token bucket. |
| **Memory** | RAM hogger that allocates and touches pages to exert real memory pressure. Ramp phases scale the allocation curve. |
| **Disk** | Controlled disk-fill and I/O-rate faults targeting a working directory. |

All four support linear ramp-up and ramp-down phases.

### Cache-box (testing primitive, not a failure mode)

| Mode | Effect |
|------|--------|
| **Passthrough** | Normal operation; records request/response pairs for later replay |
| **Replay** | Serves cached response; zero upstream CPU, zero queueing |
| **Replay-with-delay** | Serves cached response after synthetic latency (observed or fitted distribution) |

See `VISION.md` for the full cache-box experimental methodology.

## Attribute Namespace

All attributes are prefixed with `atropos.` for clean coexistence with standard OTel semantic conventions. Constants live in `internal/trace/attrs.go`.

| Prefix | Attributes |
|--------|------------|
| `atropos.fault.*` | type, injection_point, reason, duration_ms, actual_duration, detail |
| `atropos.hook.*` | name |
| `atropos.http.*` | method, path, host, user_agent |
| `atropos.grpc.*` | method, user_agent |
| `atropos.net.*` | conn.id, conn.remote_addr, conn.affected, upstream.addr, dial_duration_ms, bytes_up, bytes_down |
| `atropos.resource.*` | target_load, target_rate, ramp_up_ms, ramp_down_ms |
| `atropos.cachebox.*` | key, mode, hit, observed_latency_us, synthetic_latency_us, oversize |

## Init Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithServiceName(name)` | `"unknown"` | `service.name` resource attribute |
| `WithServiceVersion(v)` | `""` | `service.version` resource attribute |
| `WithEnvironment(env)` | `"development"` | `deployment.environment` resource attribute |
| `WithEndpoint(addr)` | `localhost:4317` | OTLP collector address |
| `WithInsecure(bool)` | `true` | Use plaintext gRPC (disable TLS) |
| `WithSampler(s)` | `AlwaysSample` | Custom `sdktrace.Sampler` |
| `WithTracerProvider(tp)` | builds one | Bring your own `TracerProvider` |

## Configure Options

Wire up the package-level default interceptor via `atropos.Configure(opts ...ConfigureOption)`:

| Option | Purpose |
|--------|---------|
| `WithEvaluator(e Evaluator)` | Attach a rule-matching evaluator to drive fault decisions |
| `WithCacheBoxCoordinator(cb *cachebox.CacheBox)` | Enable egress cache-box (passthrough/replay/replay-with-delay) |

Call `Configure` after `Init` and before serving traffic. Each option replaces its slot; missing options keep their existing wiring.

---

## Ongoing Development

Active work in flight. Branches are called out where relevant.

### SDK ↔ manteion wire protocol (on `feat/admin-endpoints`, not yet merged)

The next merge brings the SDK-side of the manteion integration:

- `RegisterRequest` / `RegisterResponse` wire types for SDK startup registration
- `Register(ctx, url)` — `POST /api/v1/sdk/register` against the manteion control plane
- `Apply(resp *RegisterResponse)` — install intent state (active fault, compiled rules, cache-box config) returned by manteion
- `CompiledRule` / `CompiledComposition` / `CompiledFault` decoders for manteion's wire format (inline, network, resource categories — all six toxic types, all four resource types)
- `FaultRegistry` in the interceptor with dedup-by-rule + graceful shutdown so multiple overlapping rules don't start duplicate background faults
- `StartPolicy` on `Decision` so non-inline faults can force Background execution cleanly

Already exposed on main: `RulesAdminHandler` (manteion can push rules via HTTP right now), `CacheBoxAdminHandler` (manteion can swap the delay source with a fitted lognormal). The missing pieces above are for the SDK-initiated path (SDK registers with manteion on startup, pulls rules via register response) rather than the manteion-initiated push path that already works.

### Cache-box Stage 2 and 3

**Stage 2** — extend the modal dispatch to ingress HTTP and gRPC. The `Decision.CacheBox` field, key strategies, store, and recorder are already protocol-agnostic — only the middleware/interceptor glue is missing.

**Stage 3** — wire the recorder's `PushFunc` hook so that:
- Cached entries flow from the SDK to manteion asynchronously (for cross-pod replay consistency)
- Fitted distribution parameters (lognormal mu/sigma) flow from manteion back to the SDK, replacing the in-process observed-delay source with a statistical one — the `POST /admin/cachebox/delay` endpoint is already shipped; manteion just needs to call it.

### Polyglot bindings for DeathStarBench

Branch: `chore/polyglot-bindings`. Design doc: `docs/plans/2026-04-19-polyglot-bindings.md`.

DeathStarBench has 10 languages but only 3 RPC protocols (Thrift, gRPC, HTTP). The plan is a protocol-aware sidecar proxy (`atropos-proxy` binary reusing this codebase) that target services talk to via `localhost:PROXY_PORT`. One implementation per protocol covers every language — no per-language SDK port required for the baseline case.

---

## Not Yet Implemented

Features in scope for the research agenda (`VISION.md`) but with no code yet.

### Cache-box on ingress + gRPC

Egress HTTP works today (Stage 1). The ingress middleware and gRPC interceptors need the same modal dispatch. Protocol-agnostic primitives (`Decision.CacheBox`, key functions, store, recorder) are already in place.

### OPA-backed evaluator

The `Evaluator` interface is deliberately minimal so it can be backed by multiple implementations. An `OPAEvaluator` using `github.com/open-policy-agent/opa/rego` would let manteion publish Rego policy bundles (policies + system metrics as JSON data) that the SDK polls and evaluates in-process. See `VISION.md` § Policy-Driven Experiment Loop for the argument and the adversarial policy search framing.

### Topology-aware span enrichment

Enrich spans with cloud-provider metadata (availability domain, fault domain, region, rack, host ID) from IMDS so trace queries can reason about physical topology ("show me all traces that crossed fault domain boundaries"). Reading the metadata is a handful of HTTP calls; designing the enrichment schema to distinguish fault-domain failure from TOR-switch failure is the interesting part.

### Scoped attributes with schema constraints

A schema pushed by manteion that defines allowed attribute keys, cardinality bounds per key per time window, temporary attributes with lifetimes, and type constraints. Mitigates the common Jaeger/Tempo problem where unconstrained attribute cardinality blows up storage and query performance.

### Configuration version correlation

Services report their current config version / feature-flag state as a resource attribute; manteion tracks version transitions and timestamps them; a Grafana annotation query overlays these on trace latency panels so "config rolled at 14:32" lines up visually with "p99 doubled at 14:33."

### Cross-layer event correlation

Atropos spans carry a correlation ID that links to metrics exemplars and structured log entries. When a fault fires, it records the correlation ID in the span, emits a metric exemplar with the same ID, and writes a structured log line with the ID — so a query engine can join across all three stores for a single incident.

### eBPF probes for non-Go services

Complement language SDKs / the protocol proxy with kernel-level syscall instrumentation (connect, accept, read, write, close). Provides infrastructure-level signals (syscall latency, connection counts, TCP retransmits) that application-level instrumentation cannot see. Open question is correlating eBPF events with OTel spans in the same process.

---

## Future Directions

Longer-horizon ideas explored in `VISION.md` and the broader faults-lab agenda.

**Interventional performance attribution.** The cache-box primitive is the core; the surrounding methodology (baseline → isolation → combination experiments, two-mode decomposition, pod-placement randomization for infrastructure confounds) is detailed in `VISION.md`. A workshop paper draft lives in `docs/paper-intro-draft.md`.

**Adversarial policy search.** OPA policies that react to live system metrics create feedback loops — "when CPU > 70% and queue_depth > 100, inject egress latency" — that surface cascading failures invisible to static chaos testing. The policy itself becomes a replayable test case.

**Ecosystem gaps worth addressing.** Several pain points in Jaeger, Zipkin, Tempo, and the OTel Collector that atropos is positioned to address — programmatic trace diff for A/B experiments, tail-based sampling with cross-span context, root cause ranking with fault state + config versions + topology boundaries, live dependency health scores from egress interceptors, service graph with fault annotations, and tighter chaos-tracing integration than the current Chaos Toolkit + OTel stitching. Natural follow-on work once the control plane (manteion) is fully wired.

---

## Cross-Language Portability

The public API is intentionally minimal so language bindings can stay thin. For polyglot meshes (DeathStarBench, Social Network), the preferred path is the protocol-aware sidecar proxy described in `docs/plans/2026-04-19-polyglot-bindings.md` — not a full native SDK port per language.

| Go (today) | Python | Java | Node/TS |
|----|--------|------|---------|
| `Init(ctx, opts...)` | `init(**opts)` | `Atropos.init(opts)` | `init(opts)` |
| `Span(ctx, name, attrs...)` | `@atropos.span("name", **kw)` | `@AtroposSpan("name")` | `atropos.span("name", attrs, fn)` |
| `IngressMiddleware(h)` | WSGI/ASGI middleware | Servlet filter | Express middleware |
| `EgressTransport(rt)` | `requests.Session` adapter | `OkHttp` interceptor | `fetch` wrapper |

---

## Related Repos

- **`manteion-go`** — central control plane (rules, SDK registration, policy distribution, zeus proxy)
- **`manteion-ui`** — React admin UI on top of manteion
- **`zeus-go`** — k6 workload runner and Archer attack orchestrator
- **`service-beds`** — Online Boutique microservices in Go, target testbed
