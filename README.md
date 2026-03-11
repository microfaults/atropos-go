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

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Service code                                           │
│                                                         │
│   atropos.Init()          -- OTel bootstrap             │
│   atropos.Span()          -- always-on spans            │
│   atropos.SpanWithFault() -- spans + fault check        │
│   atropos.IngressMiddleware()  -- HTTP server            │
│   atropos.EgressTransport()    -- HTTP client            │
│   atroposgrpc.UnaryServerInterceptor() -- gRPC          │
│                                                         │
├─────────────────────────────────────────────────────────┤
│  Interceptor layer          (internal/interceptor)      │
│                                                         │
│   Evaluator ─► Decision ─► Fault.Start() ─► Handle     │
│       │            │             │                      │
│       │       OTel span     EventAware                  │
│       │       + labels      emitter                     │
│       ▼                                                 │
│   No match? ──► hook span with fault.skipped event      │
│                                                         │
├─────────────────────────────────────────────────────────┤
│  Fault taxonomy             (internal/fault)            │
│                                                         │
│   inline/     Latency, Error, Hang                      │
│   network/    TCP proxy with toxics (RST, loss,         │
│               blackhole, drip, throttle, latency)       │
│   resource/   CPU stress, I/O stress                    │
│                                                         │
├─────────────────────────────────────────────────────────┤
│  Trace layer                (internal/trace)            │
│                                                         │
│   OTelTracer → global TracerProvider                    │
│   noopTracer → zero-cost when OTel is off               │
│   atropos.* attribute namespace                         │
└─────────────────────────────────────────────────────────┘
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

// Add attributes as you learn more.
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

The SDK instruments spans by default. To activate faults, provide an evaluator:

```go
atropos.Configure(myEvaluator)
```

The `Evaluator` interface has a single method:

```go
type Evaluator interface {
    Evaluate(ctx context.Context, req Request) *Decision
}
```

If `Evaluate` returns nil, no fault fires. If it returns a `Decision`, the interceptor validates the fault, starts it, and records the result on the span.

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

All faults share a base `FaultConfig` with `Duration`, `RampUp`, and `RampDown`. Resource faults (CPU, I/O) linearly scale intensity during ramp phases. This models real-world degradation patterns better than instant on/off, and lets you observe how services behave under gradually increasing pressure.

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
- **Background:** Runs independently of the request lifecycle. Used for resource faults (CPU, I/O) and network proxies that outlive individual requests.

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

Both support linear ramp-up and ramp-down phases.

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

## Cross-Language Portability

The public API is intentionally minimal and portable across language bindings:

| Go | Python | Java | Node/TS |
|----|--------|------|---------|
| `Init(ctx, opts...)` | `init(**opts)` | `Atropos.init(opts)` | `init(opts)` |
| `Span(ctx, name, attrs...)` | `@atropos.span("name", **kw)` | `@AtroposSpan("name")` | `atropos.span("name", attrs, fn)` |
| `IngressMiddleware(h)` | WSGI/ASGI middleware | Servlet filter | Express middleware |
| `EgressTransport(rt)` | `requests.Session` adapter | `OkHttp` interceptor | `fetch` wrapper |

---

## Roadmap

### P0: Manteion -- Rule Coordination Oracle

A central service that pushes rule and configuration changes to SDK instances. Each atropos-go SDK registers with manteion on startup. Manteion:

- Pushes evaluator rule updates (fault definitions, targeting predicates, sampling rates) to registered SDKs in real time.
- Coordinates global-level properties: kill switches, blast radius caps, experiment enrollment.
- Probes each SDK instance for health, confirming the agent is reachable and responsive.
- Provides a control plane for operators to stage experiments, review active faults, and emergency-stop.

This is the piece that turns atropos from a library into a platform. Without it, every service needs its own static rule configuration.

### P1: Topology-Aware Span Enrichment

Cloud providers (OCI, AWS, GCP) expose instance metadata: availability domain, fault domain, region, rack, host ID. Enriching spans with these attributes enables queries like "show me all traces that crossed fault domain boundaries" or "which AZ was involved in this latency spike."

The hard part is not reading the metadata (that is a handful of HTTP calls to IMDS). The hard part is knowing which attributes directly correlate to lower-level infrastructure faults. A fault domain failure looks different from a TOR switch failure, but both present as "some requests are slow." The enrichment schema needs to capture enough topology to distinguish these failure modes in trace queries without requiring the developer to understand the physical infrastructure.

Relevant prior art: Jaeger's long-standing request for service dependency topology ([jaegertracing/jaeger#782](https://github.com/jaegertracing/jaeger#782)), Tempo's service graph feature, and the OpenTelemetry resource semantic conventions for cloud infrastructure.

### P2: Response Caching and Replay

Cache responses at egress points so that when a fault is injected (or a downstream is genuinely down), the service can replay the last known good response instead of propagating the failure. This turns atropos into a tool that can both inject faults and partially mitigate them, which is valuable for testing graceful degradation paths.

Design considerations:
- Cache keying strategy (method + path + normalized query? Request hash?)
- TTL and invalidation (stale data is its own fault mode)
- Size bounds (memory pressure from caching is itself a resource concern)
- Selective caching (not all endpoints are safe to replay -- mutations, side effects)

### P3: Scoped Attributes with Schema Constraints

Developer-added attributes like `queue_depth` and `consumer_id` are powerful for trace correlation, but unconstrained attribute cardinality pollutes the trace database. High-cardinality attributes (user IDs, request IDs) can blow up storage costs and degrade query performance in backends like Tempo and Jaeger.

This needs a schema that defines the design space:
- **Allowed keys:** Whitelist of attribute names per service or globally.
- **Cardinality bounds:** Max distinct values per key per time window.
- **Lifetime:** Temporary attributes that are recorded for N minutes during an experiment, then automatically stop being emitted.
- **Type constraints:** Enforce that `queue_depth` is always an int, `tenant` is always a string from a known set.

The schema could be pushed by manteion alongside rule updates, giving operators control over what gets recorded without code changes.

Related ecosystem pain: Jaeger has open issues around attribute-based filtering and trace search performance degradation with high cardinality ([jaegertracing/jaeger#2083](https://github.com/jaegertracing/jaeger#2083)). The OpenTelemetry Collector has a `filter` processor, but it operates on spans not individual attributes, and does not support cardinality-aware decisions.

### P4: Configuration Version Correlation

Services are redeployed constantly. A latency spike that correlates with a config change in an upstream service is a different problem than one that correlates with a code deploy. Recording timestamps of version changes across services -- config version, binary version, feature flag state -- and surfacing them in trace dashboards as vertical markers gives operators an immediate "what changed?" signal.

Implementation: each service reports its current config version as a resource attribute. Manteion tracks version transitions and timestamps them. A Grafana annotation query overlays these on trace latency panels.

This addresses a real gap in existing tools. Jaeger, Zipkin, and Tempo all show you what happened but not what changed. The correlation between "config rolled at 14:32" and "p99 latency doubled at 14:33" is currently a manual exercise.

### P5: eBPF Probes for Non-Go Services

The current SDK is Go-only. For polyglot environments, eBPF probes can instrument services without language-specific SDKs by attaching to kernel-level syscalls (connect, accept, read, write, close). This gives TCP-level visibility and basic fault injection (packet delay, drop, corruption) for any process.

eBPF probes would complement language SDKs rather than replace them: they provide infrastructure-level signals (syscall latency, connection counts, TCP retransmits) while SDK spans provide application-level context (which handler, which tenant, which business operation).

The open question is how to correlate eBPF events with OTel spans. One approach: eBPF probes tag events with the thread ID and timestamp, and a userspace correlator matches them to active spans in the same process using the OTel SDK's span context. For uninstrumented services, eBPF events stand alone but can still be correlated by connection tuple (src:port, dst:port).

### P6: Cross-Layer Event Correlation

Traces, metrics, and logs are three pillars, but the connections between them are weak in practice. A span tells you a request was slow; a metric tells you CPU was high; a log tells you a GC pause happened. Correlating all three for a single incident is manual work.

Direction: atropos spans could carry a correlation ID that links to metrics exemplars and structured log entries. When a fault fires, it records the correlation ID in the span, emits a metric exemplar with the same ID, and writes a structured log line with the ID. A query engine (or Grafana datasource plugin) can then join across all three stores.

This is an important direction. Pinned for future design.

### Ecosystem Gaps Worth Addressing

Features and pain points observed across Jaeger, Zipkin, Tempo, and the OpenTelemetry Collector that atropos is positioned to address:

**Trace comparison and diff.** Jaeger has a trace comparison UI but it is limited to visual side-by-side. Open issues ([jaeger-ui#252](https://github.com/jaegertracing/jaeger-ui/issues/252), [jaeger-ui#513](https://github.com/jaegertracing/jaeger-ui/issues/513)) highlight that the diff shows structural differences but not latency diffs, and breaks when the same service runs on different nodes ([jaeger-ui#447](https://github.com/jaegertracing/jaeger-ui/issues/447)). There is no programmatic trace diff that can identify structural changes (new spans, missing spans, latency shifts) between a baseline and an experiment run. Grafana has a separate request for trace comparison ([grafana#35531](https://github.com/grafana/grafana/issues/35531)). Atropos experiments naturally produce A/B trace sets; building a diff tool on top is a natural extension.

**Tail-based sampling with context.** The OTel Collector supports tail-based sampling, but decisions are made on span attributes alone. The collector's filter processor ([otel-contrib#29093](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/29093)) cannot express "and logic" across spans within a trace for filtering decisions. Atropos has richer context (evaluator decisions, fault injection state, label predicates) that could inform smarter sampling: always sample traces that were affected by faults, always sample traces that crossed topology boundaries, downsample routine health checks.

**Root cause ranking.** Tempo and Jaeger surface trace data but leave root cause analysis to the operator. Tempo's query performance degrades significantly at scale ([tempo#5679](https://github.com/grafana/tempo/issues/5679)), making manual investigation harder. A service that knows which faults are active, which config versions are deployed, and which infrastructure boundaries were crossed could rank probable root causes for anomalous traces.

**Dependency health signals.** Zipkin's dependency graph is derived from trace data after the fact. Atropos egress interceptors see every outbound call in real time and can maintain a live dependency health score per upstream, detecting degradation before it shows up in aggregated metrics.

**Service graph with fault annotations.** Tempo's service graph shows topology but not fault state. Overlaying active fault experiments on the service graph gives operators a real-time view of what is being tested where.

**Chaos-tracing integration.** The Chaos Toolkit has an OTel extension ([chaostracing.oltp](https://chaostoolkit.org/drivers/opentracing/)), but it treats observability and fault injection as separate concerns connected only by timestamps. Atropos unifies both in a single SDK: the fault injection IS the instrumentation, so every fault naturally produces correlated trace data without external stitching.
