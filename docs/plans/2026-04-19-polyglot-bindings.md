# Polyglot Bindings for Atropos

Branch: `chore/polyglot-bindings`
Target: Instrument DeathStarBench (10 languages, 3 RPC protocols)

## Motivation

DeathStarBench's Social Network has 34 microservices across C, C++, Java, Node.js, Python, Scala, PHP, JavaScript, Go, and Lua — all speaking Thrift. Media Service adds another ~20 services in a similar polyglot mix. Hotel Reservation is all Go/gRPC.

Writing native SDK bindings in 10 languages is not sustainable. The key observation: **three protocols (Thrift, gRPC, HTTP) cover every service in the suite.** Protocol-aware interception is a finite amount of work; polyglot instrumentation is not.

## Architecture Choice: Protocol-Aware Sidecar Proxy

Rather than N language SDKs, we add a single sidecar (`atropos-proxy`) per service pod that speaks all three protocols natively. The target service talks to `localhost:PROXY_PORT` instead of the upstream service directly. The sidecar performs cache-box modal switching, inline fault injection, and policy-driven decisions — all without the target service knowing.

### Responsibility split

| Responsibility | Where | Rationale |
|---|---|---|
| Protocol interception (Thrift/gRPC/HTTP) | Sidecar | Language-agnostic at wire level |
| Cache-box modes (passthrough/replay/replay-with-delay) | Sidecar | Requires protocol deserialization |
| Inline faults (latency/error/hang) | Sidecar | Proxy-level delay/reject |
| Network faults (RST/blackhole/throttle) | Sidecar | Already at TCP layer |
| Resource faults (CPU/IO/memory stress) | Second sidecar or DaemonSet | Needs host-level access |
| OTel tracing + baggage | Target service (native OTel SDK) | Already present in DeathStarBench |
| Policy evaluation | Sidecar (embedded OPA) | Bundle polling, in-process |
| Metrics | Sidecar (Prometheus) | Exposed via /metrics |

### Deployment diagram

```
┌──────────────────────── Kubernetes pod ──────────────────────┐
│                                                               │
│  ┌──────────────────────┐      ┌──────────────────────────┐  │
│  │  target service      │◄─────┤  atropos-proxy sidecar   │  │
│  │  (any language)      │ in   │                           │  │
│  │                      │      │  Protocol handlers:       │  │
│  │  Thrift/gRPC/HTTP    │─────►│   Thrift | gRPC | HTTP   │  │
│  │  client config       │ out  │                           │  │
│  │  points to localhost │      │  Cache-box modal switch   │  │
│  │                      │      │  Inline fault exec        │  │
│  │  OTel SDK → collector│      │  Embedded OPA             │  │
│  └──────────────────────┘      └──────────────────────────┘  │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                                        ▲
                                        │ bundle poll (OPA)
                                        │
                              manteion / OPA bundle endpoint
```

## What Changes in atropos-go

### 1. Extract `proxy/` subpackage

The existing `internal/fault/network/proxy.go` already implements a TCP proxy with toxic effects. Generalize this into a protocol-aware proxy:

```
atropos-go/
├── proxy/                      # NEW: standalone sidecar binary
│   ├── cmd/atropos-proxy/      # entry point
│   ├── protocol/               # wire-level protocol decoders
│   │   ├── thrift/             # Thrift binary + compact protocol
│   │   ├── grpc/               # HTTP/2 + protobuf framing
│   │   └── http/               # HTTP/1.1 (reuse net/http)
│   ├── cachebox/               # modal middleware at proxy level
│   └── config/                 # YAML sidecar config
├── internal/
│   └── fault/                  # unchanged
└── (existing public API stays intact)
```

The proxy is a separate `main` package. It imports `internal/evaluator`, `internal/fault`, `internal/trace` — the same core the in-process SDK uses. This is intentional: **one implementation of fault logic, two deployment modes** (embedded library for Go services, sidecar for everything else).

### 2. Protocol decoders

- **Thrift**: Use `github.com/apache/thrift/lib/go/thrift` to parse method name, args, and sequence ID. Cache key = `service.method(hash(args))`. Framed binary protocol is standard for DeathStarBench.
- **gRPC**: HTTP/2 stream parsing + protobuf length-prefix framing. For unary calls, capture the single request/response pair. Streaming calls require a streaming cache policy (future work).
- **HTTP**: Existing `net/http` machinery; already solved in atropos-go ingress middleware.

All three decoders emit the same canonical `Request{Point, Labels, Payload}` that the existing `Evaluator` interface consumes. Zero changes to the evaluator or fault taxonomy.

### 3. Sidecar configuration

YAML config per sidecar describing:
- Upstream services the proxy should forward to
- Which protocol each upstream uses
- Which ports the proxy listens on (ingress and egress)
- manteion/OPA bundle endpoint
- OTel collector address

Example:

```yaml
proxy:
  ingress:
    - protocol: thrift
      listen: ":9090"
      upstream: "localhost:9091"    # real service port
      service_name: "user-service"
  egress:
    - protocol: thrift
      listen: ":19090"              # service connects here
      upstream: "social-graph:9090" # real upstream
      service_name: "social-graph"
opa:
  bundle_url: "http://manteion:8080/bundles/experiment.tar.gz"
  poll_interval: "10s"
otel:
  endpoint: "otel-collector:4317"
```

### 4. Optional thin language SDKs

Only needed for `Custom` injection points (instrumenting arbitrary business logic, not just RPC boundaries). Each language SDK is a ~200 LOC client that:
- Reads baggage from the language's OTel SDK
- Calls the sidecar's local HTTP admin API (`POST localhost:PROXY_PORT/evaluate`) to get a decision
- Blocks on inline fault execution (or delegates to sidecar if the language has no good sleep/cancel primitives)

Priority order for language SDKs:
1. Python (most DeathStarBench services that need custom spans)
2. Java (Spring services in Media Service)
3. Node.js (user/recommendation services)
4. C++ (optional; OTel C++ SDK is mature enough)

Not priority: Scala, PHP, Lua — proxy sidecar is sufficient.

## DeathStarBench Deployment Strategy

### Phase A: Hotel Reservation (all Go/gRPC)

Instrument natively with the existing atropos-go SDK. No sidecar needed. This validates cache-box on a known-simple target.

### Phase B: Proxy sidecar on Social Network (subset)

Pick 5-6 shared services on critical paths (e.g., `user-timeline-service`, `social-graph-service`, `post-storage-service`). Deploy `atropos-proxy` as a sidecar alongside each. Other services remain un-instrumented (black-box load contributors).

This matches ShapleyIQ's approach: instrument the services that matter, treat the rest as workload.

### Phase C: Full mesh

Roll out atropos-proxy to every service in Social Network and Media Service. Validate that cache-box decomposition works at mesh scale.

## Key Considerations

### 1. Thrift cache key design

Thrift method calls include a sequence ID that varies per call. Cache key must exclude seqid but include method name + args. For reply caching, replay must match the seqid of the incoming request, not the recorded one.

### 2. Connection-oriented protocols

Thrift and gRPC use long-lived connections. The proxy must correctly multiplex: one client connection → N upstream connections, each forwarded or replayed independently. The existing atropos-go network proxy handles per-connection state; we extend it with per-call (per-Thrift-message) decisions.

### 3. OTel span parenting

When the sidecar replays a cached response, the caller's span expects a child span from the callee. Options:
- **Sidecar generates a synthetic child span** with `atropos.cachebox.replayed=true` attribute. Cleanest.
- **No child span on replay** — the caller sees a "hole" in the trace. Simpler but loses replay visibility.

Recommend the first approach. The synthetic span also carries the cache hit/miss indicator.

### 4. Streaming RPCs (gRPC)

gRPC streaming doesn't fit a request-response cache model. Options:
- Don't replay streams (passthrough only; fault only at connection establishment)
- Record the first N messages of each stream and replay them (stateful cache)

Start with passthrough-only for streams. Streaming cache is future work.

### 5. Mutual TLS

DeathStarBench uses plaintext Thrift by default, so this doesn't apply. For production adoption, the proxy would need to terminate mTLS from clients and re-establish to upstreams. Out of scope for initial implementation.

### 6. Sidecar injection

Use a Kubernetes mutating admission webhook to inject the sidecar into DeathStarBench pods automatically (similar to Istio sidecar injection). For initial work, manually edit the K8s manifests.

### 7. Port remapping

Each DeathStarBench service has a fixed upstream port. The proxy needs:
- Its own port for ingress (exposed as the service port; the sidecar becomes the public endpoint)
- The target service's port moved to an internal-only port

This requires modifying the service manifests. A helper script can automate this for DeathStarBench's existing YAML.

### 8. Latency overhead

Protocol-aware proxy adds roughly:
- TCP roundtrip: ~50μs (localhost)
- Protocol deserialization: ~100μs for Thrift, ~50μs for HTTP
- OPA evaluation: ~10-100μs (pre-compiled)
- Total: ~200-400μs per call

This is small compared to typical Thrift call latency (1-10ms) but measurable. Document this overhead clearly — it's part of the baseline.

### 9. Development ergonomics

For Go services (Hotel Reservation), keep the in-process SDK as the preferred path — no sidecar, zero IPC. The proxy is a sidecar because polyglot mesh services can't embed Go. Don't force every service to use the sidecar.

## What Does *Not* Change

- `internal/evaluator`, `internal/fault`, `internal/trace`: unchanged. These are the shared core.
- Public API of atropos-go: unchanged. Existing Go users continue with `atropos.Init()` + middleware.
- VISION.md: the cache-box primitive and decomposition methodology are unchanged; only the deployment surface expands.
- manteion interface: unchanged. It publishes bundles; sidecars poll. Same contract as for in-process OPA.
- OTel collector, Prometheus, Grafana: unchanged.

## Open Questions

1. **Should the proxy terminate TLS?** Needed for production, probably not for DeathStarBench experiments.
2. **How to handle stateful services (cart, user-timeline)?** VISION.md scope constraint still applies — freeze only stateless read services. Proxy doesn't change this.
3. **Resource fault injection in mixed-language pods?** The existing CPU/memory/IO stress uses cgroup-aware detection. If running in a sidecar container, it'd stress the sidecar's cgroup, not the target's. Need a pattern for shared-PID-namespace injection (privileged sidecar with `shareProcessNamespace: true`).
4. **Admin API for in-pod control?** The sidecar needs a local control endpoint (Unix socket) for the optional language SDKs to call. Define this wire protocol.
5. **Rollout strategy:** How does the sidecar hot-reload configuration without dropping in-flight connections?

## Next Steps

1. Write a minimal Thrift proxy in `proxy/protocol/thrift/` that just forwards (no cache-box yet).
2. Deploy it as a sidecar in Hotel Reservation's `frontend` service to validate the sidecar pattern works.
3. Add passthrough/replay modes.
4. Add synthetic span generation on replay.
5. Add a sidecar injection webhook for DeathStarBench.
6. Run the first cross-workflow interference experiment with mixed native-SDK (Hotel Reservation) and sidecar-proxy (Social Network) instrumentation.

## Files to Create

- `proxy/cmd/atropos-proxy/main.go` — entry point
- `proxy/protocol/thrift/decoder.go` — Thrift binary protocol parser
- `proxy/protocol/grpc/decoder.go` — gRPC HTTP/2 + protobuf framing
- `proxy/protocol/http/decoder.go` — thin wrapper around net/http
- `proxy/cachebox/store.go` — proxy-level cache store
- `proxy/cachebox/modal.go` — mode switching logic
- `proxy/config/config.go` — YAML config
- `deploy/sidecar/injector.yaml` — K8s mutating webhook (later)
- `deploy/sidecar/deathstarbench-socialnet.yaml` — manually-edited manifests for Phase B

## Verification

- Unit tests for Thrift decoder (cover binary + compact protocol, all field types)
- Integration test: proxy in front of a toy Thrift service, verify passthrough = normal latency
- Integration test: proxy in replay mode returns cached response without hitting upstream
- End-to-end test on Hotel Reservation with Go/gRPC proxy (validates the general pattern before Thrift)
- End-to-end test on DeathStarBench Social Network subset (Phase B)
