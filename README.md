# atropos-go

Go SDK for fault injection, observability instrumentation, and request correlation in distributed systems.

Atropos embeds into your services as a library. It provides OpenTelemetry instrumentation out of the box, evaluates developer-defined rules to decide when and where to inject faults, and emits rich trace data so the effects of those faults are observable. The SDK is useful for pure observability even without fault injection configured.

In the faults-lab instrument, atropos is the in-process measurement arm: it records a service's egress HTTP responses during baseline phases, replays them **fail-closed** when the service is frozen for isolation phases, injects faults on rule match, and keeps per-`(experiment, phase)` fidelity counters — all driven by the desired state long-polled from the manteion control plane.

Embedding is one call:

```go
h, shutdown, err := atropos.Serve(ctx, atropos.Config{
    Service: "productcatalogservice",
    Version: "0.1.0",
    Routes:  []atropos.Route{{Method: "GET", Path: "/products"}},
    Handler: businessMux, // the service's own routes
})
if err != nil { log.Fatal(err) }
defer shutdown(ctx)
http.ListenAndServe(":"+port, h)
```

Everything else — OTel, the record/replay cache-box with its push client and
fidelity counters, the manteion connection, and the admin/observability
endpoints — is wired inside with env-derived defaults, and works offline when
`MANTEION_URL` is unset. For the service's own outbound calls:

```go
client := &http.Client{Transport: atropos.EgressTransport(nil)}
```

## Current State

A snapshot of what's shipped on `main`. For the research vision (cache-box primitive, interventional performance attribution, policy-driven experiments), see `VISION.md`.

| Area | Status |
|---|---|
| One-call embed — `atropos.Serve` wires OTel, evaluator, cache-box, push, manteion, and the control surface | Shipped |
| Inline faults (latency, error, hang) | Shipped |
| Network faults (TCP proxy: RST, blackhole, retransmit_delay, latency, throttle, drip) | Shipped |
| Resource faults (CPU, I/O, disk, memory) | Shipped |
| Cache-box fidelity stack — fail-closed replay, split record/replay store, staged preload (W5 checksum), per-phase fidelity counters, drain reports | Shipped |
| SDK ↔ manteion wire protocol — long-poll desired state (ETag/generation), single apply path, status report, replay-set pull, SSE nudge | Shipped |
| Fault registry — per-key stop, fired on rule replace / slot clear / watchdog / admin delete | Shipped |
| Route publishing — `Config.Routes` rides the register payload | Shipped |
| `/admin/fault`, `/admin/rules`, `/admin/cachebox` runtime control (mounted by `Serve`, outside the fault middleware) | Shipped |
| Prometheus metrics for HTTP ingress/egress + cache-box | Shipped |
| Cache-box on ingress + gRPC | Not yet implemented |
| OPA-backed evaluator | Not yet implemented |
| Out-of-process host — `atroposd` sidecar: explicit capture (per-dependency egress ports) and transparent capture (iptables REDIRECT + `SO_ORIGINAL_DST`, raw-TCP relay for non-HTTP dependencies), inline network toxics, eager arming, `host`/`capabilities` at registration, `ATROPOS_HOST=sidecar` passive guard, deploy assets under `deploy/sidecar/` | Shipped (M1, M2) |
| Node agent for resource faults — `atropos-agent` DaemonSet: `burn` placed in the target container's cgroup, `quota` on its `cpu.max`/`memory.high` with a restore journal, own-pod peer check, deploy assets under `deploy/agent/` | Shipped (M4) |
| manteion capability filter (M3), gRPC cache-box, thin-client hook API | Not yet implemented (M3 lives in manteion-go; the rest is deferred; see `docs/plans/2026-09-08-atropos-daemon-design.md`) |
| Polyglot bindings (DeathStarBench target) | Superseded by `atroposd` (the 2026-04 sidecar plan is critiqued in the design doc above) |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Service code                                                │
│                                                              │
│   atropos.Serve(ctx, Config)  -- the embed: OTel bootstrap,  │
│     evaluator + admin fault slot, cache-box + push client,   │
│     manteion connection, and the control surface:            │
│       /metrics            /atropos/health                    │
│       /admin/fault[/{id}] /admin/rules                       │
│       /admin/cachebox     /cachebox/preload/*                │
│       /cachebox/fidelity                                     │
│     (control paths bypass the fault/OTel middleware;         │
│      everything else wraps Config.Handler in it)             │
│   atropos.EgressTransport()  -- HTTP client (+ cache-box)    │
│   atroposgrpc.UnaryServerInterceptor(atropos.DefaultInterceptor())│
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
│   replay       ──► serve installed set; miss = synthetic 503 │
│   replay_delay ──► replay + synthetic latency (fail-closed)  │
│                                                              │
│   ReplaySet (immutable) · RecordBuffer · PreloadStore (W5)   │
│   FidelityRegistry (per-phase) · Recorder (async) · Delay    │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Fault taxonomy             (internal/fault)                 │
│                                                              │
│   inline/     Latency, Error, Hang                           │
│   network/    TCP proxy with toxics (RST, retransmit_delay,  │
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

The root package is the in-process host only. `internal/instrument` is the
instrument core every host builds (`Core`, `ControlSurface`, the manteion
client, the rule decoders, the admin handlers); `internal/wire` holds the JSON
wire shapes, re-exported here as type aliases; `internal/metrics` holds the
Prometheus series. `cmd/atroposd`, the out-of-process sidecar host described in
`docs/plans/2026-09-08-atropos-daemon-design.md`, builds the same core.

## Installation

```bash
go get git.ucsc.edu/microfaults/atropos-go@<tag>   # pin a v0.0.9-alpha.<sha> tag; never track a branch
```

Consumers (manteion, the service-beds mesh) require the SDK **by tag** with no `replace`
directive, so a build is reproducible from the module proxy/VCS alone (`GOPRIVATE=git.ucsc.edu/*`).
The gRPC interceptors live in a separate subpackage so HTTP-only services never pull in
`google.golang.org/grpc`:

```bash
go get git.ucsc.edu/microfaults/atropos-go/grpc
```

## Quick Start

### 1. Embed the instrument

```go
import "git.ucsc.edu/microfaults/atropos-go"

func main() {
    ctx := context.Background()

    mux := http.NewServeMux()
    mux.HandleFunc("GET /products", listProducts)

    h, shutdown, err := atropos.Serve(ctx, atropos.Config{
        Service: "productcatalogservice",
        Version: "0.1.0",
        Routes: []atropos.Route{
            {Method: "GET", Path: "/products", Description: "List products"},
        },
        Handler: mux,
    })
    if err != nil { log.Fatal(err) }
    defer shutdown(ctx)

    http.ListenAndServe(":8080", h)
}
```

`Serve` bootstraps OTel (OTLP gRPC exporter, W3C TraceContext + Baggage
propagators; endpoint from `OTEL_EXPORTER_OTLP_ENDPOINT` >
`COLLECTOR_SERVICE_ADDR` > `localhost:4317`), builds the record/replay
cache-box and its manteion push client around one shared fidelity registry,
composes the host rule evaluator with the admin fault slot (host rules always
win), connects to manteion, and returns a handler that serves the control
surface beside the middleware-wrapped business handler.

Offline is first-class: with `MANTEION_URL` unset everything works — tracing,
`/admin/*` fault control, the cache-box endpoints — there is just no control
plane to sync with. A configured-but-unreachable `MANTEION_URL` is a startup
**error**, not a silent offline run: an instrument that pretends to be
connected corrupts experiments.

`Config` fields (all optional except `Service`; `Handler` may be nil for a
control-surface-only embed, e.g. a gRPC service's admin port):

| Field | Default | Purpose |
|---|---|---|
| `Service` | — (required) | OTel `service.name`, manteion identity, metrics label |
| `Version` | `MANTEION_SERVICE_VERSION` | OTel `service.version` + experiment attribution |
| `Environment` | `"development"` | OTel `deployment.environment` |
| `Routes` | none | route inventory published at registration |
| `Handler` | nil | the service's own routes (nil = control surface only) |
| `ManteionURL` | `MANTEION_URL` | control-plane base URL; empty = offline |
| `InstanceID` | `MANTEION_INSTANCE_ID` > hostname > `Service` | the ONE instance identity (register, cache-push, fidelity) |
| `NetworkResolver` | nil | target → (listen, upstream) for network faults |
| `TracerProvider` | OTLP exporter | BYO provider (tests, custom OTel setups) |
| `Logger` | `slog.Default()` | SDK logging |

The returned `ShutdownFunc` tears down in dependency order: manteion close →
recorder drain → final cache-push flush → OTel span flush. Idempotent.

### 2. Outbound calls

```go
client := &http.Client{
    Transport: atropos.EgressTransport(nil), // nil = http.DefaultTransport
}
resp, _ := client.Get("http://payment-service/charge")
```

`EgressTransport` composes OTel client spans, egress fault checks, and
cache-box record/replay dispatch. It resolves the interceptor per round-trip,
so clients built before `Serve` runs still pick up the wiring `Serve`
installs.

### 3. gRPC Interceptors

```go
import atroposgrpc "git.ucsc.edu/microfaults/atropos-go/grpc"

server := grpc.NewServer(
    grpc.UnaryInterceptor(atroposgrpc.UnaryServerInterceptor(atropos.DefaultInterceptor())),
    grpc.StreamInterceptor(atroposgrpc.StreamServerInterceptor(atropos.DefaultInterceptor())),
)
```

A gRPC service still calls `Serve` (with `Handler: nil`) and mounts the
returned control surface on an admin port.

### 4. Faults and rules at runtime

Rules normally arrive from manteion. For local work, the control surface
`Serve` mounts does everything by hand:

```bash
# arm a single admin fault (fills gaps; never shadows manteion rules)
curl -X POST localhost:8080/admin/fault \
  -d '{"category":"inline","fault_type":"latency","params":{"delay":"500ms"}}'
curl localhost:8080/admin/fault
curl -X DELETE localhost:8080/admin/fault          # clear all
curl -X DELETE localhost:8080/admin/fault/{id}     # clear one slot

# inspect / replace the compiled rule set (wire-format []CompiledRule)
curl localhost:8080/admin/rules
curl -X POST localhost:8080/admin/rules -d @rules.json

# cache-box stats / freeze delay / clear
curl localhost:8080/admin/cachebox
curl -X POST localhost:8080/admin/cachebox/delay -d '{"mu":8.5,"sigma":0.3,"seed":42}'
curl -X DELETE localhost:8080/admin/cachebox

# health + metrics
curl localhost:8080/atropos/health
curl localhost:8080/metrics
```

POST `/admin/fault` body fields by `fault_type`: `latency` requires
`params.delay` (e.g. `"500ms"`, optional `jitter`); `error` defaults to
status 500, `"injected fault"`; `hang` requires `params.duration`. The
admin slot composes AFTER the host evaluator, so an armed admin fault can
never shadow the rules of a running experiment. When connected, manteion stays
authoritative over slots: the next `200` desired-state document that does not
name a slot drops (and stops) it, and the fault watchdog drops one that stays
unconfirmed for the grace period (90 s with the default poll settings; see
"Manteion integration" below). Offline, slots persist until deleted.

POST `/admin/rules` replaces the installed rule set through the same apply
path the manteion sync uses, scoped to rules. A rule this host cannot execute
is skipped (`204` still); a rule that does not decode is skipped too and the
answer is `422` naming it, the pushed set less the skipped rules having
replaced the installed one (an empty set when no rule decoded). When connected,
a pushed set is a short-lived override like an admin slot: the push clears the
ETag the instance holds, so the next poll — at most one 30 s hold later — is
unconditional and its `200` re-applies manteion's desired state. The push sends
no status report. Offline, a pushed set holds until the next push.

### 5. Cache-Box (Egress)

Cache-box is the measurement primitive that "freezes" a downstream dependency by replaying recorded responses instead of forwarding to the real service. Three modes, selected by the cache-box rule manteion synthesizes for the active experiment phase:

- **passthrough**: forward to real upstream, record the request/response pair asynchronously under the rule's authoritative key strategy
- **replay**: serve from the installed replay set; a miss is a **counted synthetic 503**, never a live call (fail-closed — this is what makes a frozen phase trustworthy)
- **replay_with_delay**: replay plus a synthetic delay (fitted lognormal from the desired state's `freeze_cfg`, or observed)

The store is split so record traffic can never evict replay data: an immutable, atomically-swapped **ReplaySet** serves replays (installed only via staged preload commit), while a bounded **RecordBuffer** absorbs recording. Replay lookups are scoped to the rule's `(experiment_id, phase_id)` — a hit against another phase's set fails closed and is counted. Every drop (recorder backpressure, oversize response, push failure) increments a per-phase fidelity counter, and phase end triggers a flush + **drain report** to manteion so recording completeness is verified, not assumed.

Response headers on replay: `X-Atropos-Cache-Key`, `X-Atropos-Cache-Mode`, `X-Atropos-Cache-Latency-Us`; fail-closed misses carry `X-Atropos-Cache-Miss`. Oversize responses (> 1 MiB by default) stream through unchanged and are **counted as drops** under a recording rule.

Cache-box covers **egress HTTP only**. Ingress and gRPC are tracked in Ongoing Development.

### 6. Prometheus Metrics

`GET /metrics` (mounted by `Serve`, outside the middleware) exposes
`http_server_requests_total`, `http_server_request_duration_seconds`,
`http_client_requests_total`, `http_client_request_duration_seconds`, the
cache-box hit/miss/record counters, `atropos_fault_injections_total`, and the
sync series: `atropos_sync_generation` (the generation held),
`atropos_replay_set_state{state}` (the replay-set pull state; all three read 0
when no set is desired) and `atropos_rules_unsupported_total{category}`.

### 7. atroposd: the out-of-process host

`atroposd` hosts the same instrument core as `Serve`, one sidecar per pod,
for services that cannot embed the Go SDK (or a Go service you want to
measure without touching its code). It registers with manteion as one SDK
instance, mounts the control surface above on its own port (`:16020`), and
puts two listeners around the app:

- **egress** (explicit capture mode): the app dials the sidecar's port
  instead of its dependency; the sidecar rewrites `Host` to the real
  upstream and forwards through the exact `EgressTransport` composition, so
  cache keys, `X-Atropos-*` headers, fail-closed replay and metrics are the
  ones the in-process host produces (a golden test pins key identity);
- **ingress** (optional): callers reach the sidecar's port; it wraps a
  reverse proxy to the app in the ingress composition and hands the app a
  `traceparent` under its own server span.

```bash
ATROPOS_SERVICE=recommendationservice \
MANTEION_URL=http://manteion:8080 \
ATROPOSD_EGRESS=":19090=productcatalogservice:3550" \
atroposd                                   # or: atroposd -config /etc/atropos/atroposd.yaml
# the app then uses PRODUCT_CATALOG_SERVICE_ADDR=127.0.0.1:19090
```

YAML form (`-config`; the environment overrides it):

```yaml
service: recommendationservice
manteion_url: http://manteion:8080
control_listen: ":16020"
egress:
  - listen: ":19090"
    upstream: productcatalogservice:3550
ingress_listen: ":16006"          # optional
inbound_upstream: 127.0.0.1:8080
routes:
  - {method: GET, path: /recommendations}
```

Two behaviours are sidecar-only: network faults with `network.host: inline`
run as **armed policies** on the sidecar's upstream calls (stream toxics
shape request or response bytes; `rst` and `blackhole` are reproduced on
the app-facing connection, so the app sees a reset or a hang, not a 502),
and background faults **arm when their rule applies** (except Transient
rules) rather than on the first matching request. The in-process host skips
`host: inline` rules (logged and counted in
`atropos_rules_unsupported_total{category="network"}`) and arms lazily.
Proxy overhead per request (total minus upstream) is
exported as `atropos_proxy_overhead_seconds{point}` for the G-proxy gate.

**Transparent capture** (`ATROPOSD_CAPTURE=transparent`) removes the
per-dependency ports: the `atropos-init` container (NET_ADMIN, see
`deploy/sidecar/init.sh`) REDIRECTs the app's outbound TCP to the sidecar's
egress port and the declared `ATROPOSD_INBOUND_PORTS` to its ingress port,
excluding the sidecar's own uid, loopback, and the OTel collector ports.
The sidecar recovers each connection's original destination
(`SO_ORIGINAL_DST`), keeps the app's `Host` header as the key's host (so
keys match the in-process host by construction), dials the original
destination, and relays `ATROPOSD_RAW_TCP_PORTS` (redis, kafka, postgres)
at L4 with armed toxics applied. It refuses to start — and therefore to
register — unless the init container's capture marker exists. The app
container carries `ATROPOS_HOST=sidecar`, which makes an embedded `Serve`
passive (tracing only), so a pod is instrumented by exactly one host.
Registration advertises `host` and `capabilities`; rules a host cannot
execute are skipped per rule and counted in
`atropos_rules_unsupported_total` instead of wedging the sync.
`deploy/sidecar/` holds the images, the init script and a kustomize patch
for one boutique service.

**Resource faults** never run in the sidecar: a per-node `atropos-agent`
(privileged DaemonSet, `deploy/agent/`) runs them inside the *app
container's* cgroup. When the agent's socket
(`ATROPOSD_AGENT_SOCKET`, default `/var/run/atropos/agent.sock`) answers
at sidecar start, the sidecar advertises `resource` and delegates every
resource rule for `ATROPOSD_APP_CONTAINER` (default `server`) of its pod
(`ATROPOS_POD_UID` from the downward API; the agent verifies the caller's
pod from the socket peer regardless). Resource params gain
`mechanism`: `burn` (default) re-executes the existing stress code as a
child placed in the target cgroup before it starts, sized by the target's
own limits, so semantics and ramps are the in-process ones; `quota`
lowers the target's `cpu.max` (cpu) or `memory.high` (memory) by
`target_load` of its capacity, journaled before it is written and restored
on stop, when the sidecar disconnects, and at agent boot. The in-process
host runs `burn` natively and skips `quota` rules per rule (no cgroup
authority; a single `/admin/fault` slot gets a 400). A fault lives as long
as the sidecar's streaming request to the agent; its result lands on the
sidecar's fault span as usual.

Not in this release: the manteion-side capability filter (M3), gRPC and
the thin-client hook API (deferred), IPv6 original destinations, `quota`
for `io`/`disk`. Design and milestones:
`docs/plans/2026-09-08-atropos-daemon-design.md`.

---

## Manteion integration & the fidelity pipeline

The SDK-initiated path is the production one, and it is **desired-state sync**
(contract: `docs/plans/2026-10-30-desired-state-protocol.md`). `Serve`
registers the instance; the registration answer is the materialized desired
state and is applied before the first poll. The loop then keeps the instance on
it:

- **Long-poll.** `GET /api/v1/sdk/rules?...&version=<generation held>&wait=30`
  with `If-None-Match: "<etag held>"`. For a current instance (its `version` is
  the service's generation and its ETag matches) manteion holds the request until
  the generation changes or 30 s pass; a matching ETag with any other `version`
  (older, absent or zero, or ahead after a manteion database reset) is answered
  `304` at once, carrying the current generation. The ETag is the
  document's content hash alone and opaque to the SDK; the generation travels in
  `X-Atropos-Generation` and the body's `version`.
- **`200`** — the body is the **full desired state** (`RuleSync`: `rules`,
  `active_faults`, `freeze_cfg`, `replay_set`). An empty rule set is
  authoritative (rules cleared, running background faults stopped by key, drain
  tracker fired), not "no change"; so is an empty `active_faults`, and an absent
  `freeze_cfg` or `replay_set` clears the delay source or the installed replay
  set. It goes through the single `apply` path (`POST /admin/rules` takes the
  same one, scoped to rules), then the instance reports with
  `POST /api/v1/sdk/status` and polls again at once.
- **`304`** — the document is unchanged: the instance adopts the generation in
  `X-Atropos-Generation` and applies nothing.
- **Skipped per rule.** One bad rule never fails the sync; the rest apply. A rule
  this host cannot execute (a category outside its capabilities, `quota`
  in-process, network `host: inline` outside the sidecar or `host: proxy` without
  a resolver, a composition) is skipped, logged, counted in
  `atropos_rules_unsupported_total{category}` (compositions under
  `category="composition"`) and reported under `unsupported`. A rule it cannot
  decode is skipped, logged and reported under `errors`; it is not counted.
- **Freeze.** `freeze_cfg.context` scopes the delay source to its
  `(experiment_id, phase_id)`: replays under another pair keep the entry's
  observed latency, and the `/admin/cachebox` stats show the source and its scope.
  A `200` whose `freeze_cfg` names the source already installed (the same `mu`,
  `sigma`, `seed` and scope) keeps it, so its sample sequence runs on across the
  documents of a frozen phase instead of restarting at each.
- **Status report.** A full snapshot of what the instance holds: applied
  generation, ETag, `unsupported` rules, `errors`, and the desired replay set
  with its pull state (`fetching|committed|failed`: `fetching` and `failed` name
  a set not held yet, while an earlier committed set may still be installed;
  absent = none desired). It goes out after every
  apply and on every replay-set transition. One that fails to send is re-sent
  before the next poll and by its own retry (2 s, doubling up to the poll wait),
  so manteion's convergence barrier does not starve on one lost report.
- **Cadence.** Registration advertises
  `poll_interval_ms = max(poll interval, hold)`: 30 s with the defaults (a 10 s
  interval, a 30 s hold), so manteion's liveness thresholds tolerate the hold. A
  failed poll keeps the stale state and backs off (60 s at most); the sixth
  consecutive failure re-registers once, and the first poll that succeeds after
  failures drops the held generation and ETag and re-registers, so the instance
  re-converges on a full document through the register answer. A poll or status
  report answered `404` means manteion no longer knows the instance (its
  database was reset, or another pod deregistered the same id): the instance
  drops the held generation and ETag and registers again at once, applying the
  answer; once per loss, so while the report after that re-register has not
  landed a further `404` does not register again. The SSE `rules_changed` nudge
  (`GET /api/v1/sdk/events`) is kept and ends any wait between polls early.
- **Watchdog.** A fault slot that stays unconfirmed for
  `max(3·max(interval, hold), 30 s)` is dropped: 90 s by default. A `200` that
  names the slot, or a `304` for a document that did, re-confirms it; an
  `/admin/fault` slot is never confirmed, so connected it lives that long (or
  until the next `200` that does not name it).

Cache-box record/replay rules arrive with a `CacheBoxContext` carrying the `(experiment_id,
phase_id)` pair and the authoritative key strategy; the record path keys entries with exactly that
strategy and ships it as wire provenance.

The fidelity pipeline around a recording phase:

1. **Record** — passthrough rule active; entries land in the RecordBuffer and stream to manteion
   (`POST /api/v1/cache/ingest`) in batches with a `batch_seq` monotonic over the instance's lifetime
   (it never restarts when the pair changes) and the instance's `epoch` (its start time in unix
   nanoseconds), so manteion's dedupe key `(experiment, phase, instance, epoch, batch_seq)` names one
   batch, across restarts too.
2. **Drain** — the rule leaves the rule set at phase end; the drain tracker flushes the recorder,
   settles all in-flight batches, then POSTs a per-pair drain report (recorded / pushed / dropped).
   Manteion's drain barrier compares those against what it actually received.
3. **Pull** — for the isolation phase the desired state carries a `replay_set` (artifact `id`, the
   `(experiment_id, phase_id)` pair it is installed under, entry `count`, W5 `sha256`, `size_bytes`,
   `url`). When its `id` differs from the committed one the instance fetches
   `GET /api/v1/cache/artifacts/{id}` (NDJSON, one entry per line) in `Range` chunks of at most
   4 MiB, retrying a chunk from the same offset when its request cannot be built or sent or is
   answered 5xx, 408 or 429 (five requests per chunk at most), stages the entries through the
   preload path and commits only when the entry count and the W5 checksum match, which atomically
   installs the ReplaySet. A mismatch or any fetch failure leaves the prior set untouched and
   reports `failed` (the reason lands in the status report's `errors`). Each state (`fetching` →
   `committed` | `failed`) is reported as it is reached; a newer `id` supersedes a fetch in flight.
   A failed pull is pulled again on the instance's own timer while the set stays desired (5 s,
   doubling up to 60 s, each wait ±20 %), so manteion sees `failed` → `fetching` → `committed`
   even while it answers every poll `304`, and a restarted instance converges on its own. A `200`
   that names the failed set again is a fresh signal: the set is fetched again at once and the
   backoff starts over. Only a set no pull can fetch (no `(experiment_id, phase_id)` scopes it, an
   unsupported key strategy, no artifact `url`) gets no timer.
   `POST /cachebox/preload/{begin,chunk,commit}` stays mounted for manual use.
4. **Freeze/replay** — the synthesized replay rule + a delay source (`freeze_cfg` in the desired
   state, installed scoped to its `context`'s `(experiment_id, phase_id)`;
   `POST /admin/cachebox/delay` remains for manual use, and a source pushed that way lasts until
   the next `200` without `freeze_cfg`) freeze the service; misses fail closed and are counted;
   manteion pulls `GET /cachebox/fidelity?experiment_id=&phase_id=` (W6) at phase end for the
   verdict.

All of the host wiring this pipeline needs — identity resolution, the push
client bound to the cache-box's fidelity registry, and every handler mount at
the exact paths manteion addresses — happens inside `Serve`. The instance
identity is resolved **once** (`Config.InstanceID` > `MANTEION_INSTANCE_ID` >
hostname > service name) and used verbatim in the register call, the
cache-push envelopes and drain reports, and the fidelity endpoint — manteion
correlates all three. Environment read by `Serve`: `MANTEION_URL`,
`MANTEION_INSTANCE_ID`, `MANTEION_ADVERTISE_ADDR`, `MANTEION_SERVICE_VERSION`,
`MANTEION_INIT_TIMEOUT`, plus the OTel endpoint vars above.

---

## Ongoing Development

### Cache-box Stage 2

Extend the modal dispatch to ingress HTTP and gRPC. The `Decision.CacheBox` field, key strategies,
store, and recorder are already protocol-agnostic — only the middleware/interceptor glue is
missing. (Egress HTTP, including the full fidelity pipeline above, is shipped.)

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
| `Serve(ctx, Config)` | `serve(**cfg)` | `Atropos.serve(cfg)` | `serve(cfg)` |
| `EgressTransport(rt)` | `requests.Session` adapter | `OkHttp` interceptor | `fetch` wrapper |

---

## Related Repos

- **`manteion-go`** — control plane: experiment/phase FSM, rule oracle, cache store, drain barrier, fidelity verdicts
- **`manteion-ui`** — React admin UI on top of manteion
- **`zeus-go`** — execution plane: k6 workflow-run launcher + vegeta attacks, driven by manteion
- **`service-beds`** — Online Boutique microservices in Go, the instrumented target mesh

## Project report

This component is documented in the UCSC master's project report
*Safe Evolution and Interventional Fault Attribution in Microservice
Meshes* (Pranay Mundra, 2026) — Part II, as the in-process SDK of the
faults-lab instrument (cache-box freeze primitive, fault taxonomy,
fidelity counters).
