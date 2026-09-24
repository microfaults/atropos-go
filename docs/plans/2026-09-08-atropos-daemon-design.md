# `atroposd`: the out-of-process instrument — critique of the sidecar plan and a greenfield design

> **Status:** design accepted 2026-09-24 with the decisions recorded in §7 (scope: the HTTP Go fleet;
> node agent in scope; gRPC and thin clients deferred). Branch `claude/atropos-daemon-design-7abfe4`
> (off `develop` @ `f2066ed`).
> **Scope:** what a daemon process for atropos should look like so that services in any language get
> the same instrument Go services get today (faults, cache-box, control-plane sync, telemetry), what is
> wrong with the designs sketched so far, and which alternatives are better per capability.
> **Not in scope:** implementation. Section 6 is a staged plan. The only manteion change is additive:
> the `host`/`capabilities` register fields and the delivery-time capability filter (§5.3, M3).

---

## 0. Answer first

1. **The loopback idea is the existing plan.** "Service sends to `localhost:PORT`, the daemon picks it
   up" is exactly `docs/plans/2026-04-19-polyglot-bindings.md` (the `atropos-proxy` sidecar, branch
   `chore/polyglot-bindings`, never implemented). It gets the *process boundary* right (one daemon per
   pod, sharing the Go core) and the *capture mechanism* wrong.
2. **Explicit loopback ports are the wrong capture mechanism** for three reasons that are each
   sufficient on their own: coverage becomes opt-in per edge (holes are silent, the failure class D1/D2
   were written to kill); the `Host` header becomes `localhost:PORT`, and four of the five cache-key
   strategies fold `Host` into the key, so recordings and replay sets stop matching across instruments
   (INV-2); and every dependency address in eleven manifests becomes the resolver the report wanted in
   one place.
3. **Recommended shape (accepted, §7):** a per-pod sidecar `atroposd` with **transparent capture**
   (init-container `iptables` REDIRECT + `SO_ORIGINAL_DST`), an **L7 proxy** for HTTP/1.1 (cache-box,
   inline faults; h2c gRPC deferred), an **L4 toxic layer** applied to the proxy's upstream connections
   (network faults, always-on, which dissolves the proxy-lifecycle problem the report describes),
   **no resource faults in the sidecar itself**: they are delegated to a privileged per-node
   **`atropos-agent`** that runs the existing stress code inside the target container's cgroup (§5.14),
   and **loopback TCP only for the thin-client control API** (custom/transient hooks; deferred, the Go
   fleet has none). It registers as one SDK instance per pod under the existing wire contract; the only
   manteion change is the accepted delivery-time capability filter (§5.3).
4. **Prerequisite refactor:** pull the instrument core out of the root package so `atropos.Serve` and
   `cmd/atroposd` are two hosts of one implementation. Today the root package *is* the instrument and
   most of it is unexported.

---

## 1. What exists today (facts the design must respect)

### 1.1 Three sketches of a daemon, no code

| Source | Shape | Status |
|---|---|---|
| `docs/plans/2026-04-19-polyglot-bindings.md` (branch `chore/polyglot-bindings`, one doc-only commit on the Stage-1 base `ec20b07`; pruned from `origin` 2026-06-16, still on the `github` mirror; archived copy in `faults-lab/docs/archive/harvested-2026-06/`) | `atropos-proxy` per-pod sidecar; Thrift/gRPC/HTTP decoders; service dials `localhost:PROXY_PORT`; embedded OPA with bundle polling; thin per-language SDKs calling `POST localhost:PROXY_PORT/evaluate`; DeathStarBench as target | Design only |
| Master's report, `07-platform.tex` §"Network faults: wired and unwired" | "a small daemon beside each service, a per-pod sidecar or per-node process, that owns the proxy permanently"; services address dependencies through the daemon's ports all the time; "the polyglot answer" | Argument only |
| `faults-lab/docs/handoffs/PROMPT-fable-pressure-and-faults-2026-08.md` Q3 | Five options for network faults: inline jitter; env-var redirect (sidecar or in-process listener); persistent always-forwarding proxy with toggled toxics; `Config.External` + Toxiproxy; full per-pod wiring | Question, not answered |

### 1.2 The in-process instrument the daemon must reproduce

`atropos.Serve(ctx, Config)` wires: OTel bootstrap; one instance identity (`Config.InstanceID` >
`MANTEION_INSTANCE_ID` > hostname > service); the cache-box (`ReplaySet` + `RecordBuffer` +
`Recorder` + `FidelityRegistry` + staged `PreloadStore`) and its push client; the host `StaticEvaluator`
composed before the admin `demoEvaluator`; the process-lifetime `FaultRegistry`; the manteion client
(init wait, register, poll, SSE nudge, watchdog, drain tracker); and the control surface mounted
*outside* the fault middleware. Egress goes through `EgressTransport` (otelhttp client span → fault or
cache-box dispatch → metrics). gRPC has fault-only interceptors and **no cache-box**.

The evaluator sees labels, not requests: `atropos.http.{method,path,host,query,user_agent}` plus
`atropos.cachebox.workflow` from W3C baggage (`internal/interceptor/middleware.go`), or
`atropos.grpc.method` for gRPC. Rules match by injection point + label equality
(`internal/evaluator/static.go`). One decision per `Evaluate`; background faults are armed lazily on the
first matching request and deduplicated by rule name in the registry
(`docs/plans/2026-05-17-concurrent-multi-fault-execution.md`).

### 1.3 The wire contract (both directions)

| Direction | Endpoint | Purpose |
|---|---|---|
| SDK → manteion | `GET /api/v1/sdk/init` | readiness wait |
| SDK → manteion | `POST /api/v1/sdk/register` (`RegisterRequest{ID, Service, Version, Address, PollIntervalMs, Routes}`) / `DELETE /api/v1/sdk/register/{id}` | identity; `Address` is what every fanout below dials |
| SDK → manteion | `GET /api/v1/sdk/rules?service&version&instance_id` → `RuleSync{Version, Rules, ActiveFaults, FreezeCfg}` | authoritative desired state; empty set clears |
| SDK → manteion | `GET /api/v1/sdk/events` (SSE `rules_changed`) | poll nudge |
| SDK → manteion | `POST /api/v1/cache/ingest` (batched `WireEntry`, `batch_seq` per pair) | recording stream (W2) |
| SDK → manteion | `POST /api/v1/sdk/cachebox/drain` (`DrainReport`) | phase-end accounting (W3) |
| manteion → SDK | `/admin/fault[/{id}]`, `/admin/rules`, `/admin/cachebox[/delay]` | fanout control (`manteion-go/internal/atropos/client_*.go`) |
| manteion → SDK | `POST /cachebox/preload/{begin,chunk,commit,abort}` | staged replay-set install (W4/W5) |
| manteion → SDK | `GET /cachebox/fidelity?experiment_id&phase_id` | verdict input (W6) |
| scrape | `GET /metrics`, `/atropos/health` | Prometheus, ops |

Rulings that bind the daemon: **D1(b)** push-fanout is load-bearing (so a daemon's advertised
address must be real), with pull-inversion as the long-term direction; **D2** start-of-phase coverage
quorum = all instances alive at phase start; **D7** no silent terminal drops; **D8** one experiment per
mesh; **D9** the poll predicate defines "active". Plus the 2026-08-27 ruling that core-tier experiments
use only pod-scoped interventions (inline faults + freeze), because resource faults spend node
resources shared by every co-located pod on the single-node bed.

### 1.4 The bed

- Go port (`service-beds/microservices-demo-go`): eleven services, **one container per pod**, dependencies
  by env var (`PRODUCT_CATALOG_SERVICE_ADDR=productcatalogservice:3550`), clients build
  `http://<addr>/path` so the `Host` header is the env value verbatim, control surface on the app port
  (`MANTEION_ADVERTISE_ADDR=$(POD_IP):8080`). The upstream manifests still carry
  `sidecar.istio.io/rewriteAppHTTPProbers: "true"`: sidecar injection was anticipated by the source
  project.
- Original boutique (`service-beds/microservices-demo`): **gRPC everywhere** (`protos/demo.proto`) across
  Go, Java (adservice), C# (cartservice), Node (currency, payment), Python (email, recommendation,
  shopping assistant). This, not DeathStarBench, is the polyglot mesh already in the repo.
- k3s, single node, VM1. Network faults have never been exercised (no `NetworkResolver` anywhere).

---

## 2. Requirements the daemon inherits (non-negotiable)

1. **Same core, one implementation.** Keying (`canonical_v2` and friends), fail-closed replay (INV-1),
   record/replay split (INV-4), per-pair fidelity counters, W2/W3/W4/W5/W6 wire behavior, rule decode,
   fault registry stop-on-rule-removal, drain tracking, watchdog: all reused from `internal/*`, not
   re-implemented.
2. **Key identity across hosts (INV-2).** A request keyed by the in-process SDK and the same request
   keyed by the daemon must produce the same key, or preload sets recorded by one cannot be replayed by
   the other.
3. **One SDK instance per pod, addressable.** `ID` = pod name, `Service`, `Address` reachable by
   manteion (D1(b)); coverage quorum (D2) counts instances.
4. **Fail closed, loudly.** Configured-but-unreachable manteion is fatal (matches `Serve`); capture that
   is not in place must prevent registration rather than register a half-instrumented pod.
5. **Observer effect is accounted, not assumed.** Whatever the daemon adds to a measurement must be
   visible in telemetry and gated (the lab's methodology is gate-driven: E-DOSE, G2, INVALID verdicts).
6. **Pod-scoped by default.** No mechanism the sidecar owns may spend node resources shared with other
   pods without declaring it (the 08-27 ruling).
7. **Zero required manteion changes** for the first deployment. Additive fields are fine; the wire
   compatibility tests (`wire_compat_test.go`, `wire_fidelity_test.go`) stay green.

---

## 3. Flaws in the design so far

Grouped: (A) the explicit-loopback capture model, which is the user's proposal and the plan's;
(B) the 2026-04 plan's other choices; (C) the report's sketch; (D) in-process behavior a daemon should
fix rather than copy.

### A. Explicit loopback capture (`localhost:PORT` per dependency)

**A1. Coverage is opt-in per edge, and holes are silent.** Every dependency the operator forgets to
redirect is simply not instrumented: no rule matches, no record, no span, no error. The July incidents
(schemeless addresses masked for weeks; a baseline published but never acknowledged) are precisely
this failure class, and D1/D2 exist to make it impossible. A capture model that re-introduces it as a
manifest-editing discipline is a regression.

**A2. It corrupts cache keys.** With `PRODUCT_CATALOG_SERVICE_ADDR=localhost:19090`, Go's client sets
`Host: localhost:19090`. `canonical_v2` folds `strings.ToLower(r.URL.Host)` into the SHA-256 frame;
`exact_with_host`, `exact_with_body`, and `exact_with_host_norm` fold `r.Host` verbatim; only plain
`exact` ignores the host. Keys recorded through the loopback
proxy differ from keys recorded in-process for the identical request, so a preload set from one
instrument misses 100% on the other, the failure INV-2 was written against. (The fix, rewriting `Host`
back to the configured upstream before keying, is possible; §5.2 requires it for explicit mode. But
transparent capture never has the problem.)

**A3. The resolver moves into eleven manifests.** The report's argument for a daemon was that "the
daemon carries the resolver configuration in one place instead of in every service." Explicit ports do
the opposite: each service's env block becomes the resolver, and ingress interception additionally
requires re-pointing the Service object, probes, and the app's listen port. Per-service, per-dependency
surgery, with the coverage risk of A1.

**A4. No guard against double interception.** Every Go-port service already embeds `Serve` +
`EgressTransport`. Put a proxy underneath and each egress call is evaluated twice, spanned twice,
counted twice, and under a passthrough rule recorded twice (once with each key shape from A2). The
handoff's Q3 asked exactly this ("does a TCP proxy underneath the cache-box double-count or conflict?").
Nothing in the plan answers it; §5.12 does.

**A5. The per-node variant breaks both loopback and identity.** From a pod's network namespace,
`localhost` is the pod, not the node, so a per-node daemon cannot be reached at `localhost` at all
(hostNetwork or node-IP addressing would be needed). And the wire contract assumes one instance = one
address = one control surface; a node daemon hosting eleven services needs eleven admin listeners or a
`/svc/{name}/…` prefix, which is a wire change. Per-pod keeps identity 1:1 for free.

### B. The 2026-04 polyglot plan's other choices

**B1. It targets a control plane that does not exist.** The plan polls OPA bundles and reads a YAML
sidecar config; the shipped control plane is manteion's `RuleSync`/`CompiledRule` poll, SSE nudge,
fanout, staged preload, fidelity pull, and drain report, none of which existed in April. A daemon that
does not speak §1.3 is not an instrument in this lab.

**B2. Wrong first target.** Thrift decoders for DeathStarBench are the plan's largest work item. The
polyglot mesh already in `service-beds` is the original Online Boutique, which is gRPC only. The real gap
is **gRPC cache-box** (h2c, unary), which the in-process SDK also lacks. Thrift is YAGNI until a
DeathStarBench deployment is actually planned.

**B3. Resource faults in a sidecar stress the wrong cgroup.** Acknowledged in the plan's open question 3
and never resolved: CPU/memory stress in the sidecar container throttles the sidecar's cgroup, not the
app's. `shareProcessNamespace` + moving a stress process into the app's cgroup needs privilege the
sidecar should not have. On this bed resource faults are already off the core tier for node-coupling
reasons, so the daemon should simply not own them (§4, §5.6).

**B4. The thin SDK is under-specified in the direction that matters.** `POST /evaluate` over TCP per
custom hook, returning "a decision" the language client must then execute (sleep? raise? hang?) is
not a contract. Which side sleeps, how the hook span is parented, and what happens on error are the
whole design (§5.8).

**B5. No validity gate for the hop it adds.** The plan estimates 200–400 µs per call and stops. The
report is more honest ("one more process whose queueing lives inside every number"), but neither
defines a signal. Constant overhead is priced by the plain-phase bracket; *queueing* in the proxy
under load is not constant and must be measured and gated (§5.13). With both ingress and egress capture,
every edge crosses **two** proxies (caller's egress, callee's ingress), not one.

**B6. Trace topology is hand-waved.** "Synthetic child span on replay" does not say which
`service.name` the sidecar's spans carry, how the app's native OTel spans nest with them, or how
manteion's "no fault active in this phase" assertion (built on `atropos_fault_injections_total` and
`atropos.*` span events) continues to hold. §5.7 fixes this.

**B7. Wrong layering for the proxy.** The plan extends the existing per-connection TCP `network.Proxy`
"with per-call decisions". Cache-box and inline faults need an **L7** proxy (request/response
boundaries, headers, bodies, trailers for gRPC); toxics need **L4** access to a byte stream. Building
L7 out of the TCP proxy code is the hard way; building L4 toxics *inside* an L7 proxy's upstream
connection is the easy way, and it is exactly the deferred `NetworkEnvelope.Host = "inline"` semantics
the wire format already has a slot for.

**B8. Streaming and stateful services.** Passthrough-only for gRPC streams is the right call, but under a
recording rule a stream must be *counted* as uncovered (the same way an oversize body is counted as a
drop today), or coverage reads as clean. Stateful services remain out of scope (VISION), unchanged.

### C. The report's "daemon owns the proxy permanently"

**C1. Right diagnosis, missing mechanism.** The lifecycle problem is real (a `network.Proxy` fault only
listens while it runs; a service pointed at it gets connection-refused the rest of the time). "The
daemon owns the proxy permanently" is correct. But the sketch does not say how the service's traffic
reaches the daemon, which is the entire question; per-node conflicts with A5; and "boundary
observation" for polyglot services requires the L7 layer the sketch does not mention.

### D. In-process behavior the daemon should fix, not copy

**D1. Lazy arming.** Background faults start on the first matching request. For network toxics that is
backwards: a rule that says "throttle egress to redis" should take effect when the rule *applies*, not
when the next call happens to match. The daemon owns connection state, so toxics become armed policy
on the listener (eager arming, the multi-fault note's option C) with no per-request registry dance.

**D2. `NetworkResolver` as a Go callback.** A per-service closure mapping target → (listen, upstream)
cannot exist in a polyglot service. Under transparent capture it disappears: the original destination
*is* the upstream, and the target name comes from the `Host`/`:authority` header for HTTP/gRPC or from
the app's `*_ADDR` env block for raw TCP.

**D3. The control surface rides the app port.** `Serve` mounts `/admin/*` on the service's own listener
"outside the middleware", so a hang fault can never delay control, but the control plane is still
sharing the app's accept queue and `http.Server`. In a sidecar the admin surface gets its own port,
which is strictly better for the "an instrument must stay controllable while the service is faulted"
invariant.

---

## 4. Alternatives, per capability

"Better way" is per capability, not one mechanism. Verdicts assume the recommended per-pod,
transparent-capture sidecar of §5; each row also says what the in-process Go SDK keeps doing.

| Capability | In-process Go SDK (today) | Explicit-loopback sidecar (plan) | Transparent sidecar (proposed) | Other option, verdict |
|---|---|---|---|---|
| Inline faults at ingress (latency/error/hang) | middleware, in the handler goroutine | L7 proxy on a re-pointed app port | L7 proxy on the REDIRECTed inbound port; identical semantics (request held before the app sees it) | — |
| Inline faults at egress | `EgressTransport` | L7 proxy per dependency port | L7 proxy on the outbound REDIRECT port; original dst gives the target | — |
| Network toxics (rst, blackhole, latency, throttle, drip, retransmit_delay) | `network.Proxy` fault: unwired, lifecycle-bound, never exercised | TCP proxy in sidecar, still needs listen/upstream config | Toxic wrapper on the proxy's upstream `net.Conn` (HTTP) or a raw L4 relay (redis/kafka/postgres ports); always-on listener, toxics toggled by rules; implements `host=inline` | Toxiproxy via `Config.External`: L4 only, second process, no cache-box; reject. In-process always-on listener for Go only (handoff option 2): solves Go, not polyglot; do not build a third host |
| Resource faults (cpu/mem/disk/io) | in the service process: right cgroup, bounded by the container's own limit | wrong cgroup (B3) | **delegated to the node agent** (§5.14): the existing stress code runs as a child placed in the target's cgroup (`burn`, unchanged semantics), or the target's cgroup limits are lowered (`quota`, throttling, node-neutral); `disk` stays in-process only | In scope (M4). No external tool offers an equivalent |
| Cache-box, egress HTTP | `EgressTransport` dispatch (shipped, the measured instrument) | works only if `Host` is rewritten before keying (A2) | same engine, original `Host` preserved, keys byte-identical (golden test, §5.13) | — |
| Cache-box, egress gRPC | none | none | deferred (§7); when built: h2c L7 proxy; unary only; key = `:path` + body digest + allowlisted metadata; replay reproduces `grpc-status`/`grpc-message` trailers; miss = `grpc-status 14`; streams passthrough and counted | Per-language ports of the engine: 4 languages × keying + fail-closed + W4/W5/W6; reject |
| Cache-box, ingress (VISION Phase 1.5, not built) | needs a `ResponseWriter` recorder | possible at the ingress proxy | possible at the ingress proxy, and cheaper than in-process (the handler is skipped entirely) | build it once, in the proxy |
| Custom / transient injection points | `Interceptor.Hook` in Go | `POST /evaluate` over TCP, unspecified | thin client over loopback TCP to the daemon's hook API (§5.8, deferred: no Go-port service has a hook); daemon executes latency/hang server-side, returns error decisions for the client to raise | this is the one place loopback is right |
| OTel | otelhttp spans in-process | proxy spans, unspecified identity | proxy spans with `service.name=<service>` and `atropos.host=sidecar`; app's native OTel nests under/over them via propagated `traceparent` | — |
| Metrics | `/metrics` on app port | `/metrics` on sidecar | `/metrics` on the admin port, same names, plus proxy overhead histograms | — |
| Coverage guarantee | by construction (code) | by discipline (manifests) | by construction (REDIRECT covers every TCP connect); startup self-check refuses to register if capture is absent | — |
| Config burden | `Config` in code + env | N env vars per service + port remaps | one injector patch; optional YAML for protocol classification and routes | — |

Process-model alternatives, with verdicts:

| Model | Verdict | Why |
|---|---|---|
| Per-pod sidecar, transparent capture | **Recommended** | 1:1 identity under the existing contract; coverage by construction; original `Host` preserved; pod-scoped; injection is one kustomize patch |
| Per-pod sidecar, explicit loopback ports | Keep as a *mode* (dev, docker-compose, M1 bring-up), not the production mechanism | A1–A3; requires `Host` rewrite before keying |
| Per-node daemon (ambient / ztunnel-style) | Reject for now | A5; single-node bed makes it one shared-fate process for the whole mesh |
| eBPF `cgroup/connect4` redirection instead of iptables | Later, same semantics | fewer moving parts than iptables but more kernel dependence; the proxy design is unchanged, only the capture primitive |
| Per-language SDK ports | Reject except the ~100-line thin client | 4+ languages × the whole fidelity stack |
| In-process always-on listener (Go only) | Reject | third host of the core; does not help polyglot |
| Toxiproxy | Reject | L4 only; the toxics already exist in `internal/fault/network` |

---

## 5. Proposed architecture

### 5.1 Components

```
┌────────────────────────────── pod ───────────────────────────────────────────┐
│                                                                              │
│  init: atropos-init (NET_ADMIN)                                              │
│    iptables -t nat: OUTPUT tcp !uid=atropos !dport∈{excludes} → REDIRECT 16001│
│                     PREROUTING tcp dport=<app ports>          → REDIRECT 16006│
│    writes /var/run/atropos/capture.ok                                        │
│                                                                              │
│  ┌──────────────────────┐        ┌────────────────────────────────────────┐  │
│  │ app (any language)   │ ─────► │ atroposd (uid atropos)                 │  │
│  │ native OTel optional │ egress │  :16001 egress L7 (HTTP/1.1) + L4      │──┼─► upstream
│  │                      │        │  :16006 ingress L7 (HTTP/1.1)          │◄─┼── callers
│  │ thin client optional │ ◄───── │  :16021 hook API (loopback only)       │  │
│  │                      │ ingress│  :16020 control: /admin/* /cachebox/*  │◄─┼── manteion fanout,
│  └──────────────────────┘        │         /cachebox/fidelity /metrics    │  │   Prometheus
│                                  │         /atropos/health                │  │
│                                  │  core: evaluator · cache-box · fault   │  │
│                                  │        registry · push · drain ·       │──┼─► manteion
│                                  │        manteion client (register/poll/ │  │   (register, poll,
│                                  │        SSE/watchdog) · OTel exporter   │  │   ingest, drain)
│                                  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

Port numbers are defaults, configurable; they are chosen to not collide with the boutique's service
ports or with Istio's, since the manifests carry Istio annotations.

- **Control-plane agent**: the existing `manteion.go` / `manteion_client.go` / `register.go` /
  `cache_push.go` / `cache_drain.go` logic, one instance per pod.
- **L7 proxy**: `net/http` server on both listeners; `httputil.ReverseProxy` over an HTTP/1.1 transport.
  (The h2c/gRPC variant, `h2c` on the listeners plus `http2.Transport{AllowHTTP: true}` upstream, is
  specified in §5.5 and deferred by §7.) Request flow at each listener = today's middleware flow: build
  labels → `Evaluate` → cache-box dispatch (egress) or fault dispatch → forward → metrics.
- **L4 toxic layer**: a `net.Conn` wrapper applied to the upstream dial (downstream direction) and to
  the accepted client connection (upstream direction) when a network rule is armed for that target;
  reuses the `Toxic`/`ConnToxic` implementations. Non-HTTP ports (classified in config: redis, kafka,
  postgres) get a raw L4 relay with the same wrapper and no L7 parsing.
- **Hook API**: loopback TCP, JSON, for thin clients (§5.8).
- **Control surface**: the same handlers `Serve` mounts today, on their own port.

### 5.2 Traffic capture

**Transparent (default).** The init container installs nat rules: outbound TCP from any uid except the
proxy's own is REDIRECTed to 16001, except an exclude list (manteion, the OTel collector, and any
configured ports); inbound TCP to the app's declared ports is REDIRECTed to 16006. The proxy recovers
the original `(ip, port)` with `SO_ORIGINAL_DST` and dials it. DNS is untouched (UDP), so the app still
resolves `productcatalogservice` to its ClusterIP and the `Host` header stays
`productcatalogservice:3550`, byte-identical to in-process.

Startup self-check: `atroposd` refuses to register with manteion unless `/var/run/atropos/capture.ok`
exists and a probe connection from the app's uid lands on 16001. No capture → no instance → D2's
coverage quorum fails at phase start instead of a plausible empty run.

**Explicit (mode `capture: explicit`).** For docker-compose, local development, and the M1 bring-up:
a static map `listen port → upstream host:port`. The proxy **rewrites `r.Host` and `r.URL.Host` to the
mapped upstream before label extraction and keying** (this is the A2 mitigation and is mandatory), and
forwards to it. Same code path from that point on.

Prerequisites to verify on VM1 before M2 (not verified in this session; the SSH transport timed out):
kernel version, cgroup v2 mounted at `/sys/fs/cgroup`, `iptables` (nft or legacy backend) usable from
the init image, PodSecurity allowing `NET_ADMIN` in `default`, and the k3s Kubernetes version (native
sidecar containers, `initContainers[].restartPolicy: Always`, need ≥ 1.29; otherwise ordering is
best-effort and the app may see connection-refused until the proxy listens, which is fail-closed but
noisy).

### 5.3 Identity, registration, and what manteion sees

- `ID` = pod name (both containers share the pod hostname, so `os.Hostname()` gives the same value the
  in-process SDK would have used), `Service` from `ATROPOS_SERVICE` (the injector copies the pod's `app`
  label), `Version` from `MANTEION_SERVICE_VERSION`, `Address` = `$(POD_IP):16020`.
- Manteion fans out to the registered address exactly as today. Prometheus scrapes 16020 for atropos
  metrics (the app's own metrics, if any, stay on the app port).
- **Additive register fields** (accepted, §7): `host: "sidecar"` and `capabilities: [...]`, where the
  set is `inline`, `network`, `cachebox`, plus `resource` only when the node agent socket (§5.14) is
  present at startup. Manteion stores them on `SDKInstance` and filters at delivery time: the
  per-instance poll (`?service&instance_id`) and every fanout skip rules whose fault category the
  instance did not advertise, so an instance never receives what it cannot execute (M3). Defense in
  depth on the daemon: an unsupported rule is skipped per rule (never per sync, so one rule cannot
  wedge a whole `RuleSync`), logged at WARN, and counted in `atropos_rules_unsupported_total{category}`
  (D7). Today's manteion ignores both fields, so M1 and M2 run before M3 lands.
- Routes: optional list in the sidecar YAML; matches the direction of the parked OpenAPI migration
  (`registerroutes-to-openapi-migration.md`) better than asking a Python service to publish Go structs.

### 5.4 Evaluation and labels

Identical label set, produced from the original request: `atropos.http.{method,path,host,query,
user_agent}` and `atropos.cachebox.workflow` from the `baggage` header; for h2c gRPC,
`atropos.grpc.method` = `:path` plus `atropos.grpc.user_agent`. Rules therefore match exactly as they
match in-process, and manteion's rule compiler is unchanged.

### 5.5 Cache-box

- HTTP: unchanged engine and dispatch (`internal/interceptor/cachebox.go` logic hosted in the egress
  listener). Same headers (`X-Atropos-Cache-*`), same fail-closed 503 `application/problem+json`, same
  context gating on `(experiment_id, phase_id)`, same preload/fidelity/drain handlers on 16020. One
  subtlety the golden test (§5.13) exists for: the app's outbound request arrives at the egress listener
  as a *server-side* request in origin form, so `r.URL.Host` is empty and only `r.Host` is set, whereas
  the in-process `EgressTransport` keys a *client-side* request whose `URL.Host` is populated.
  `canonical_v2` reads `r.URL.Host`. The listener therefore rebuilds the client-side view
  (`URL.Scheme`, `URL.Host` from `Host`, and `:authority` for h2c) before label extraction and keying.
- gRPC (unary; **deferred by §7**, kept as the specification for when a gRPC bed is targeted): key
  strategy `grpc_v1` = length-prefixed SHA-256 over (`:path`, `:authority`, allowlisted
  metadata, SHA-256 of the raw request body bytes). Caveat to document: protobuf serialization is
  deterministic per encoder but not canonical across languages (map ordering, unknown fields); the
  existing `CollisionsDivergent == 0` fidelity check remains the runtime proof that the projection is
  complete. Replay reproduces status, response headers, body frames, and trailers; a miss fails closed
  with `grpc-status: 14 UNAVAILABLE` and `grpc-message: cachebox_miss key=…`. Streams are forwarded and,
  under a recording rule, counted as `record_dropped` with reason `unsupported_stream` so coverage is
  honest.
- Ingress cache-box (future): the ingress listener is the natural host; nothing in this design blocks
  it.

### 5.6 Faults

- **Inline** (latency, error, hang): at the listener that matches the rule's injection point, before
  forwarding. Semantics identical to the middleware (hang holds the request open; error short-circuits
  with the configured status).
- **Network**: `NetworkEnvelope.Host` gains its intended meaning. `host=inline` (default in the daemon)
  applies the toxic to the proxied connection for connections whose original destination matches
  `network.target` (by `Host`/`:authority` for HTTP, by the app's `*_ADDR` env map for raw TCP), at the
  configured `scope` fraction, in the configured direction. Arming is **eager**: the rule's apply
  installs the policy; the first affected connection is the next one opened. Rule removal disarms and
  closes affected connections (mirroring `stopBackgroundFaults` semantics: a toxic must not outlive its
  phase). A toxic the client is supposed to *observe* (RST, blackhole) is reproduced on the client leg
  (hijack + `SetLinger(0)` / hold), not translated into an HTTP error, so what the app sees is what a
  real network fault looks like. `host=proxy` (the standalone `network.Proxy` fault) stays valid for the
  in-process host only.
- **Resource**: never executed inside the sidecar (wrong cgroup, B3). When the node agent socket is
  present the sidecar advertises `resource` and delegates each resource rule to `atropos-agent` (§5.14),
  which runs the *existing* stress implementations as a child process placed in the target container's
  cgroup, so semantics, ramps, and the `faultparams` schemas are unchanged. Without the socket the
  category is not advertised and, as defense in depth, rejected per rule with a counted, logged reason.

### 5.7 Telemetry

- Spans: the daemon runs its own OTel SDK with resource `service.name=<service>`,
  `service.instance.id=<pod>`, and `atropos.host=sidecar`. Ingress listener → SERVER span whose parent is
  the incoming `traceparent`; the outgoing request to the app carries the new context, so an app with
  native OTel nests its server span *under* the sidecar's. Egress listener → CLIENT span parented on the
  app's outgoing `traceparent` (the app's client span, if it has one). `atropos.fault.inject` and
  `atropos.cachebox.check` spans and every `atropos.*` event keep their names, so manteion's
  telemetry-based assertions ("no fault active in this phase") are unchanged. Trace queries that count
  spans per service must be told about the extra span layer; document it in the experiment-spec template.
- Metrics: same names as today plus `atropos_proxy_overhead_seconds{point=ingress|egress}` (time in the
  proxy excluding upstream time), `atropos_proxy_inflight{point}`, `atropos_proxy_accept_queue`, and
  `atropos_rules_unsupported_total{category}`.

### 5.8 Thin client API (the loopback that is right; deferred by §7)

No Go-port service uses a custom or transient hook today, so nothing needs this until a polyglot bed
is targeted. The contract is fixed now so the daemon's hook span and decision shapes do not drift.

Loopback TCP `127.0.0.1:16021`, HTTP/1.1 + JSON, reachable only inside the pod's network namespace.
One call per hook:

```
POST /v1/hook
{"name":"charge_card","labels":{"atropos.workflow":"checkout"},"traceparent":"00-…"}

200 {"decision":null,"traceparent":"00-…"}                         # no rule matched
200 {"decision":{"name":"r1","fault":"latency","executed":true},"traceparent":"00-…"}
      # daemon slept (or held for hang) BEFORE responding; client just continues
200 {"decision":{"name":"r2","fault":"error","status":503,"message":"injected fault"},"traceparent":"00-…"}
      # client raises/returns the error itself
```

The daemon owns the hook span (`atropos.hook.<name>`), parents it on the supplied `traceparent`, and
returns the child context so the client can parent its own spans. Latency and hang are executed
server-side inside the request, so a client is ~100 lines in any language: build JSON, POST, read
`decision`, raise on `error`. Reference clients for Python, Node, Java, C#. Custom and transient
injection points are the only reason a service needs this; everything else is zero-code.

### 5.9 Configuration

Environment, same names as `Serve` where they exist: `ATROPOS_SERVICE` (new), `MANTEION_URL`,
`MANTEION_ADVERTISE_ADDR` (default `$(POD_IP):16020`), `MANTEION_SERVICE_VERSION`,
`MANTEION_INIT_TIMEOUT`, `OTEL_EXPORTER_OTLP_ENDPOINT` / `COLLECTOR_SERVICE_ADDR`. Optional YAML
(`/etc/atropos/atroposd.yaml`):

```yaml
capture: transparent            # transparent | explicit
agent_socket: /var/run/atropos/agent.sock   # optional; present → advertise the resource capability
inbound_ports: [8080]           # app ports to intercept at ingress
exclude_outbound: [4317, 8080]  # otel collector, manteion (host from MANTEION_URL is also excluded)
protocols:                      # default is http (with h2c autodetect on the preface)
  6379: tcp                     # redis: L4 relay, toxics only
  9092: tcp                     # kafka
targets:                        # raw-TCP name map for network.target; HTTP uses Host
  redis: redis-cart:6379
explicit:                       # only for capture: explicit
  19090: productcatalogservice:3550
routes:                         # optional, published at register
  - {method: GET, path: /products}
```

### 5.10 Deployment

- **M1/M2:** a kustomize patch per service adds the init container, the `atroposd` container, a shared
  `emptyDir` at `/var/run/atropos`, resources (`cpu: 250m`, `memory: 128Mi` requested; declared in the
  experiment spec), and the env block. Skaffold builds one extra image (`atroposd`) from
  `cmd/atroposd`.
- **M4:** the agent DaemonSet (`deploy/agent/`) with its hostPath socket directory mounted read-only
  into every sidecar (§5.14).
- **Later:** a mutating admission webhook keyed on a pod label, the same shape the plan proposed.
- **docker-compose:** `atroposd` as a companion container with `network_mode: service:<app>` and
  `cap_add: [NET_ADMIN]`, or `capture: explicit` when capabilities are unavailable.

### 5.11 Core extraction (the prerequisite refactor, M0)

Today `package atropos` is simultaneously the wire-type package manteion imports, the in-process host,
and the instrument core; the host wiring is unexported. `cmd/atroposd` lives in the same module and can
import `internal/*`, so:

- `internal/wire`: the wire structs (`RegisterRequest`, `RegisterResponse`, `RuleSync`, `CompiledRule`
  and friends, `FaultRequest`, `NetworkEnvelope`, `DelayRequest`, `Preload*`, `DrainReport*`,
  `FidelitySnapshot*`, `Route`, `HealthStatus`, `FaultStatus`, `StaticRule`, `CacheBoxWireEntry`,
  `CacheBoxStats`) move here; the root package keeps **type aliases** with identical names and JSON
  tags, so manteion's imports and the wire-compat tests are unaffected (the same trick `types.go`
  already uses for `CacheBoxContext`).
- `internal/instrument`: `Identity` (the one-time instance-id resolution), `Core` (fidelity registry,
  cache-box, push client, host + demo evaluators, fault registry, interceptor, drain tracker),
  `ControlSurface(core) http.Handler` (today's `controlMux` minus the business handler), and
  `ControlPlane` (today's `connectManteion` + `applyTargets` + `apply`).
- Root `atropos.Serve` becomes: build `Core`, connect `ControlPlane`, return `ControlSurface` +
  `ingressMiddleware(cfg.Handler)`. `EgressTransport` and the gRPC interceptors are unchanged.
- `cmd/atroposd`: build `Core`, connect `ControlPlane`, serve `ControlSurface` on 16020, start the
  listeners and the hook API.

No behavior change; the existing test suite is the regression gate.

### 5.12 Mixed-fleet guard

A pod is instrumented by exactly one host. The injector sets `ATROPOS_HOST=sidecar` on the app
container; `atropos.Serve` in-process reads it and, when set, does not register, does not poll, does not
apply faults or cache-box, and does not mount the control surface (tracing-only behavior, as if
`MANTEION_URL` were unset, plus a one-line log). Optional manteion follow-on: flag two registrations
with the same pod name and different addresses.

### 5.13 Validity gates the daemon adds

1. **Key-identity golden test (INV-2, CI):** for a corpus of requests, the in-process `EgressTransport`
   key and the daemon's key are byte-equal for every strategy, in both capture modes.
2. **Instrument-equivalence experiment (before any sidecar result is reported):** re-run E-DOSE through
   the sidecar; the dose-response slope must reproduce the in-process value (7.1–7.3, the path-traversal
   count) within its confidence band. This is the sidecar's version of "inject a known delay and
   confirm the attribution recovers it".
3. **G-proxy (per phase):** `atropos_proxy_overhead_seconds` p99 below a declared bound and sidecar CPU
   below a declared fraction of its limit, else the phase verdict is INVALID with reason
   `proxy_saturated`. The plain-phase bracket prices the constant part; this gate watches the queueing
   part.
4. **Capture self-check (startup):** no `capture.ok` → no registration (§5.2).

### 5.14 Node agent (`atropos-agent`): resource faults in the right cgroup

**Why a second component.** A resource fault must spend the *target's* CPU, memory, or I/O budget, and
only a process inside the target container's cgroup does that. The sidecar is a different container
with its own cgroup (B3). A privileged per-node agent can place a process into any cgroup on the node;
nothing else can.

**Shape.** A DaemonSet, one pod per node (one, on VM1), `hostPID: true`, the host cgroup filesystem
mounted read-write at `/host/sys/fs/cgroup`, `privileged: true` (or `CAP_SYS_ADMIN` +
`CAP_SYS_RESOURCE`, verified in M4), and a Unix socket on a hostPath, `/var/run/atropos/agent.sock`,
that sidecars mount read-only. The agent has no manteion identity of its own: the sidecar is the SDK
instance, receives the rule, owns the fault span and the registry entry, and calls the agent to execute.

**Mechanisms.** Selected per rule by a new optional param, `mechanism`, on `faultparams.ResourceCPU`,
`ResourceMemory`, and `ResourceIO` (additive; `Validate` accepts `""`, `burn`, `quota`):

- `burn` (default; the in-process semantics, unchanged): the agent re-executes itself as
  `atropos-agent stress <category> <params-json>`, writes the child's PID into the target container's
  `cgroup.procs` *before* signalling it to start, and the child runs the existing
  `internal/fault/resource/*` implementation with ramp-up, sustain, and ramp-down. The child's CPU and
  memory are charged to the target's own limit (`cpu: 200m`, `memory: 128Mi` on every boutique
  container), so the pod's node footprint is bounded by exactly the limit its normal traffic already
  has; whether that is node-neutral depends on the node's headroom, which the experiment spec declares
  per run. One subtlety: the child runs in the host cgroup namespace, so `AvailableCPUs()` (which reads
  `/sys/fs/cgroup/cpu.max` relative to its own view) would see the host root; the agent reads the target
  cgroup's `cpu.max` and passes the effective CPU count to the child explicitly.
- `quota` (a new intervention: throttling rather than contention, and node-neutral because it removes
  capacity instead of consuming it): the agent lowers the target cgroup's `cpu.max`, raises
  `memory.high` pressure, or sets `io.max` rbps/wbps by the requested fraction for the fault's duration,
  expresses the ramp as stepped quota changes, and restores the original value on stop, on agent
  restart (a journal file per active fault), and when the sidecar disappears (the sidecar heartbeats;
  a heartbeat missing past `max(3 × poll interval, 30s)` restores, the same grace the fault watchdog
  uses). `quota` is the mechanism that satisfies the 08-27 pod-scoping ruling outright.

`disk` (fill) has no cgroup analogue and stays in-process-only; the sidecar does not advertise it.

**Local API (JSON over the socket).** `POST /v1/faults {pod_uid, container, fault: FaultRequest}` →
`{id}`; `DELETE /v1/faults/{id}`; `POST /v1/faults/{id}/heartbeat`; `GET /v1/faults`. The agent takes
the caller's PID from `SO_PEERCRED`, maps it through `/proc/<pid>/cgroup` to a pod UID, and refuses any
request whose `pod_uid` differs: **a sidecar can only fault its own pod.** Results (actual duration,
error, detail) stream back on the same connection so the sidecar records them on its fault span exactly
as `Handle.SetOnResult` does in-process.

**Cgroup resolution.** Pod UID + container name → cgroup path via the containerd CRI socket
(`/run/k3s/containerd/containerd.sock`, `ContainerStatus` → cgroup path), with a fallback that scans
`/host/sys/fs/cgroup/kubepods*.slice/*pod<uid>*/cri-containerd-*.scope`. The exact layout on VM1's k3s
is an M4 precondition to verify, together with cgroup v2 being the mounted hierarchy.

**Telemetry and registry.** The sidecar's `atropos_fault_injections_total` and its fault span carry the
injection, as today. The agent adds `atropos_agent_active_faults{mechanism}` and
`atropos_agent_errors_total{reason}` on its own `/metrics` for ops. The sidecar registers the fault in
its `FaultRegistry` under the rule name, so rule removal stops it through the agent and the "rule
removal is the fault-stop signal" invariant holds.

**What does not change.** Rule shapes beyond one optional param; manteion's validation (through
`faultparams`); the in-process host, which executes `burn` natively and rejects `quota` at decode until
it learns it.

---

## 6. Staged plan

| Milestone | Deliverable | Exit criterion | Rough size |
|---|---|---|---|
| **M0** core extraction | `internal/wire`, `internal/instrument`; root aliases; `Serve` as thin host | `go test ./...` green; `wire_compat_test.go` / `wire_fidelity_test.go` untouched and green; manteion builds against the branch with no changes | move-mostly, ~2.5k LOC touched |
| **M1** `atroposd` HTTP, explicit mode | egress + ingress L7 listeners (HTTP/1.1), inline faults, cache-box, network toxics on upstream conns, control surface on 16020, hook API | one Go-port service (recommend `recommendationservice`, whose egress to productcatalog is the structurally-1-key edge used for instrument validation on 08-06) runs with `ATROPOS_HOST=sidecar` and its `PRODUCT_CATALOG_SERVICE_ADDR` pointed at the sidecar's explicit port; record → drain → preload → freeze → replay passes end-to-end; key golden test green | ~1.5k LOC + tests |
| **M2** transparent capture + injection | init container, iptables rules, capture self-check, kustomize overlay, mixed-fleet guard, G-proxy metrics, `host`/`capabilities` in the register payload | whole Go-port fleet deployable in sidecar mode behind an overlay (the in-process overlay stays the default for reported experiments until the daemon has matured, §7); E-DOSE reproduces through the sidecar; G-proxy bound chosen from the measurement | ~400 LOC + manifests; VM1 prerequisites verified first |
| **M3** manteion capability filter | `SDKInstance.Host`/`Capabilities` persisted from register; poll and fanout skip rules whose category the instance did not advertise; `faultparams` gains `mechanism` | a resource rule attached to a sidecar-only service is filtered server-side and the sidecar's `atropos_rules_unsupported_total` stays at zero | manteion-go, ~300 LOC |
| **M4** node agent | `cmd/atropos-agent` DaemonSet, socket API with the peer-cred own-pod check, `burn` via cgroup placement, `quota` via cgroup limits with a restore journal, sidecar delegation and the `resource` capability | CPU `burn` on one service through the agent reproduces that service's in-process dose-response; `quota` shows a throttling dose-response; `kill -9` of the agent restores every quota | ~900 LOC; VM1 cgroup-v2 layout verified first |
| **Deferred** (§7) | gRPC/h2c cache-box (§5.5), thin-client hook API (§5.8), Thrift/DeathStarBench | revisit when a non-Go bed is targeted | — |

M1 before M2 is deliberate: explicit mode needs no cluster privileges, so the proxy's correctness (keys,
fail-closed, preload, drain) is validated before iptables enters the picture.

**Status (2026-09-24, branch `feat/atroposd`):** M0 done (`86c40a9`..`220b48d`, plan
`2026-09-24-atroposd-m0-core-extraction.md`); M1 done (`39ac174`..`97aaf88`, plan
`2026-09-24-atroposd-m1-daemon.md`). Two findings changed the M1 shape: the inline `error` fault had never
short-circuited a response in either host (fixed in the shared middleware), and network toxics for
`host=inline` landed as a request-level `ToxicTransport` with hijack-based client-leg reproduction
rather than a per-connection dialer wrapper (§5.6 semantics kept: the app observes a reset or a hang).

---

## 7. Decisions (taken 2026-09-24)

1. **Target: the Go-port fleet only, for now.** Consequences: the L7 proxy is HTTP/1.1 only; the
   original polyglot boutique and DeathStarBench are out of scope; gRPC (§5.5) and Thrift are deferred.
2. **Capture: per-pod sidecar with transparent capture**, as recommended. Explicit mode is kept for the
   M1 bring-up and for docker-compose, with the mandatory `Host` rewrite (§5.2).
3. **Resource faults: build the cgroup node agent** (§5.14, M4). The sidecar never executes them itself;
   it advertises `resource` only when the agent socket is present.
4. **The Go SDK stays a first-class host until the daemon has matured.** The in-process overlay remains
   the default for reported experiments; sidecar mode is opt-in per overlay behind the mixed-fleet guard
   (§5.12); M0 must be behavior-neutral for `Serve`.
5. **Manteion: accept the additive `host`/`capabilities` register fields and build the delivery-time
   capability filter** (M3).
6. **gRPC cache-box: deferred.** The Go boutique is all HTTP/1.1; the thin-client hook API (§5.8) is
   deferred with it, since no Go-port service uses a custom hook.

---

## 8. Risks

- **Cluster privileges.** `NET_ADMIN` for the init container and, for M4, a privileged DaemonSet. On a
  private single-node k3s this is acceptable; it must be stated in the experiment specs.
- **Proxy queueing under load.** The reason G-proxy exists. The sidecar's CPU shares the single node with
  the app it measures; its limit is part of the measured system and must be declared per run.
- **Two hops per edge** when both ingress and egress capture are on. Inbound capture is per-service
  configurable; egress-only capture halves the overhead where ingress faults are not needed.
- **Connection semantics change.** The app's client pool now terminates at the proxy; keep-alive and
  HTTP/2 multiplexing behavior differ from a direct connection. Toxics that the client must observe are
  reproduced on the client leg (§5.6), but connection-count metrics will differ from in-process runs.
- **Protobuf non-canonical encoding** for `grpc_v1` keys across languages; guarded by the divergent-collision
  check, but a mixed-language caller set for one callee may need the `_norm`-style key relaxation the
  HTTP side already grew.
- **Ordering at startup** on k3s versions without native sidecar containers (connection-refused until the
  proxy listens; fail-closed, but noisy in probes).
- **Agent privilege.** A privileged DaemonSet with host PID and cgroupfs access is the most powerful
  component in the mesh. The peer-cred own-pod check and the restore journal are its containment; its
  presence is declared in every experiment spec.
- **Memory `burn` inside the target cgroup** can make the OOM killer pick the app instead of the stress
  child. The same risk exists in-process today; `memory.high`-based `quota` is the gentler alternative.
- **Cgroup layout drift** across k3s and containerd versions. The CRI lookup is primary and the path scan
  is the fallback; both are verified on VM1 before M4.
- **Scope creep.** Thrift, gRPC, the thin clients, and ingress cache-box are deferred; the node agent is
  in scope (§5.14). Anything beyond that is a separate design note.

---

## Appendix A. Where the current code goes

| Today (root package) | M0 home | Notes |
|---|---|---|
| `serve.go` | `internal/instrument` (+ thin `Serve` adapter in root) | `controlMux` → `ControlSurface` |
| `manteion.go`, `manteion_client.go`, `register.go` (apply, watchdog) | `internal/instrument/controlplane` | unchanged logic |
| `cache_push.go`, `cache_drain.go` | `internal/instrument` | unchanged logic |
| `admin.go`, `rules_admin.go`, `cachebox_admin.go`, `cachebox_preload.go`, `cachebox_fidelity_handler.go`, `health.go`, `metrics.go` | `internal/instrument` (handlers) | mounted by `ControlSurface` |
| `compiled_rule.go` (decoders), `faultparams` | `internal/wire` + decoders in `internal/instrument` | `faultparams` stays public (manteion imports it) |
| `types.go`, `sync.go`, `routes.go`, wire structs | `internal/wire` with root aliases | JSON tags pinned by the wire tests |
| `middleware.go`, `atropos.go` (`EgressTransport`, `DefaultInterceptor`) | root, unchanged | Go-host-only API |
| `internal/{cachebox,evaluator,interceptor,fault,trace}` | unchanged | the shared core |
| new: `cmd/atroposd`, `internal/proxy` (L7 listeners, L4 relay, toxic conn wrapper, original-dst), `internal/hookapi` (deferred), `deploy/sidecar/` | — | the daemon |
| new: `cmd/atropos-agent`, `internal/agent` (socket API, peer-cred check, cgroup resolution, stress re-exec, quota journal), `deploy/agent/` | — | the node agent (M4) |
