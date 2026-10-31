## Project Intention
You are an agent that's helping me build a system for tracking, synthetically injecting faults and managing their blast radius in a distributed system. Specifically, a lot of the ideas overlap with Gremlin Choas Engineering tools but there are key differences here.

## Request Payload Evaluator
A part of the project deals with parsing a payload tree (JSON) in a possible HTTP POST request body and evaluating it against a graph built from rules defined by the developer. This rule engine also tells what fault should be injected if it should be (evaluation succeeds against the graph).

## Fault Injector
We want multiple types of faults so let's define a taxonomy:
1. Injection point:
1.1. Inbound - when the request hits the service
1.2. Outbound - when the service makes a request to another service to complete the parent request
1.3. Transient - when the original request has completed (so leaving some side effects)
1.4. Custom - annotated code blocks by the developer
2. Fault type:
2.1. Resources:
2.1.1. CPU
2.1.2. Memory
2.1.3. Disk
2.1.4. I/O
2.1.5. GPU
2.2. Network:
2.2.1. Latency
2.2.2. Packet drops
3. Duration

## OpenTelemetry integration
We want to define a nice, easy to use interface for the developer such that when faults are triggered, they are properly instrumented with OpenTelemetry and their effects can be observed.

## Observability
We want to eventually allow this system to be observed on Grafana alongside the service the SDK is embedded in. This means we need to define a clear interface for exporting metrics, traces and logs to a backend (e.g. Prometheus + Loki + Tempo). The client need not be aware of these details. It should be configurable via environment variables or a config file.

## Request correlation
Another important tool that is in not Gremlin is the ability to correlate requests. This means that when a fault is triggered, it should be possible to see the few requests that led to up to the fault or sort of caused it to trigger. There might be multiple causes or rather a failure cannot be traced to a single request.

## Admin Endpoints

`atropos.Serve` mounts the runtime-control endpoints on the handler it
returns, at the exact paths below, OUTSIDE the fault/OTel middleware. There
are no public handler factories to mount by hand.

### /admin/fault

Runtime fault injection control against the admin fault slot (a second
evaluator composed after the host rule evaluator — armed admin faults never
shadow manteion rules).

| Method | Path           | Body             | Response |
|--------|----------------|------------------|----------|
| GET    | `/admin/fault` | —                | 200 `{"active": bool, "faults": [...]}` JSON |
| POST   | `/admin/fault` | `FaultRequest` JSON (see below) | 201 `{"active": true, "faults": [...]}` JSON |
| DELETE | `/admin/fault` | —                | 200 `{"active": false}` JSON |
| DELETE | `/admin/fault/{id}` | —           | 200 `{"active": false}` JSON (clears one slot) |

POST body is the platform's unified `FaultRequest` shape (the same struct
compiled rules embed): first-class envelope fields plus a `params` object
whose schema per `(category, fault_type)` is defined by the exported
`faultparams` package:

```json
{
  "id": "optional-slot-id",
  "category": "inline|network|resource",
  "fault_type": "latency",
  "duration_ms": 5000,
  "ramp_up_ms": 0,
  "ramp_down_ms": 0,
  "network": {"target": "redis", "direction": "upstream", "scope": 1.0},
  "params": {"delay": "500ms", "jitter": "50ms"}
}
```

`network` is required for (and exclusive to) the `network` category.
Common `params` examples: inline latency `{"delay","jitter"}`; inline error
`{"status_code","message"}` (defaults 500 / `"injected fault"`); inline hang
`{"duration"}`; resource cpu `{"target_load","window","mechanism"}`. On
resource faults `mechanism` is `burn` (default: the stress code runs in the
target's cgroup, via the node agent in sidecar mode) or `quota` (the node
agent lowers the target cgroup's `cpu.max`/`memory.high` instead); the
in-process host has no cgroup authority, so it skips a `quota` fault per
rule in a rule set (see `/admin/rules`) and rejects it with 400 as a single
admin slot. See `faultparams/params.go` for the full catalogue.

### /admin/cachebox

Runtime cache-box control.

| Method | Path                    | Body                                                                          | Response |
|--------|-------------------------|-------------------------------------------------------------------------------|----------|
| GET    | `/admin/cachebox`       | —                                                                             | 200 `Stats` JSON |
| POST   | `/admin/cachebox/delay` | `{"mu": float, "sigma": float, "seed"?: uint64, "context"?: CacheBoxContext}` | 204 (replaces delay source with lognormal distribution; `context` is accepted on the wire, not yet honoured) |
| DELETE | `/admin/cachebox`       | —                                                                             | 204 (clears store; preserves lifetime counters) |

### /admin/rules

Runtime rule-set management against the host evaluator.

| Method | Path           | Body                                            | Response |
|--------|----------------|-------------------------------------------------|----------|
| GET    | `/admin/rules` | —                                               | 200 `[]StaticRule` JSON (the installed, decoded rules) |
| POST   | `/admin/rules` | `[]CompiledRule` JSON; `null` clears the set    | 204 (atomic replace) or 400 `{"error": ...}` |

POST is the push side of the single rule-apply path the manteion sync uses
(`applyRules` in `internal/instrument/register.go`): decode against this
host's capabilities, atomic replace, stop of background faults whose rule
vanished, eager arming (sidecar) and cache-box drain observation happen
exactly once per delivery. Rules this host cannot execute — categories
outside its capabilities (unknown categories included), `mechanism: quota`
resource faults on the in-process host, network rules the in-process host
has nothing to run (`host=inline`, which needs the sidecar's inline toxic
host, or `host=proxy` without a `NetworkResolver`), and composition rules —
are skipped per rule, logged at WARN and counted in
`atropos_rules_unsupported_total{category}` (`category="network"` for the
network cases, `category="composition"` for compositions); the rest of the
set still applies. Malformed params of a known category reject the whole set
with 400. The push body carries no version, so the poll's rule version is
left untouched.

## SDK Bootstrap

Services embed via `atropos.Serve(ctx, atropos.Config{...})`, which registers
with manteion on startup so manteion can serve them rules and reconcile
intent on rolling deploys. Registration, polling, and intent application are
internal to `Serve` (they live in `internal/instrument`); the wire types
below are aliases of `internal/wire` and stay public because manteion
imports them.

### Types

- `RegisterRequest{ID, Service, Version, Address, PollIntervalMs, Routes, Host, Capabilities}` — the POST body. `Routes` is the optional HTTP route inventory for manteion's workflow-builder catalog; `Host` (`in-process` | `sidecar`) and `Capabilities` (the fault categories this host executes) are additive.
- `Route{Method, Path, Description, DependsOn}` — one published HTTP route. `DependsOn` entries are `"METHOD /path"` (same service) or `"service METHOD /path"` (fully qualified).
- `RegisterResponse{Status, RuleSync}` — the response embeds `RuleSync{Version, Rules, ActiveFaults, FreezeCfg}`, the same desired-state payload `GET /api/v1/sdk/rules` serves, so the SDK converges before its first poll. `Rules` and `ActiveFaults` are never omitted: `[]` clears the category, a `null`/absent `rules` means no change.
- `CompiledRule`, `CompiledFault`, `CompiledComposition`, `CompiledCompositionMember` — the JSON wire format for rules, mirroring `manteion-go/internal/ruleconv`. `CompiledComposition` is carried on the wire but not yet executable on the SDK side; the decoder skips composition rules per rule and counts them (see `/admin/rules`).

### Typical Usage

```go
h, shutdown, err := atropos.Serve(ctx, atropos.Config{
    Service: os.Getenv("SERVICE_NAME"),
    Version: version,
    Routes:  routes,
    Handler: mux,
})
if err != nil {
    log.Fatalf("atropos: %v", err)
}
defer shutdown(ctx)
http.ListenAndServe(addr, h)
```

Registration identity: `Config.InstanceID` > `MANTEION_INSTANCE_ID` >
hostname > service name — resolved once and used for register, cache-push,
and the fidelity endpoint alike.

### Limitations (current)

- The rule decoder supports all three fault categories: inline (latency, error, hang), network (latency, retransmit_delay, blackhole, drip, rst, throttle), and resource (cpu, disk, io, memory). Network `host=proxy` faults (the in-process default) need `Config.NetworkResolver`. A host executes only the categories in its capability set; a rule outside it, an unknown category included, is skipped per rule and counted (see `/admin/rules`).
- Network faults with `host=inline` run in the atroposd sidecar, which executes them as armed proxy policies through `ToxicTransport` (see below). The in-process host has no proxied connection to shape, so it skips such rules per rule (logged at WARN, counted under `atropos_rules_unsupported_total{category="network"}`) and rejects a single `/admin/fault` slot with 400; `host=proxy` without a `NetworkResolver` is handled the same way. An empty `network.host` means `inline` in the sidecar (design §5.6) and `proxy` in-process.
- Composition rules are skipped per rule on decode (logged at WARN, counted under `atropos_rules_unsupported_total{category="composition"}`) — the SDK has no composition evaluator yet (parallel/sequential fault dispatch with direction inheritance is future work).
- The unexported `decodeRuleSet` (`internal/instrument/decode.go`) sorts decoded rules by `Priority` descending (higher = evaluated first). Equal-priority rules preserve input order. Each rule gets its own outcome: decoded into `Rules`, `Unsupported` (this host cannot execute it, compositions included) or `Errors` (malformed).
- Transient (taxonomy 1.3) is implemented for HTTP ingress. Once the response is complete — the handler returned, or an inline error fault short-circuited it — the Transient point is evaluated with the ingress labels plus `atropos.http.status_code`, and a matching fault is started asynchronously on its own goroutine, so the response is never delayed by it (a failed start is only logged). A matching fault always runs in background mode through the fault registry (inline mode is meaningless after the response and is not honoured), detached from the request's cancellation but with its span parented on the request's server span. Armed `/admin/fault` slots and eager arming do not apply at Transient. The gRPC interceptors and `Hook` do not evaluate it.

## atroposd (sidecar host)

`cmd/atroposd` hosts the same core out of process (`internal/atroposd`,
`internal/proxy`). It mounts the admin endpoints above on its own control
port (`ATROPOSD_CONTROL_ADDR`, default `:16020`) and registers under
`ATROPOS_SERVICE`; explicit capture mode takes `ATROPOSD_EGRESS` as
comma-separated `listen=upstream` pairs and an optional
`ATROPOSD_INGRESS_ADDR`/`ATROPOSD_INBOUND_UPSTREAM`. Sidecar-only
behaviours: `network.host: inline` faults become armed proxy policies
(`internal/proxy.PolicyTable`), and background faults arm when their rule
applies (except Transient rules; `CoreConfig.EagerArm`).
