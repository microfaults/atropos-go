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

## Admin Handlers

The SDK ships three `http.Handler` factories for runtime control. Mount them on an internal/admin mux that is not exposed to external traffic.

### FaultAdminHandler

`FaultAdminHandler() http.Handler` exposes runtime fault injection control via a built-in `DemoEvaluator`.

| Method | Path           | Body             | Response |
|--------|----------------|------------------|----------|
| GET    | `/admin/fault` | —                | 200 `{"active": bool, "fault": {...}}` JSON |
| POST   | `/admin/fault` | fault request JSON (see below) | 201 `{"active": true, "fault": {...}}` JSON |
| DELETE | `/admin/fault` | —                | 200 `{"active": false}` JSON |

POST body fields by fault type:

| `"type"` | Required fields | Optional fields | Defaults |
|----------|-----------------|-----------------|----------|
| `"latency"` | `delay` (duration string, e.g. `"500ms"`) | `jitter` (duration string) | — |
| `"error"` | — | `status_code` (int), `message` (string) | 500, `"injected fault"` |
| `"hang"` | `duration` (duration string) | — | — |

Example mount:

```go
mux.Handle("/admin/fault", atropos.FaultAdminHandler())
```

### CacheBoxAdminHandler

`CacheBoxAdminHandler(cb *CacheBox) http.Handler` exposes runtime cache-box control.

| Method | Path                    | Body                                             | Response |
|--------|-------------------------|--------------------------------------------------|----------|
| GET    | `/admin/cachebox`       | —                                                | 200 `Stats` JSON |
| POST   | `/admin/cachebox/delay` | `{"mu": float, "sigma": float, "seed"?: uint64}` | 204 (replaces delay source with lognormal distribution) |
| DELETE | `/admin/cachebox`       | —                                                | 204 (clears store; preserves lifetime counters) |

Example mount:

```go
mux.Handle("/admin/cachebox", atropos.CacheBoxAdminHandler(cb))
mux.Handle("/admin/cachebox/", atropos.CacheBoxAdminHandler(cb))
```

### RulesAdminHandler

`RulesAdminHandler(eval *StaticEvaluator) http.Handler` exposes runtime rule-set management.

| Method | Path           | Body                | Response |
|--------|----------------|---------------------|----------|
| GET    | `/admin/rules` | —                   | 200 `[]StaticRule` JSON |
| POST   | `/admin/rules` | `[]StaticRule` JSON | 204 (atomic replace) |

Example mount:

```go
eval := atropos.NewStaticEvaluator()
atropos.Configure(atropos.WithEvaluator(eval))
mux.Handle("/admin/rules", atropos.RulesAdminHandler(eval))
```

## SDK Bootstrap

Services embedding atropos-go register with manteion on startup so manteion can serve them rules and reconcile intent on rolling deploys.

### Types

- `RegisterRequest{ID, Service, Version, Address}` — the POST body.
- `RegisterResponse{Status, Rules, ActiveFault, FreezeCfg}` — the response. Rules/ActiveFault/FreezeCfg are populated when manteion has intent tracked for the service.
- `CompiledRule`, `CompiledFault`, `CompiledComposition`, `CompiledCompositionMember` — the JSON wire format for rules, mirroring `manteion-go/internal/ruleconv`. `CompiledComposition` is carried on the wire but not yet executable on the SDK side; `DecodeCompiledRules` errors on composition rules.

### Functions

- `Register(ctx, manteionURL, req) (RegisterResponse, error)` — POSTs to `manteionURL + /api/v1/sdk/register` with a 5s default timeout.
- `Apply(resp, ApplyTargets{Evaluator, DemoEval, CacheBox}) error` — installs rules, active fault, and freeze config onto the provided SDK objects. Missing targets for populated response fields are errors.
- `DecodeCompiledRules([]CompiledRule) ([]StaticRule, error)` — lower-level helper used by Apply.

### Typical Usage

```go
eval := atropos.NewStaticEvaluator()
demo := &atropos.DemoEvaluator{}
cb := atropos.NewCacheBox(atropos.CacheBoxConfig{Store: atropos.NewCacheBoxMemStore(1024)})

atropos.Configure(atropos.WithEvaluator(eval), atropos.WithCacheBoxCoordinator(cb))

resp, err := atropos.Register(ctx, os.Getenv("ATROPOS_MANTEION_URL"), atropos.RegisterRequest{
    ID:      os.Getenv("POD_NAME"),
    Service: os.Getenv("SERVICE_NAME"),
    Address: fmt.Sprintf("http://%s:9090", os.Getenv("POD_IP")),
})
if err != nil {
    log.Fatalf("register: %v", err)
}
if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval, DemoEval: demo, CacheBox: cb}); err != nil {
    log.Fatalf("apply: %v", err)
}
```

### Limitations (current)

- `DecodeCompiledRules` supports all three fault categories: inline (latency, error, hang), network (latency, loss, blackhole, drip, rst, throttle), and resource (cpu, memory, disk, io). Network faults require a `NetworkResolver` option via `WithNetworkResolver`.
- The `disk` resource type is decoded by the SDK but is not in manteion's `validFaultTypes` — it is SDK-only and cannot be assigned via manteion rules.
- Composition rules are rejected on decode — the SDK has no composition evaluator yet.
- `DecodeCompiledRules` sorts decoded rules by `Priority` descending (higher = evaluated first). Equal-priority rules preserve input order.