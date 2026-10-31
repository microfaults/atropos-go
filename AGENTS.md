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

Slot lifetime when connected to manteion: the next desired-state `200` whose
`active_faults` list does not name a slot drops it (a `304` applies nothing), and the
fault watchdog drops one that stays unconfirmed for `max(3·max(poll interval,
long-poll hold), 30 s)` — 90 s with the defaults (10 s interval, 30 s hold).
`POST /admin/fault` never confirms a slot, so an admin fault is a short-lived
override while connected; offline, slots persist until deleted.

### /admin/cachebox

Runtime cache-box control.

| Method | Path                    | Body                                                                          | Response |
|--------|-------------------------|-------------------------------------------------------------------------------|----------|
| GET    | `/admin/cachebox`       | —                                                                             | 200 `Stats` JSON: store, recorder and record-buffer counters plus `Delay` (`Installed`, `Mu`, `Sigma`, and the `ExperimentID`/`PhaseID` the delay source is scoped to; both empty = every replay) |
| POST   | `/admin/cachebox/delay` | `{"mu": float, "sigma": float, "seed"?: uint64, "context"?: CacheBoxContext}` | 204 (replaces the delay source with a lognormal distribution; `context` is honoured: it scopes the source to its `(experiment_id, phase_id)`, and a replay under any other pair keeps sleeping the entry's observed latency); 400 on malformed JSON, a negative `mu`/`sigma`, or a `context` naming only one of the pair |
| DELETE | `/admin/cachebox`       | —                                                                             | 204 (clears the record-side store and the installed replay set; preserves lifetime counters) |

A delay source installed by `POST /admin/cachebox/delay` holds until the next
desired-state `200` that carries no `freeze_cfg` clears it (that document means
"no delay source desired"; a registration answer without `freeze_cfg` does the
same, a `304` leaves it). `freeze_cfg` in the desired state goes through the
same validation and installs the same scoped source; one that fails it installs
nothing and is reported under `errors`, and one naming the source already
installed (the same `mu`, `sigma`, `seed` and scope) keeps it, so its sample
sequence is not restarted by every document of a frozen phase.

### /admin/rules

Runtime rule-set management against the host evaluator.

| Method | Path           | Body                                            | Response |
|--------|----------------|-------------------------------------------------|----------|
| GET    | `/admin/rules` | —                                               | 200 `[]StaticRule` JSON (the installed, decoded rules) |
| POST   | `/admin/rules` | `[]CompiledRule` JSON; `null` clears the set    | 204 (atomic replace); 400 `{"error": ...}` (malformed JSON); 422 `{"error": "rule a: decode: ...; rule b: decode: ..."}` (some rules did not decode and were skipped: the pushed set less those rules replaced the installed one, an empty set when none decoded); 500 `{"error": ...}` (this host cannot hold rules; nothing was applied) |

POST is the push side of the single apply path the manteion sync uses (`apply`
in `internal/instrument/register.go`, here scoped to rules): decode against this
host's capabilities, atomic replace, stop of background faults whose rule
vanished, eager arming (sidecar) and cache-box drain observation happen
exactly once per delivery, serialized with the sync's own applies. Rules this
host cannot execute — categories outside its capabilities (unknown categories
included), `mechanism: quota` resource faults on the in-process host, network
rules the in-process host has nothing to run (`host=inline`, which needs the
sidecar's inline toxic host, or `host=proxy` without a `NetworkResolver`), and
composition rules — are skipped per rule, logged at WARN and counted in
`atropos_rules_unsupported_total{category}` (`category="network"` for the
network cases, `category="composition"` for compositions); the rest of the set
still applies and the answer is still 204. (The manteion sync also lists them
under `unsupported` in its status report; the push sends no report.) A rule
that does not decode — malformed params of a known category, an unknown fault
type — is skipped and logged, not counted; the push then answers 422, with its
`rule <name>: decode: ...` message joined to the others with `; ` (the sync
reports it under `errors` instead), and the rules that did decode already
replaced the installed set, the skipped ones removed with it: a push in which
no rule decodes replaces the installed set with an empty one. The push touches
only rules: admin fault slots, the delay source and the installed replay set
stay as they are, and the push carries no version, so the poll's generation is
left untouched.

Connected to manteion, a pushed set is a short-lived override, like an admin
fault slot: a push that applied (204 or 422) clears the ETag the instance
holds, so the next poll is unconditional and its `200` re-applies manteion's
desired state over the pushed set (a poll already held during the push comes
back as a late `304` and adopts nothing). A pushed set therefore lasts until
the next poll completes: at most one long-poll hold (30 s) plus the apply. The
push sends no status report. Offline, a pushed set holds until the next push.

## SDK Bootstrap

Services embed via `atropos.Serve(ctx, atropos.Config{...})`, which registers
with manteion on startup and then keeps the instance on manteion's desired
state, so manteion can reconcile intent on rolling deploys. Registration, the
sync loop, the status report and the replay-set pull are internal to `Serve`
(they live in `internal/instrument`); the wire types below are aliases of
`internal/wire` and stay public because manteion imports them. The contract is
`docs/plans/2026-10-30-desired-state-protocol.md`:

- **Register.** `POST /api/v1/sdk/register` answers `201` with the materialized
  `RuleSync` (plus `status`), applied before the first poll.
- **Poll.** `GET /api/v1/sdk/rules?service=&instance_id=&version=<generation held>&wait=30`
  with `If-None-Match: "<etag held>"` (sent only when an ETag is held). A `200`
  carries a `RuleSync`: it goes through the single `apply` path, a `StatusReport`
  goes to manteion, and the instance polls again at once. A `304` means the
  document is unchanged: the instance adopts the generation in
  `X-Atropos-Generation` and applies nothing. The ETag is the content hash alone
  and opaque to the SDK; the generation rides `X-Atropos-Generation` and
  `version`. A failed poll keeps the stale state and backs off; the SSE
  `rules_changed` nudge still ends a wait early.
- **Status.** `POST /api/v1/sdk/status` with a `StatusReport` after every apply
  and whenever the replay-set state changes. A report that fails to send is
  re-sent before the next poll and by its own retry (2 s, doubling up to the
  poll wait).
- **Registration lost.** A status report or a poll answered `404` means manteion
  no longer knows the instance (a database reset, an instance-id collision):
  the instance drops the generation and ETag it holds and registers again,
  applying the answer, then resumes polling. Once per loss: until a report
  lands again, a further `404` does not register again (the report stays
  pending for its retry; a poll counts as failed).
- **Replay set.** A `replay_set` whose `id` differs from the committed one is
  pulled by the instance itself (`GET /api/v1/cache/artifacts/{id}` in `Range`
  chunks of at most 4 MiB, staged through the preload path, committed when the
  entry count and the W5 digest match) and its state is reported
  (`fetching|committed|failed`); an absent `replay_set` clears the installed one.
  A chunk whose request cannot be built or sent, or is answered 5xx, 408 or 429,
  is retried from its offset (five requests per chunk at most). A failed pull is
  pulled again on the instance's own timer while the set stays desired (5 s,
  doubling up to 60 s, each wait ±20 %), reported `failed` → `fetching` → ...;
  a document naming another set or none drops the retry, one still naming it
  leaves it on its schedule. A set no pull can fetch (no pair scopes it, an
  unsupported key strategy, no artifact `url`) is not retried.
- **Ingest `epoch`.** Cache-push batches (`POST /api/v1/cache/ingest`) carry the
  instance's start time in unix nanoseconds as `epoch`, extending manteion's
  dedupe key across restarts. `batch_seq` is monotonic per epoch and never
  restarts when the batch's `(experiment, phase)` pair changes, so
  `(experiment, phase, instance, epoch, batch_seq)` names one batch.

### Types

- `RegisterRequest{ID, Service, Version, Address, PollIntervalMs, Routes, Host, Capabilities}` — the POST body. `Routes` is the optional HTTP route inventory for manteion's workflow-builder catalog; `Host` (`in-process` | `sidecar`) and `Capabilities` (the fault categories this host executes) are additive.
- `Route{Method, Path, Description, DependsOn}` — one published HTTP route. `DependsOn` entries are `"METHOD /path"` (same service) or `"service METHOD /path"` (fully qualified).
- `RegisterResponse{Status, RuleSync}` — the response embeds the `RuleSync` below, the same desired-state payload `GET /api/v1/sdk/rules` serves, so the SDK converges before its first poll.
- `RuleSync{Version, Etag, Rules, ActiveFaults, FreezeCfg, ReplaySet}` — the desired state (JSON `version`, `etag`, `rules`, `active_faults`, `freeze_cfg`, `replay_set`). `Version` is the generation. `Etag` is the document's ETag (omitted when empty); the SDK sends it back as `If-None-Match` and treats it as opaque. `Rules` and `ActiveFaults` are never omitted: `[]` clears the category, a `null`/absent list means no change. `FreezeCfg` (a `DelayRequest`) and `ReplaySet` are omitted when none is desired: absent clears the delay source / the installed replay set.
- `ReplaySetRef{ID, ExperimentID, PhaseID, KeyStrategy, Count, SHA256, SizeBytes, URL}` — the replay set to pull. `experiment_id` and `phase_id` name the pair the set is installed under (the cache-box serves it only to the rule whose context matches that pair); `id` is `sha256:<hex>` of the artifact bytes, `sha256` the W5 digest over its entries, `count` the entry count, `url` relative to the manteion base URL. A set with no pair (not on the ref, not on a replay rule's context, not on `freeze_cfg.context`) is reported `failed`.
- `StatusReport{InstanceID, Service, AppliedGeneration, Etag, Unsupported, Errors, ReplaySet}` — the body of `POST /api/v1/sdk/status`, a full snapshot of what the instance holds, sent after every apply and whenever the replay-set state changes. `Unsupported` and `Errors` replace the previous report (an empty list clears it, so neither is omitted: `[]`, never `null`), and `ReplaySet` names the desired replay set and its pull state (`fetching` and `failed` name a set not yet held; an earlier committed set may still be installed); absent means none is desired. A replay set that failed to pull adds its reason to `Errors`.
- `UnsupportedRule{Rule, Category}` — one rule (or active-fault slot id) the instance skipped, with the category it could not execute (a fault category, or `composition`).
- `ReplaySetStatus{ID, State}` — the desired replay set and its pull state: `fetching`, `committed` or `failed`. Only `committed` means the instance holds it; under `fetching` or `failed` an earlier committed set may still be installed.
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
- Resource faults with `mechanism: quota` need the node agent's cgroup authority. The in-process host has none, so it skips such rules per rule (logged at WARN, counted under `atropos_rules_unsupported_total{category="resource"}`) and rejects a single `/admin/fault` slot with 400; the sidecar delegates resource faults to the node agent when its socket answers at start.
- Composition rules are skipped per rule on decode (logged at WARN, counted under `atropos_rules_unsupported_total{category="composition"}` and listed under `unsupported` in the status report) — the SDK has no composition evaluator yet (parallel/sequential fault dispatch with direction inheritance is future work).
- The unexported `decodeRuleSet` (`internal/instrument/decode.go`) sorts decoded rules by `Priority` descending (higher = evaluated first). Equal-priority rules preserve input order. Each rule gets its own outcome: decoded into `Rules`, `Unsupported` (this host cannot execute it, compositions included) or `Errors` (malformed).
- `/admin/rules` pushes are short-lived while connected: the next poll, at most one long-poll hold later, re-applies manteion's desired state over a pushed set, and a push sends no status report. A push in which no rule decodes replaces the installed set with an empty one (skipped rules removed) and answers 422.
- A desired-state body with neither an `etag` nor `rules` is manteion's degraded no-document register answer (it could not materialize the desired state: `version: 0`, `rules: null`, `active_faults: null`, no `freeze_cfg`, no `replay_set`). The instance applies nothing for it — it keeps its rules, active-fault slots, delay source and replay set — re-confirms the slots it holds, repeats its status report and keeps polling.
- Transient (taxonomy 1.3) is implemented for HTTP ingress. Once the response is complete — the handler returned, or an inline error fault short-circuited it — the Transient point is evaluated with the ingress labels plus `atropos.http.status_code`, and a matching fault is started asynchronously on its own goroutine, so the response is never delayed by it (a failed start is only logged). A matching fault always runs in background mode through the fault registry (inline mode is meaningless after the response and is not honoured), detached from the request's cancellation but with its span parented on the request's server span. Armed `/admin/fault` slots and eager arming do not apply at Transient. The gRPC interceptors and `Hook` do not evaluate it.

## atroposd (sidecar host)

`cmd/atroposd` hosts the same core out of process (`internal/atroposd`,
`internal/proxy`). It mounts the admin endpoints above on its own control
port (`ATROPOSD_CONTROL_ADDR`, default `:16020`) and registers under
`ATROPOS_SERVICE`; explicit capture mode takes `ATROPOSD_EGRESS` as
comma-separated `listen=upstream` pairs and an optional
`ATROPOSD_INGRESS_ADDR`/`ATROPOSD_INBOUND_UPSTREAM`. It runs the same
desired-state sync as `Serve` (long-poll, status report, replay-set pull),
because both connect the same core through `ConnectManteion`. Sidecar-only
behaviours: `network.host: inline` faults become armed proxy policies
(`internal/proxy.PolicyTable`), and background faults arm when their rule
applies (except Transient rules; `CoreConfig.EagerArm`).
