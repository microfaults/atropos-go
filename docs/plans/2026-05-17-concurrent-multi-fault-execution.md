# Concurrent Multi-Fault Execution

> **Status:** Not started. Design note only. Reference from `admin.go` `DemoEvaluator.Evaluate`.

**Goal:** Allow multiple armed faults — including faults in the same category — to execute concurrently on a single SDK instance.

## Current behavior (what works, what doesn't)

`DemoEvaluator` already *stores* N slots, keyed by ID. `TestFaultAdmin_MultiSlotIDs` exercises POSTing two latency faults with IDs `f1` and `f2` and observing both in the slot map. That's a bookkeeping property.

Execution does **not** match storage:

1. **`Evaluate` returns one `Decision` per call.** See [admin.go](../../admin.go) `DemoEvaluator.Evaluate`. It walks categories in `inline > network > resource` priority and returns the first slot found. So:
   - With any inline slot armed, network/resource slots never fire.
   - Within a category, only the lex-first slot wins. The rest are dead weight.

2. **Background faults are armed lazily** via that single returned decision. See [internal/interceptor/interceptor.go](../../internal/interceptor/interceptor.go) `StartFault`. A CPU stress slot only starts when a request comes in *and* its decision is what `Evaluate` returned.

3. **The fault registry dedups by `decision.Name`.** See [internal/interceptor/registry.go](../../internal/interceptor/registry.go) `StartOrJoin`. Every admin POST hardcodes `Name: "admin"` ([admin.go](../../admin.go) `handleFaultPost`); every manteion-applied fault uses `Name: "active_fault"` ([register.go](../../register.go) `applyActiveFault`). Two CPU stresses set under the same Name dedup against each other — only one goroutine runs.

### Effective behavior today

| Scenario | What actually runs |
|---|---|
| Two admin latency slots (`f1`, `f2`) | Only `f1`'s latency. `f2` is inert. |
| Inline latency + network throttle | Latency wins per-request. Network proxy never arms. |
| Two admin CPU stress slots | One CPU goroutine (registry dedups `"admin"`). |
| Admin cpu + manteion `active_fault` cpu | Different `Name` keys, *could* run together — but `Evaluate` returns only one per call, so only one ever starts. |

The multi-slot model today is for **management** (atomic swap, individual DELETE, reconciliation with Manteion's view), not **layered execution**.

## What needs to change

Three pieces. The third is small; the first two are real design moves.

### 1. `Evaluate` returns multiple decisions (or arms eagerly)

The `Evaluator` interface returns `*Decision` (singular). Two paths to support concurrency:

**Option A — Composite decision.** `Evaluate` returns one `*Decision` that wraps multiple faults; interceptor unwraps and starts each. Keeps the interface stable but pushes complexity into `Decision` and the interceptor.

**Option B — Multi-decision interface.** Add a sibling method `EvaluateAll(ctx, req) []*Decision` that defaults to wrapping `Evaluate` for backward compat. Interceptor prefers it when present. Cleaner separation.

**Option C — Eager arming for background faults.** Background slots (network/resource) start their goroutine on `Set`/`Apply`, not on first request. Inline slots stay lazy (they're per-request by nature). This sidesteps `Evaluate` entirely for background work and matches the current intent of the registry-based dedup. Probably the simplest path.

Sketch for Option C:

```go
// In DemoEvaluator.Set, when slot.req.effectiveCategory() != "inline":
//   pre-arm via a startFn the evaluator was given at construction time.
// In ClearSlot, when removing a background slot:
//   stop the running fault (need a handle in faultSlot).
type faultSlot struct {
    decision        *Decision
    req             *FaultRequest
    lastConfirmedAt time.Time
    handle          *fault.Handle // set for background faults that were eagerly armed
}
```

### 2. Per-slot fault lifecycle

Today, clearing a slot drops the decision but does **not** stop a background goroutine that was started from it. The registry only releases the entry when the fault's own `Result` callback fires (i.e., the fault completes by itself). So a CPU stress armed with `duration_ms=0` keeps running after `ClearSlot` until process exit.

Fix: tie the goroutine's lifetime to the slot. Store the `*fault.Handle` on `faultSlot`, and call `handle.Stop()` (or cancel its ctx) in `ClearSlot`.

This is also the right place to add `expiresAt` so the watchdog can reap based on declared `duration_ms` lifecycle, not just `lastConfirmedAt` heartbeat staleness.

### 3. Registry key includes slot ID

Today, `key := decision.Name` ([interceptor.go](../../internal/interceptor/interceptor.go) `startRegistered`). Change to include the slot ID so two slots producing decisions with the same `Name` register separately:

```go
key := decision.Name + ":" + decision.SlotID
```

`Decision` would gain a `SlotID string` field. `Apply` and `handleFaultPost` populate it from the slot ID. Existing rule-based decisions leave it empty, so they keep deduping by `Name` alone.

## Boundaries — what stays single-fault

Inline faults are inherently single-decision-per-request — a latency *and* an error on the same request would need composition semantics that don't exist today (which fault's status? whose body?). Even with the changes above, per-request behavior should pick one inline decision via priority/labels; the multi-fault story is really about **independent background faults running in parallel** plus **inline picked deterministically when multiple inline slots match different routes**.

## Risk / scope notes

- Eager arming on `Set` changes failure semantics: today, a bad fault config only errors when the first request triggers it; with eager arming, errors surface immediately on POST/Apply. Probably an improvement, but a behavior change.
- `Confirm` becomes more important: with per-slot lifetimes, the watchdog needs slot-specific heartbeats to avoid reaping slots that are correctly running but quiet.
- The admin/manteion reaping contract documented in [admin.go](../../admin.go) `FaultAdminHandlerWith` interacts with all of this. Revisit whether admin slots should be excluded from manteion reconciliation when this lands.
