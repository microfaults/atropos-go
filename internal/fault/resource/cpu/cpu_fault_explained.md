# CPU Fault & Fault Interface — Complete Code Walkthrough

The CPU stress fault is part of the **Atropos** fault-injection SDK. It synthetically consumes a configurable fraction of CPU capacity using duty-cycle spinning, with support for ramp-up/ramp-down phases and container-aware CPU detection.

---

## Architecture & Type Hierarchy

```
┌────────────────────────────────┐
│       fault.Fault (interface)  │  ← All faults implement this
│       Validate() error         │
│       Start(ctx) (*Handle, err)│
└──────────────┬─────────────────┘
               │
               │  embeds
               ▼
┌──────────────────────────────┐
│     fault.FaultConfig        │  ← Duration, RampUp, RampDown
└──────────────┬───────────────┘
               │
               │  embeds
               ▼
┌──────────────────────────────┐
│   resource.Config            │  ← + TargetLoad, Window (duty-cycle)
└──────────────┬───────────────┘
               │
               │  embeds
               ▼
┌──────────────────────────────────────────┐
│        cpu.Stress                        │  ← The actual CPU fault
│  + emit fault.EventEmitter              │
│  Validate(), Start(ctx), SetEventEmitter │
└──────────────────────────────────────────┘
```

**Key interfaces implemented:**
- `fault.Fault` — lifecycle ([Validate](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#48-52) + [Start](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20))
- `fault.EventAware` — optional OTel span event emission

---

## File Overview

| File | Lines | Purpose |
|---|---|---|
| [internal/fault/fault.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go) | 90 | Core [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) interface, [Handle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#47-52), [Result](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#85-90) |
| [internal/fault/event.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/event.go) | 13 | [EventEmitter](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#36-40) and [EventAware](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/event.go#10-13) interfaces |
| [internal/fault/resource/config.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/config.go) | 53 | Shared config for resource-pressure faults |
| [internal/fault/resource/cpu/stress.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go) | 234 | CPU stress implementation (duty-cycle) |
| [internal/fault/resource/cpu/detect.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go) | 76 | Container-aware CPU count detection |
| [internal/fault/resource/cpu/stress_test.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go) | 179 | Test suite |
| [internal/trace/trace.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go) | 86 | OTel span/tracer abstraction |
| [types.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/types.go) | 74 | Public re-exports of internal types |
| [faults.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/faults.go) | 46 | Public factory functions |

---

## Section 1 — The [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) Interface ([internal/fault/fault.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go))

```go
type Fault interface {
    Validate() error
    Start(ctx context.Context) (*Handle, error)
}
```

### What it does
Defines the **universal contract** for all fault types in Atropos. Every fault — CPU, memory, disk, latency, error — implements exactly two methods:

1. **[Validate()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#48-52)** — checks configuration before execution (fail-fast)
2. **[Start(ctx)](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20)** — begins the fault **asynchronously** and returns a [Handle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#47-52) for control

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Two-method interface** | Minimal surface area; easy to implement new fault types | 1. Single `Execute(ctx) (Result, error)` — blocking<br>2. [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) + `Configurable` + `Stoppable` — fine-grained interfaces | The two-method split forces a clean validate-then-execute pattern. A single blocking method would prevent early stop. Multiple interfaces add complexity for little benefit at this scale. |
| **[Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20) returns `*Handle`, not [Result](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#85-90)** | Non-blocking: faults run in the background (goroutines), caller can do other work or stop early | 1. Blocking `Run()` — simpler but ties up the caller<br>2. Return a channel directly | [Handle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#47-52) is a richer abstraction than a raw channel — it provides [Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75), [Done()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#66-70), and callbacks. Blocking would prevent running faults alongside request serving. |
| **`context.Context` parameter** | Allows parent cancellation (e.g., server shutdown, request timeout) to propagate | 1. No context, use `Handle.Stop()` only | Context is idiomatic Go for cancellation. Without it, the interceptor layer can't cascade its shutdown to running faults. |

---

## Section 2 — [Handle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#47-52) & [Result](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#85-90) ([internal/fault/fault.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go))

```go
type Handle struct {
    done     chan Result
    cancel   context.CancelFunc
    onResult func(Result)
}

type Result struct {
    ActualDuration time.Duration
    Err            error
    Detail         any    // fault-specific diagnostics
}
```

### What it does
[Handle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#47-52) is the **control plane** for a running fault:
- **[Done()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#66-70)** — returns a receive-only channel; blocks until the fault completes
- **[Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75)** — triggers early cancellation (non-blocking)
- **[Send(result)](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#76-83)** — called by the fault implementation to deliver the outcome
- **[SetOnResult(fn)](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#61-65)** — hook for the interceptor to record telemetry before the result is consumed

[Result](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#85-90) is the **outcome report**:
- `ActualDuration` — how long the fault actually ran (may be less than configured if stopped early)
- [Err](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#27-28) — non-nil if cancelled or failed
- [Detail](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#30-35) — type `any`, allows each fault to attach diagnostics (e.g., `cpu.Detail` with worker count)

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Buffered channel (`make(chan Result, 1)`)** | [Send()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#76-83) never blocks even if nobody is listening yet | 1. Unbuffered — blocks sender until receiver reads<br>2. `sync.Once` + field | Buffer of 1 is perfect for single-result delivery. Unbuffered would deadlock if [Send()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#76-83) is called before anyone calls [Done()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#66-70). |
| **`onResult` callback** | Lets the interceptor record OTel span results synchronously before the consumer reads [Done()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#66-70) | 1. Middleware wrapping around the result channel<br>2. Observer pattern with multiple listeners | A single callback is simple and guarantees telemetry is recorded even if the consumer doesn't check [Done()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#66-70). |
| **`Detail any` (not generics)** | Keeps the [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) interface non-generic; any fault can attach its own diagnostics | 1. [Detail](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#30-35) as a typed interface with methods<br>2. Generic `Result[T]` | `any` requires type assertion by the consumer but avoids making [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) generic (which would complicate the registry and interceptor). A typed `FaultDetail` interface would be cleaner but adds boilerplate to every fault type. |
| **[Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75) calls `cancel()` (context cancellation)** | Single mechanism for both voluntary stop and timeout — all goroutines check `ctx.Done()` | 1. Separate `stopCh` channel<br>2. Atomic flag | Using context is unified: [Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75), parent cancellation, and `context.WithTimeout` all flow through the same `ctx.Done()` channel. No duplicate signal paths. |

---

## Section 3 — [EventEmitter](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#36-40) & [EventAware](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/event.go#10-13) ([internal/fault/event.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/event.go))

```go
type EventEmitter func(name string, attrs ...attribute.KeyValue)

type EventAware interface {
    SetEventEmitter(fn EventEmitter)
}
```

### What it does
An **optional** interface for faults that want to emit timestamped events on the OTel span (e.g., "ramp-up started", "sustain phase", "ramp-down complete"). The interceptor checks if a fault implements [EventAware](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/event.go#10-13) and injects the emitter before calling [Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20).

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Optional interface (not part of [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45))** | Not all faults need events (latency, error faults are instant) | 1. Add [SetEventEmitter](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#36-40) to the [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) interface — every fault must implement it<br>2. Pass emitter as parameter in [Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20) | Optional interface avoids burdening simple faults. Adding it to [Fault](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#41-45) would force no-op implementations everywhere. Passing in [Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20) would change the signature for all faults. |
| **Function type, not interface** | [EventEmitter](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#36-40) is a single function — an interface with one method is unnecessary | 1. `EventWriter` interface with [Emit()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#36-40) | A function type is more flexible (closures capture span context directly). |
| **Setter injection, not constructor injection** | The interceptor creates the fault first (from config), then optionally wires up the emitter | 1. Pass emitter in a constructor/builder | Setter injection allows the fault to be constructed without knowing about tracing. This keeps the fault package decoupled from the trace package. |

---

## Section 4 — Resource Config ([internal/fault/resource/config.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/config.go))

```go
type Config struct {
    fault.FaultConfig              // Duration, RampUp, RampDown

    TargetLoad float64             // fraction of resource (0.0, 1.0]
    Window     time.Duration       // duty-cycle period (default 100ms)
}
```

### What it does
A shared configuration base for **all resource-pressure faults** (CPU, memory, disk I/O). Adds two fields to the base [FaultConfig](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#10-15):

1. **`TargetLoad`** — what fraction of the resource to consume (e.g., 0.5 = 50% CPU)
2. **[Window](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/config.go#46-53)** — the duty-cycle period (how often to alternate between burn and sleep)

### The Duty-Cycle Concept

```
│←────── Window (100ms) ──────→│
│                               │
│  ┌──── burn ────┐             │
│  │  load × win  │   sleep     │
│  │  (e.g. 30ms) │  (70ms)     │
│  └──────────────┘             │
```

Within each [Window](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/config.go#46-53), the fault burns CPU for `TargetLoad × Window` and sleeps the rest. This produces a **time-averaged** CPU load matching the target.

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Embedding [FaultConfig](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#10-15)** | Inherits `Duration`, [RampUp](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#159-176), `RampDown` validation without duplication | 1. Flat struct with all fields<br>2. Composition with explicit field | Embedding is idiomatic Go struct composition. The downside is that `Config.Validate()` must chain to `FaultConfig.Validate()` — easy to forget. |
| **`TargetLoad` as float64 (0,1]** | Normalized: 0.3 means 30% regardless of core count | 1. Absolute core count (e.g., 2.5 cores)<br>2. Percentage integer (30) | Normalized is more portable (works on 4-core dev laptop and 64-core server). Absolute core count would be more predictable but requires the caller to know how many cores are available. |
| **100ms default window** | Balances granularity and overhead; 10 cycles/sec is smooth enough for most workloads | 1. Smaller (10ms): more precise but higher context-switch overhead<br>2. Larger (1s): less overhead but bursty, produces noticeable spikes | 100ms is a well-known sweet spot from the iBench / stress-ng literature. Application-level monitoring at 1s granularity sees a smooth load. |
| **iBench SoI model inspiration** | Academic foundation for reproducible interference experiments | 1. Ad-hoc "just spin" approach<br>2. Full iBench implementation with µarch-level stressors | iBench's system-level SoI model (not microarchitectural) is the right abstraction for chaos engineering. Microarchitectural stressors (cache thrashing, TLB stress) are too low-level for service-level fault testing. |

---

## Section 5 — CPU Stress Implementation ([internal/fault/resource/cpu/stress.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go))

This is the core of the CPU fault. Let's break it down function by function.

### 5a — Struct Definition

```go
type Stress struct {
    resource.Config             // Duration, RampUp, RampDown, TargetLoad, Window
    emit fault.EventEmitter     // optional OTel event hook
}
```

Embeds `resource.Config` (which embeds `fault.FaultConfig`). Three levels of embedding give clean field access: `s.Duration`, `s.TargetLoad`, `s.Window`.

### 5b — [Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20) — The Entry Point (lines 61-133)

```go
func (s *Stress) Start(ctx context.Context) (*fault.Handle, error) {
    // 1. Validate config
    // 2. Detect available CPUs (cgroup-aware)
    // 3. Calculate workers and per-worker load
    // 4. Create timeout context + Handle
    // 5. Launch worker goroutines (pinned to OS threads)
    // 6. Wait for completion, send Result
}
```

**Step-by-step:**

| Step | Code | What happens |
|---|---|---|
| Validate | `s.Validate()` | Checks duration > 0, load ∈ (0,1], ramp constraints |
| Detect CPUs | [AvailableCPUs()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go#11-29) | cgroup v2 → v1 → `runtime.NumCPU()` fallback |
| Calc workers | `ceil(TargetLoad × available)` | e.g., 0.3 load on 4 CPUs → 1.2 → 2 workers |
| Cap workers | `min(workers, NumCPU())` | Never exceed physical cores |
| Per-worker load | `totalUnits / workers` | Each worker burns this fraction of its time |
| Timeout context | `context.WithTimeout(ctx, Duration)` | Hard deadline regardless of ramp schedule |
| Phase events goroutine | [emitPhaseEvents(...)](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#135-177) | Emits OTel events at ramp transitions |
| Worker goroutines | `runtime.LockOSThread()` + [dutyCycle(...)](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#178-228) | One goroutine per worker, pinned to OS thread |
| Result delivery | `handle.Send(result)` | After all workers complete, reports diagnostics |

### 5c — Worker Calculation Logic

```
Given: TargetLoad = 0.3, AvailableCPUs = 4.0

totalUnits = 0.3 × 4.0 = 1.2    (we need to consume 1.2 CPU-cores worth)
workers    = ceil(1.2) = 2       (two goroutines)
perWorker  = 1.2 / 2 = 0.6      (each burns 60% of its time)

Result: 2 workers × 60% duty = 1.2 cores consumed = 30% of 4 cores ✓
```

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Non-blocking [Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20)** | Returns `*Handle` immediately; workers run in background | 1. Blocking `Run()` — simpler<br>2. Return channels/futures | Non-blocking lets the interceptor kick off the fault and continue handling the request. The [Handle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#47-52) allows early cancellation. |
| **`runtime.LockOSThread()` per worker** | Pins the goroutine to a real OS thread — ensures the CPU burn actually hits a physical core | 1. Regular goroutines (Go scheduler may multiplex onto fewer threads)<br>2. Use OS-level CPU affinity (`sched_setaffinity`) | `LockOSThread` is the simplest way to guarantee thread-level work. Without it, the Go scheduler could park a "burning" goroutine, reducing actual CPU consumption. `sched_setaffinity` would be more precise but is platform-specific. |
| **`context.WithTimeout` as the hard deadline** | Guarantees the fault ends even if duty-cycle timing drifts | 1. Manual deadline tracking in each worker | Context timeout is the Go-standard pattern and composes with parent cancellation. If the parent context is cancelled, all workers stop immediately via `ctx.Done()`. |
| **Workers = `ceil(totalUnits)`** | Ensures enough goroutines to reach the target load | 1. [round()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go#73-76) or `floor()` — might undercount<br>2. One goroutine per CPU — simpler distribution | Ceiling ensures we always have enough workers. The per-worker load is then adjusted downward so the total matches exactly. One-per-CPU would waste resources when load is low (e.g., 10% on 8 cores → 8 goroutines each at 1.25%). |

---

## Section 6 — Duty-Cycle Loop ([dutyCycle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#178-228), lines 178-227)

```go
func dutyCycle(ctx context.Context, targetLoad float64, 
    totalDuration, rampUp, rampDown, window time.Duration, 
    globalStart time.Time) {
    
    for {
        // Check context
        // Compute effective load based on ramp phase
        // Burn for (load × window)
        // Sleep for (window - burnTime)
    }
}
```

### The Three-Phase Timeline

```
│←── rampUp ──→│←── sustain ──→│←── rampDown ──→│
│               │               │                 │
│   0 → target  │   target      │   target → 0    │
│               │               │                 │
│←───────────── Duration ──────────────────────→│
```

**Load calculation per phase:**
- **Ramp-up:** `load = target × (elapsed / rampUp)` — linear 0 → target
- **Sustain:** `load = target` — constant
- **Ramp-down:** `load = target × (1 - progress)` — linear target → 0

**Per window:**
```
burnTime  = window × load     (e.g., 100ms × 0.3 = 30ms)
sleepTime = window - burnTime (e.g., 100ms - 30ms = 70ms)
```

### The Burn Phase — "doing nothing, loudly"

```go
burnDeadline := time.Now().Add(burnTime)
for time.Now().Before(burnDeadline) {
    select {
    case <-ctx.Done():
        return
    default:
    }
}
```

This is a **hot spin loop** — it does nothing useful but consumes CPU. The `select` with `default` checks for cancellation on each iteration without blocking.

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Spin loop (not math-heavy workload)** | Pure CPU consumption without side effects (no memory, no I/O, no cache thrashing) | 1. Math-heavy loop (Fibonacci, prime sieve) — stresses ALU specifically<br>2. Memory-heavy loop — stresses cache hierarchy<br>3. Use `runtime.GOMAXPROCS` manipulation | Spin loop is the cleanest system-level SoI — it consumes exactly one CPU thread's time. Math-heavy would add microarchitectural bias. `GOMAXPROCS` manipulation is fragile and affects the entire runtime. |
| **Context check inside burn loop** | Ensures sub-millisecond responsiveness to cancellation even during burn | 1. Only check between windows (up to 100ms lag)<br>2. Use a timer instead of polling | Polling in the burn loop adds negligible overhead (the loop body is already just `time.Now()` calls). Without it, [Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75) could take up to one full window to take effect. |
| **Linear ramp (not exponential/sigmoid)** | Simple, predictable, easy to reason about in monitoring dashboards | 1. Exponential ramp — smoother transition<br>2. Sigmoid — avoids sharp corners at start/end<br>3. Step function — instant change | Linear is the simplest to implement and verify. Exponential/sigmoid would look better on a graph but are harder to validate in tests. For chaos engineering, the ramp shape matters less than the target load accuracy. |
| **`time.NewTimer` for sleep (not `time.Sleep`)** | Can be cancelled via `select` on `ctx.Done()` | 1. `time.Sleep()` — blocks unconditionally | `time.Sleep` is not cancellable — the worker would be stuck sleeping even after [Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75) is called. `time.NewTimer` + select is the Go pattern for cancellable sleeps. |
| **Shared `globalStart` time** | All workers compute ramp phase relative to the same start instant — ensures synchronized load | 1. Per-worker start time — drift between workers | Shared start time guarantees all workers transition between phases simultaneously. Per-worker would cause staggered ramps. |

---

## Section 7 — Container CPU Detection ([internal/fault/resource/cpu/detect.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go))

```go
func AvailableCPUs() float64 {
    if cpus := detectCgroupV2(); cpus > 0 { return cpus }
    if cpus := detectCgroupV1(); cpus > 0 { return cpus }
    return float64(runtime.NumCPU())
}
```

### Detection Chain

| Priority | Source | File | Example |
|---|---|---|---|
| 1st | cgroup v2 | `/sys/fs/cgroup/cpu.max` | `150000 100000` → 1.5 CPUs |
| 2nd | cgroup v1 | `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` + `cpu.cfs_period_us` | quota=200000 / period=100000 → 2.0 CPUs |
| 3rd | Fallback | `runtime.NumCPU()` | 8 (on bare metal) |

### Why This Matters

```
Container with --cpus=2 running on 8-core host:

runtime.NumCPU() → 8          ← WRONG (sees host cores)
AvailableCPUs()  → 2.0        ← CORRECT (reads cgroup limit)

If we used NumCPU() with TargetLoad=0.5:
  totalUnits = 0.5 × 8 = 4 CPUs consumed
  Container limit = 2 CPUs → THROTTLED, 100% utilization, not 50%!

With AvailableCPUs():
  totalUnits = 0.5 × 2 = 1 CPU consumed → actual 50% ✓
```

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **cgroup v2 first, v1 fallback** | Modern kernels (5.x+) use cgroup v2; v1 is legacy but still common | 1. Only support v2<br>2. Use a library like `containerd/cgroups` | Priority chain covers both without dependencies. A library would handle edge cases (mixed hierarchies) but adds a dependency for ~50 lines of code. |
| **Returns `float64`** | Container limits can be fractional (`--cpus=1.5`) | 1. Return `int` (truncate/round) | Float preserves precision. Returning `int` would lose the fractional core count, making load calculation inaccurate for fractional limits. |
| **[roundCPU](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go#73-76) to 2 decimal places** | Avoids floating-point noise (e.g., 1.9999999 → 2.00) | 1. No rounding — accept noise<br>2. Round to integer | Two decimal places matches the precision of cgroup limits (`--cpus=1.50`). No rounding would cause surprising worker counts. |
| **Silent fallback (no logging)** | File-not-found on `/sys/fs/cgroup/...` is expected on bare metal — not an error | 1. Log at debug level<br>2. Return error | Silent fallback is correct: the absence of cgroup files is the normal case on non-containerized systems. Logging would spam on dev machines. |

---

## Section 8 — Phase Event Emission ([emitPhaseEvents](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#135-177), lines 136-176)

```go
func (s *Stress) emitPhaseEvents(ctx context.Context, globalStart time.Time) {
    // Emit "ramp-up started" → wait → "ramp-up complete"
    // Emit "sustain started"
    // Wait for sustain phase → emit "ramp-down started" → wait → "ramp-down complete"
}
```

### Events Timeline

```
Timeline:  ├── rampUp ──┤──── sustain ────┤── rampDown ──┤
Events:    ↑            ↑                 ↑              ↑
           ramp_up      ramp_up           ramp_down      ramp_down
           _start       _complete         _start         _complete
                        ↑
                        sustain_start
```

These events appear as **timestamped annotations** on the OTel span in Grafana/Tempo, allowing you to correlate CPU load changes with application behavior.

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Separate goroutine for events** | Workers are pinned to OS threads (`LockOSThread`) — can't emit span events from them | 1. Emit events from within [dutyCycle](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#178-228) — but workers are locked to threads | The event goroutine runs on the Go scheduler (not pinned) and has access to the span context. Workers must stay on their pinned threads for accurate CPU burning. |
| **`time.After` + `select` for phase transitions** | Cancellable waiting — if the fault is stopped mid-ramp, events stop cleanly | 1. `time.Sleep()` — not cancellable | Same pattern as the duty-cycle sleep: cancellable via context. |
| **Events only emitted if `emit != nil` and ramp phases exist** | No-op if tracing is disabled or there are no ramp phases | 1. Always emit, let the nil emitter panic | The nil check (`if s.emit != nil`) is the standard Go guard pattern. Avoids panics if the fault runs without the interceptor wiring up tracing. |

---

## Section 9 — OTel Trace Integration ([internal/trace/trace.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go))

```go
type Tracer interface {
    Start(ctx, name, attrs...) (context.Context, Span)
}

type Span interface {
    SetAttributes(attrs...)
    AddEvent(name, attrs...)
    RecordResult(r fault.Result)
    EndWithError(err error)
    End()
}
```

### What it does
Wraps OpenTelemetry's `trace.Tracer` and `trace.Span` behind **custom interfaces**. The [RecordResult](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#60-76) method on [Span](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#23-30) automatically:
- Sets `atropos.fault.duration_ms` attribute
- Sets span status (OK / Error)
- Records the [Detail](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#30-35) as a string attribute
- Ends the span

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Custom [Tracer](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#18-21)/[Span](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#23-30) interfaces over raw OTel** | Allows testing with mocks; encapsulates fault-specific recording logic ([RecordResult](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#60-76)) | 1. Use `otel.Tracer` directly everywhere | Custom interfaces decouple fault logic from OTel specifics. If you switched to Jaeger or Zipkin directly, only the adapter changes. The tradeoff is an extra abstraction layer. |
| **[RecordResult](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#60-76) combines multiple operations** | One call to set attributes + status + end the span | 1. Separate `SetDuration()`, `SetStatus()`, [End()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#83-86) calls | [RecordResult](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#60-76) ensures the span is always properly closed with all diagnostic data. Separate calls risk forgetting to end the span. |

---

## Section 10 — Public API & Type Re-exports ([types.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/types.go), [faults.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/faults.go))

### [types.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/types.go) — Re-exports

```go
type Fault      = fault.Fault
type FaultConfig = fault.FaultConfig
type Handle     = fault.Handle
type Result     = fault.Result
type EventAware = fault.EventAware
```

All internal types are re-exported as **type aliases** in the public `atropos` package. Users import `atropos.Fault`, not `atropos-go/internal/fault.Fault`.

### [faults.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/faults.go) — Factory Functions

Currently **no public CPU factory** — the CPU fault is used internally by the interceptor when a rule engine decision specifies `fault_type: "cpu"`. The existing factories are for inline faults:

```go
func NewLatencyFault(delay, jitter time.Duration) Fault { ... }
func NewHangFault(duration time.Duration) Fault { ... }
func NewErrorFault(statusCode int, message string) Fault { ... }
func NewDiskStressFault(...) Fault { ... }
```

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Type aliases (`=`) not type definitions** | `atropos.Fault` and `fault.Fault` are the **same** type — no conversion needed | 1. Wrapper types — would require explicit conversion | Aliases are transparent: no runtime cost, no conversion. The downside is that internal package changes directly affect the public API. |
| **Internal package (`internal/`)** | Prevents external imports of implementation details; only the public `atropos` surface is stable | 1. All code in one package — simpler but leaks internals<br>2. Separate module per fault type | `internal/` is Go's built-in access control. It enforces that consumers only use the stable public API. |
| **No public CPU factory (yet)** | CPU faults are typically triggered by rule engine decisions, not manually | 1. `NewCPUStressFault(load, duration)` public factory | A public factory would allow manual/programmatic fault injection. Currently, the admin API + rule engine is the intended entry point for resource faults. |

---

## Section 11 — Test Suite ([stress_test.go](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go))

| Test | What it verifies |
|---|---|
| [TestStress_Basic](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#25-48) | 30% load for 500ms completes in ~500ms, returns valid [Detail](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress.go#30-35) |
| [TestStress_NonBlocking](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#49-72) | [Start()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/trace/trace.go#19-20) returns in < 50ms (proves non-blocking) |
| [TestStress_Stop](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#73-90) | Calling [Stop()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/fault.go#71-75) after 300ms on a 5s fault terminates within 1s |
| [TestStress_ContextCancel](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#91-111) | Parent context cancellation propagates and terminates the fault |
| [TestStress_AutoDetectCPUs](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#112-136) | [AvailableCPUs()](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go#11-29) returns > 0; result matches `Detail.AvailableCPUs` |
| [TestStress_InvalidConfig](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#137-158) | Invalid configs (zero load, load > 1, zero duration, ramp ≥ duration) are rejected |
| [TestStress_RampUp](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/stress_test.go#159-176) | 400ms ramp-up on 800ms duration completes without error |

### Design Decisions & Tradeoffs

| Decision | Why | Alternatives | Tradeoff |
|---|---|---|---|
| **Duration tolerance bands (400-700ms for 500ms target)** | CPU scheduling is non-deterministic; tight bounds would flake in CI | 1. Exact match — flaky<br>2. Very wide bounds — meaningless | ±40% tolerance is wide enough for CI runners with variable load but narrow enough to catch regressions (e.g., fault running for 5s instead of 500ms). |
| **No mocking of [AvailableCPUs](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go#11-29)** | Tests run on real hardware; the detection code is simple enough to test directly | 1. Inject [AvailableCPUs](file:///c:/Users/hello/Desktop/SwayamFolder/WorkMaterial/Projects/microserviceFaultTestingProject/atropos-go/internal/fault/resource/cpu/detect.go#11-29) as a dependency<br>2. Mock the cgroup files in `/tmp` | Testing against real hardware validates the full stack. Mocking would only test the parsing logic, not the actual detection chain. |

---

## Summary — How It All Fits Together

```
     ┌─────────────────────────────────────────────────┐
     │                  Interceptor                     │
     │  1. Evaluator says: inject CPU fault, 30%, 5s   │
     │  2. Constructs cpu.Stress{TargetLoad: 0.3, ...} │
     │  3. Checks EventAware → injects EventEmitter    │
     │  4. Calls stress.Start(ctx) → gets Handle        │
     │  5. handle.SetOnResult(span.RecordResult)        │
     │  6. Waits on handle.Done() or request completes  │
     └────────────┬────────────────────────────────────┘
                  │
                  ▼
     ┌─────────────────────────────────────────────────┐
     │              cpu.Stress.Start()                  │
     │  1. AvailableCPUs() → 4.0 (from cgroup)         │
     │  2. workers = ceil(0.3 × 4.0) = 2               │
     │  3. perWorkerLoad = 1.2 / 2 = 0.6               │
     │  4. Launches 2 goroutines + 1 event goroutine    │
     └────────────┬────────────────────────────────────┘
                  │
                  ▼
     ┌─────────────────────────────────────────────────┐
     │            dutyCycle (per worker)                 │
     │  Loop:                                           │
     │    ├── Compute load from ramp phase              │
     │    ├── Burn: spin for (load × 100ms)             │
     │    └── Sleep: timer for (100ms - burn)            │
     │  Until: ctx.Done() (timeout or Stop)             │
     └─────────────────────────────────────────────────┘
```

### Key Architectural Tradeoffs Summary

| Area | Current Choice | Alternative Worth Considering |
|---|---|---|
| CPU burn method | Spin loop (pure, no side effects) | Math-heavy for ALU-specific stress |
| Worker threading | `LockOSThread` per goroutine | CPU affinity for core pinning |
| Load shaping | Duty-cycle with 100ms window | PID controller for feedback-based load |
| Ramp function | Linear | Sigmoid for smoother transitions |
| CPU detection | Manual cgroup parsing | `containerd/cgroups` library |
| Public API | Internal-only, rule-engine driven | Public factory for programmatic use |
| Result detail | `any` type | Typed interface |
