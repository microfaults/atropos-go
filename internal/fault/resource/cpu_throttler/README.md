## CPU Throttle Fault Injection

### Overview

A non-blocking, **container-aware** CPU throttle fault injector that targets a **total CPU percentage** across all available cores with optional **ramp-up**. Designed to be embedded in an SDK where request processing must continue while the fault runs in the background.

**Container-aware:** Automatically detects the CPU quota from cgroup v1/v2 (Docker `--cpus`, k8s resource limits) instead of blindly using `runtime.NumCPU()` which reports host cores inside containers.

### Files

| File | Purpose |
|------|---------:|
| `cpu_spike.go` | Core throttle logic — `StartCPUThrottle(ctx, cfg) → (*CPUThrottleHandle, error)` |
| `cpu_detect.go` | Cgroup-aware CPU detection — `AvailableCPUs() float64` |
| `cpu_spike_test.go` | 7 unit tests |
| `cmd/cpu_spike_poc/main.go` | Standalone CLI demo |
| `Dockerfile.poc` | Docker image with CPU limits |

### How It Works

**Total CPU load:** `TargetLoad` is a fraction of ALL available CPU capacity. With `--cpus=2` and `TargetLoad=0.8`, the throttler uses 80% of 2 cores = 1.6 core-units total.

**CPU detection:** `AvailableCPUs()` reads cgroup v2 (`/sys/fs/cgroup/cpu.max`) or v1 (`cpu.cfs_quota_us / cpu.cfs_period_us`), falling back to `runtime.NumCPU()` on bare metal.

**Worker calculation:** The throttler auto-computes:
- Workers = `ceil(TargetLoad × AvailableCPUs)`
- Per-worker load = `(TargetLoad × AvailableCPUs) / Workers`

**Duty cycle:** Each worker alternates burn/sleep within a configurable window:

```
┌─────── one window (100ms) ───────┐
│  burn (load × window)   │  sleep │
└─────────────────────────┘────────┘
```

**Ramp-up:** During the ramp-up phase, effective load linearly interpolates from 0% → target, then holds steady.

### Safety Controls (3 Layers)

| Layer | Mechanism | What it prevents |
|-------|-----------|-----------------:|
| **Code** | Cgroup-aware CPU detection | Over-using container CPU budget |
| **Application** | `context.WithTimeout` + parent cancel | Throttle running forever |
| **Container** | Docker `--cpus` flag | Escaping container CPU budget |

### API

```go
cfg := fault.CPUThrottleConfig{
    TargetLoad: 0.8,                  // 80% of total available CPU
    Duration:   10 * time.Second,     // total fault duration
    RampUp:     3 * time.Second,      // ramp 0% → 80% over 3s
}

handle, err := fault.StartCPUThrottle(ctx, cfg)
if err != nil {
    // invalid config
}

// ✅ Non-blocking — your request/goroutine continues here
doOtherWork()

// When you need the result:
result := <-handle.Done()
// result.AvailableCPUs   → 2.0  (from cgroup)
// result.Workers         → 2
// result.PerWorkerLoad   → 0.8
// result.ActualDuration  → ~10s

// Or cancel early:
handle.Stop()
```

### Config Parameters

| Parameter | Type | Default | Description |
|-----------|------|--------:|-------------|
| `TargetLoad` | `float64` | — | Total CPU utilisation fraction (0.0, 1.0] across all available cores |
| `Duration` | `time.Duration` | — | Total fault lifetime (including ramp-up) |
| `RampUp` | `time.Duration` | `0` | Linear ramp from 0 → target (0 = instant) |
| `Window` | `time.Duration` | `100ms` | Duty-cycle granularity |

### Running Tests

```bash
go test ./internal/fault/resource/cpu_throttler/ -v -count=1
```

| Test | Verifies |
|------|----------|
| `Basic` | 30% total load, 500ms — completes and delivers result on channel |
| `NonBlocking` | `StartCPUThrottle` returns in < 50ms |
| `Stop` | `handle.Stop()` cancels throttle early |
| `ContextCancel` | External context cancellation |
| `AutoDetectCPUs` | `AvailableCPUs()` returns > 0 and matches result |
| `InvalidConfig` | Bad load, duration, ramp → returns error |
| `RampUp` | Ramp-up completes within duration |

### Running the CLI

```bash
# Bare metal
go run ./cmd/cpu_spike_poc/ -load 0.6 -duration 10s -rampup 3s

# Docker (respects --cpus limit)
docker run --rm --cpus=2 atropos-cpu-poc -load 0.8 -duration 10s -rampup 3s
```

The POC prints periodic ticks proving the main goroutine is **not blocked** while the CPU throttle runs.

### Docker Demo

```bash
# Build
docker build -f Dockerfile.poc -t atropos-cpu-poc .

# Run with 2 CPU limit, 80% total load → 1.6 core-units
docker run --rm --cpus=2 atropos-cpu-poc -load 0.8 -duration 10s -rampup 3s

# In another terminal
docker stats
```

You should see CPU % ramp to ~160% (80% of 2 cores) over 3s, then hold.

### Scalability Pattern

The `Config → Start(ctx, cfg) → Handle` pattern repeats for every fault type:

```
internal/fault/resource/
├── cpu_throttler/      ← this package
├── memory_pressure/    ← future: allocate & hold memory
├── disk_fill/          ← future: write temp files
├── io_stress/          ← future: heavy read/write loops
└── network_latency/    ← future: add delay to outbound calls
```

Each follows the same non-blocking, channel-based signature, making them composable with the rule engine and OTel instrumentation.
