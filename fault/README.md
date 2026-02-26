## CPU Spike Fault Injection — POC

### Overview

A controlled CPU spike fault injector that saturates N logical cores for a configurable duration, with multiple safety layers to prevent runaway resource usage.

### Files

| File | Purpose |
|------|---------|
| `fault/cpu_spike.go` | Core logic — `InjectCPUSpike(ctx, cfg) → result` |
| `fault/cpu_spike_test.go` | 5 unit tests |
| `cmd/cpu_spike_poc/main.go` | Standalone CLI demo (`--cores`, `--duration`) |
| `Dockerfile.poc` | Docker image with CPU limits |

### How It Works

Each requested core gets a goroutine with `runtime.LockOSThread()` running a tight busy-wait loop:

```go
go func() {
    runtime.LockOSThread()
    for {
        select {
        case <-ctx.Done(): return
        default: // burn CPU
        }
    }
}()
```

**Why `LockOSThread()`?** Without it, Go's scheduler can multiplex goroutines onto the same OS thread — meaning 4 "spike" goroutines might only use 1 real core. Locking ensures each goroutine saturates its own core.

### Safety Controls (3 Layers)

| Layer | Mechanism | What it prevents |
|-------|-----------|-----------------|
| **Code** | Cores clamped to `runtime.NumCPU()` | Requesting more cores than exist |
| **Application** | `context.WithTimeout` + parent cancel | Spike running forever |
| **Container** | Docker `--cpus` flag | Escaping container CPU budget |

### API

```go
cfg := fault.CPUSpikeConfig{
    Cores:    2,                    // saturate 2 cores
    Duration: 5 * time.Second,     // for 5 seconds
}

result := fault.InjectCPUSpike(ctx, cfg)
// result.ActualCores    → 2
// result.ActualDuration → ~5s
// result.Err            → nil (or cancellation error)
```

### Running Tests

```bash
go test ./fault/ -v -run TestInjectCPUSpike -count=1
```

| Test | Verifies |
|------|----------|
| `Basic` | 1 core, 500 ms, completes within ±200 ms |
| `Cancellation` | External cancel after 200 ms stops spike early |
| `ClampsCores` | 999 requested → clamped to `NumCPU()` |
| `ZeroDuration` | Returns error immediately |
| `InvalidCores` | 0 cores → returns error |

### Running the CLI

```bash
go run ./cmd/cpu_spike_poc --cores=2 --duration=5s
```

Supports `Ctrl+C` for early cancellation.

### Docker Demo (Recommended for Safe Testing)

```bash
# Build
docker build -f Dockerfile.poc -t atropos-cpu-poc .

# Run — limit container to 2 CPU cores, spike for 5s
docker run --rm --cpus=2 atropos-cpu-poc --cores=2 --duration=5s

# In another terminal, watch CPU usage
docker stats
```

You should see CPU % approach **200%** (2 cores × 100%) during the spike, then the container exits cleanly.

### Scalability Pattern

The `Config → Inject(ctx, cfg) → Result` pattern repeats for every fault type:

```
fault/
├── cpu_spike.go       ← this POC
├── memory_pressure.go ← future: allocate & hold memory
├── disk_fill.go       ← future: write temp files
├── io_stress.go       ← future: heavy read/write loops
└── network_latency.go ← future: add delay to outbound calls
```

Each follows the same signature, making them composable with the rule engine and OTel instrumentation.
