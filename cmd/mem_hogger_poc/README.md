# Memory Hogger POC

Proof-of-concept CLI for the **memory-pressure fault** in atropos-go. It allocates a target percentage of available RAM, holds it for a configurable duration, and optionally thrashes the allocated pages to stress the TLB and page tables.

## What this does

The memory hogger is a chaos engineering tool that simulates memory pressure on a service. It works in two phases:

1. **Allocate (ramp-up):** Gradually allocates 1 MiB chunks of memory until the target consumption is reached. Each chunk is page-touched (one write per 4 KiB page) to force the OS to back virtual pages with physical frames — ensuring real RSS growth, not just virtual address space reservation.

2. **Hold + optional thrash:** Holds all allocations alive for the remaining duration. If thrashing is enabled, worker goroutines continuously walk the allocated pages with a stride of 4097 bytes (one byte past a page boundary), doing read-modify-write operations. This forces a TLB miss on every access and stresses the page table walker, simulating the kind of memory pressure that causes observable performance degradation in real workloads.

During ramp-down, chunks are released linearly over the ramp-down period.

### Why this matters

- **OOM readiness:** Validate that your services handle out-of-memory conditions gracefully (restarts, circuit breakers, backpressure).
- **Noisy neighbor simulation:** Reproduce the effect of a co-located service consuming excessive memory on shared infrastructure.
- **Swap/paging behavior:** Observe how your service degrades when the kernel starts swapping pages to disk.
- **Right-sizing:** Determine if your container memory limits are set appropriately by pushing close to them under controlled conditions.

## Container-aware memory detection

The hogger automatically detects available memory, respecting container limits:

| Priority | Source | Path | Notes |
|----------|--------|------|-------|
| 1 | cgroup v2 | `/sys/fs/cgroup/memory.max` | Docker/k8s on modern kernels |
| 2 | cgroup v1 | `/sys/fs/cgroup/memory/memory.limit_in_bytes` | Older Docker/k8s setups |
| 3 | Linux | `/proc/meminfo` MemTotal | Bare-metal Linux |
| 4 | macOS | `sysctl hw.memsize` | Local dev machines |

When you run `docker run --memory=256m`, the hogger reads the 256 MiB limit from the cgroup and uses that as the basis for percentage calculations — not the host's total RAM.

## CLI flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--load` | float | `0.5` | Target memory consumption as a fraction of available memory (0.0–1.0]. `0.7` = consume 70%. |
| `--duration` | duration | `5s` | Total fault duration, including ramp-up and ramp-down periods. |
| `--rampup` | duration | `1s` | Time to linearly ramp allocation from 0 to target. `0` = allocate instantly. |
| `--rampdown` | duration | `1s` | Time to linearly release memory from target to 0. `0` = release instantly. |
| `--chunk` | int (bytes) | `1048576` (1 MiB) | Size of each allocation chunk. Smaller = finer ramp granularity but more slice overhead. |
| `--thrashing` | bool | `false` | Enable page-thrashing mode. Workers continuously access allocated pages in a TLB-hostile stride pattern. |
| `--thrashworkers` | int | `2` | Number of goroutines performing thrashing access (only used when `--thrashing` is set). |

## Usage

### Run directly (no Docker)

```bash
cd atropos-go

# Basic: consume 50% of system memory for 5 seconds
go run ./cmd/mem_hogger_poc

# Custom: 30% load, 10s duration, 2s ramp up/down
go run ./cmd/mem_hogger_poc --load=0.3 --duration=10s --rampup=2s --rampdown=2s

# With thrashing enabled
go run ./cmd/mem_hogger_poc --load=0.2 --duration=10s --thrashing --thrashworkers=4
```

### Run in Docker (recommended for realistic testing)

```bash
cd atropos-go

# Build the image
docker build -f cmd/mem_hogger_poc/Dockerfile.poc -t mem-hogger-poc .

# Run with a 256 MiB memory limit, consume 70%
docker run --rm --memory=256m mem-hogger-poc --load=0.7 --duration=10s --rampup=2s --rampdown=2s

# Watch container memory usage in another terminal
docker stats
```

### Test near-OOM behavior

```bash
# Push to 95% of a tight 128 MiB limit
docker run --rm --memory=128m mem-hogger-poc --load=0.95 --duration=5s --rampup=0 --rampdown=0
```

If the container gets OOM-killed, Docker reports exit code 137. That's expected — observing how your orchestrator handles OOM kills is one of the main use cases.

## Running the unit tests

```bash
cd atropos-go
go test -v ./internal/fault/resource/memory/...
```

The test suite covers:

| Test | What it verifies |
|------|-----------------|
| `TestStress_Basic` | Allocates 10% for 500ms, checks duration and detail fields |
| `TestStress_NonBlocking` | `Start()` returns instantly (fault runs in background) |
| `TestStress_Stop` | Early cancellation via `handle.Stop()` |
| `TestStress_ContextCancel` | External context cancellation propagates correctly |
| `TestStress_InvalidConfig` | Rejects zero load, load > 1, zero duration, ramp >= duration |
| `TestStress_RampUp` | Ramp-up phase completes without error |
| `TestStress_Thrashing` | Thrashing mode runs and reports `ThrashingEnabled=true` |
| `TestStress_AutoDetectMemory` | Verifies cgroup/system memory detection returns a sane value |

## Project structure

```
atropos-go/
├── cmd/mem_hogger_poc/
│   ├── main.go            # CLI entrypoint (flag parsing, banner, result display)
│   └── Dockerfile.poc     # Multi-stage alpine build for container testing
└── internal/fault/resource/memory/
    ├── detect.go          # cgroup v2/v1/procfs/sysctl memory detection
    ├── config.go          # Config struct, validation, defaults
    ├── stress.go          # Core 2-phase fault logic (allocate → hold/thrash)
    └── stress_test.go     # Unit test suite
```

The memory fault implements the `fault.Fault` interface (`Validate() error`, `Start(ctx) (*fault.Handle, error)`), so it plugs into the same orchestration layer as the CPU and IO faults.

## Example output

```
╔══════════════════════════════════════════╗
║     atropos-go · Memory Hogger POC       ║
╚══════════════════════════════════════════╝

  Available memory:       268435456 bytes (256.0 MiB)
  Current usage:          3874816 bytes (3.7 MiB)
  Target load:            70% of 268435456 bytes
  Target allocation:      184030003 bytes (175.5 MiB)
  Chunk size:             1048576 bytes (1024.0 KiB)
  Duration:               10s
  Ramp-up:                2s
  Ramp-down:              2s
  Thrashing:              false

Memory hogger started (non-blocking) — doing other work…

  [main] still running… tick #1
  [main] still running… tick #2
  ...

── Memory Hogger Results ────────────────────
  Available memory:       268435456 bytes (256.0 MiB)
  Target allocation:      184242995 bytes (175.7 MiB)
  Peak allocated:         183500800 bytes (175.0 MiB)
  Chunks allocated:       175
  Thrashing enabled:      false
  Actual duration:        10.001080589s
  Status:                 completed normally
```
