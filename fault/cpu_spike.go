package fault

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// CPUSpikeConfig defines parameters for a CPU spike fault injection.
type CPUSpikeConfig struct {
	// Cores is the number of logical CPU cores to saturate.
	// Clamped to runtime.NumCPU() if it exceeds available cores.
	Cores int

	// Duration is how long the CPU spike should last.
	// A zero or negative duration causes the spike to return immediately.
	Duration time.Duration
}

// CPUSpikeResult reports what actually happened during the spike.
type CPUSpikeResult struct {
	// ActualCores is the number of cores that were actually used (after clamping).
	ActualCores int

	// ActualDuration is how long the spike ran before stopping.
	ActualDuration time.Duration

	// Err is non-nil if the spike was cancelled externally or encountered an error.
	Err error
}

// InjectCPUSpike burns CPU on the requested number of cores for the given
// duration. It is safe to cancel early via the parent context.
//
// Safety controls:
//  1. Cores are clamped to runtime.NumCPU().
//  2. Duration is enforced via context.WithTimeout.
//  3. Each goroutine locks its OS thread so the spike is deterministic.
//  4. All goroutines are joined before returning — no leaked goroutines.
func InjectCPUSpike(ctx context.Context, cfg CPUSpikeConfig) CPUSpikeResult {
	// ── Validate & clamp ──────────────────────────────────────────────
	maxCores := runtime.NumCPU()
	cores := cfg.Cores
	if cores <= 0 {
		return CPUSpikeResult{Err: fmt.Errorf("cpu_spike: cores must be > 0, got %d", cores)}
	}
	if cores > maxCores {
		cores = maxCores
	}

	if cfg.Duration <= 0 {
		return CPUSpikeResult{
			ActualCores:    cores,
			ActualDuration: 0,
			Err:            fmt.Errorf("cpu_spike: duration must be > 0, got %s", cfg.Duration),
		}
	}

	// ── Create a timeout-scoped context ───────────────────────────────
	spikeCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	// ── Launch workers ────────────────────────────────────────────────
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < cores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Pin this goroutine to its own OS thread so it truly
			// saturates a physical/logical core.
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			// Tight busy-wait loop. The select checks for cancellation
			// periodically without adding meaningful overhead.
			for {
				select {
				case <-spikeCtx.Done():
					return
				default:
					// Burn CPU — this is intentionally empty.
					// The loop itself is the workload.
				}
			}
		}()
	}

	// Block until all workers finish (context timeout or external cancel).
	wg.Wait()
	elapsed := time.Since(start)

	// ── Build result ──────────────────────────────────────────────────
	result := CPUSpikeResult{
		ActualCores:    cores,
		ActualDuration: elapsed,
	}

	// Distinguish between normal completion (timeout) and external cancel.
	if ctx.Err() != nil && spikeCtx.Err() == context.DeadlineExceeded {
		// The parent context was cancelled while our timeout hadn't
		// fired yet — this is an external cancellation.
		result.Err = ctx.Err()
	}

	return result
}