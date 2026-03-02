package fault

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// CPUThrottleConfig defines parameters for a CPU throttle fault injection.
type CPUThrottleConfig struct {
	// TargetLoad is the desired total CPU utilization as a fraction (0.0, 1.0].
	// This represents the fraction of ALL available CPU capacity.
	// For example, 0.5 on a 4-core system means 50% of total CPU = 2 full cores
	// worth of work.
	//
	// Available CPU is detected automatically (cgroup-aware for Docker/k8s),
	// so TargetLoad = 0.8 with --cpus=2 uses 80% of 2 cores = 1.6 core-units.
	TargetLoad float64

	// Duration is the total time the fault should run (including ramp-up).
	// Must be > 0.
	Duration time.Duration

	// RampUp is the time over which load linearly increases from 0 → TargetLoad.
	// Set to 0 for an instant jump to full load.
	// Must be < Duration.
	RampUp time.Duration

	// RampDown is the time over which load linearly decreases from TargetLoad → 0.
	// Set to 0 for an instant jump to zero load.
	// Must be < Duration.
	RampDown time.Duration

	// Window is the duty-cycle period.  Within each window the worker burns
	// CPU for (effectiveLoad × Window) and sleeps the rest.
	// Smaller windows give finer-grained control but slightly more overhead.
	// Defaults to 100 ms if zero.
	Window time.Duration
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

// CPUThrottleResult reports what actually happened during the throttle.
type CPUThrottleResult struct {
	// AvailableCPUs is the detected CPU capacity (cgroup-aware).
	AvailableCPUs float64

	// Workers is the number of worker goroutines used.
	Workers int

	// PerWorkerLoad is the duty-cycle load each worker ran (0.0–1.0).
	PerWorkerLoad float64

	// ActualDuration is how long the throttle ran before stopping.
	ActualDuration time.Duration

	// Err is non-nil if the throttle was cancelled or encountered an error.
	Err error
}

// ---------------------------------------------------------------------------
// Handle (non-blocking control surface)
// ---------------------------------------------------------------------------

// CPUThrottleHandle is returned by StartCPUThrottle and lets the caller
// monitor or cancel the throttle without blocking.
type CPUThrottleHandle struct {
	done   chan CPUThrottleResult
	cancel context.CancelFunc
}

// Done returns a channel that receives exactly one CPUThrottleResult when the
// throttle finishes (either naturally, via Stop, or via context cancellation).
// It is safe to call Done multiple times; the same channel is returned.
func (h *CPUThrottleHandle) Done() <-chan CPUThrottleResult {
	return h.done
}

// Stop requests an early shutdown of the throttle.  It does not block; the
// result will still be delivered on Done().
func (h *CPUThrottleHandle) Stop() {
	h.cancel()
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

const defaultWindow = 100 * time.Millisecond

// StartCPUThrottle begins a CPU throttle in the background and returns a
// handle immediately — it never blocks the caller.
//
// TargetLoad is the fraction of TOTAL available CPU capacity (container-aware).
// The throttler auto-detects the CPU quota (cgroup v1/v2 for Docker/k8s) and
// distributes load across the right number of workers.
//
// Example: TargetLoad=0.8 with Docker --cpus=2 → uses 80% of 2 cores total.
//
// Safety controls:
//  1. CPU quota auto-detected from cgroup (Docker/k8s) or runtime.NumCPU().
//  2. Duration is enforced via context.WithTimeout.
//  3. Each goroutine locks its OS thread for deterministic core usage.
//  4. All goroutines are joined before the result is sent — no leaked goroutines.
func StartCPUThrottle(ctx context.Context, cfg CPUThrottleConfig) (*CPUThrottleHandle, error) {
	// ── Validate ──────────────────────────────────────────────────────
	if cfg.TargetLoad <= 0 || cfg.TargetLoad > 1.0 {
		return nil, fmt.Errorf("cpu_throttle: target_load must be in (0.0, 1.0], got %.2f", cfg.TargetLoad)
	}
	if cfg.Duration <= 0 {
		return nil, fmt.Errorf("cpu_throttle: duration must be > 0, got %s", cfg.Duration)
	}
	if cfg.RampUp < 0 {
		return nil, fmt.Errorf("cpu_throttle: ramp_up must be >= 0, got %s", cfg.RampUp)
	}
	if cfg.RampUp >= cfg.Duration {
		return nil, fmt.Errorf("cpu_throttle: ramp_up (%s) must be < duration (%s)", cfg.RampUp, cfg.Duration)
	}
	if cfg.RampDown < 0 {
		return nil, fmt.Errorf("cpu_throttle: ramp_down must be >= 0, got %s", cfg.RampDown)
	}
	if cfg.RampDown >= cfg.Duration {
		return nil, fmt.Errorf("cpu_throttle: ramp_down (%s) must be < duration (%s)", cfg.RampDown, cfg.Duration)
	}
	if cfg.RampUp+cfg.RampDown >= cfg.Duration {
		return nil, fmt.Errorf("cpu_throttle: ramp_up (%s) + ramp_down (%s) must be < duration (%s)", cfg.RampUp, cfg.RampDown, cfg.Duration)
	}

	// ── Detect available CPUs (cgroup-aware) ──────────────────────────
	available := AvailableCPUs()

	// ── Compute workers and per-worker load ───────────────────────────
	// Total CPU units to consume = TargetLoad × available CPUs.
	// Distribute across workers evenly, capping per-worker at 1.0 (100%).
	totalUnits := cfg.TargetLoad * available

	// Number of workers = ceil(totalUnits), but at least 1 and at most NumCPU().
	workers := int(math.Ceil(totalUnits))
	if workers < 1 {
		workers = 1
	}
	maxWorkers := runtime.NumCPU()
	if workers > maxWorkers {
		workers = maxWorkers
	}

	perWorkerLoad := totalUnits / float64(workers)
	if perWorkerLoad > 1.0 {
		perWorkerLoad = 1.0
	}

	window := cfg.Window
	if window <= 0 {
		window = defaultWindow
	}

	// ── Build handle ──────────────────────────────────────────────────
	throttleCtx, cancel := context.WithTimeout(ctx, cfg.Duration)

	handle := &CPUThrottleHandle{
		done:   make(chan CPUThrottleResult, 1),
		cancel: cancel,
	}

	// ── Launch background orchestrator ────────────────────────────────
	go func() {
		defer cancel() // ensure timeout context is always freed

		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Pin to OS thread so we genuinely saturate a core.
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()

				worker(throttleCtx, perWorkerLoad, cfg.Duration, cfg.RampUp, cfg.RampDown, window, start)
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)

		result := CPUThrottleResult{
			AvailableCPUs:  available,
			Workers:        workers,
			PerWorkerLoad:  perWorkerLoad,
			ActualDuration: elapsed,
		}

		// If parent ctx was cancelled (not just our timeout), surface the error.
		if ctx.Err() != nil && throttleCtx.Err() == context.Canceled {
			result.Err = ctx.Err()
		}

		handle.done <- result
	}()

	return handle, nil
}

// ---------------------------------------------------------------------------
// Worker (duty-cycle loop)
// ---------------------------------------------------------------------------

// worker runs a single duty-cycle loop on one core until ctx is done.
func worker(ctx context.Context, targetLoad float64, totalDuration, rampUp, rampDown, window time.Duration, globalStart time.Time) {
	rampDownStart := totalDuration - rampDown

	for {
		// Check for cancellation before each cycle.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Compute effective load based on where we are in the timeline:
		//   [0, rampUp)           → ramp up:   0 → targetLoad
		//   [rampUp, rampDownStart) → steady:  targetLoad
		//   [rampDownStart, duration) → ramp down: targetLoad → 0
		elapsed := time.Since(globalStart)
		load := targetLoad

		if rampUp > 0 && elapsed < rampUp {
			// Linear interpolation: 0 → targetLoad over rampUp.
			load = targetLoad * (float64(elapsed) / float64(rampUp))
		} else if rampDown > 0 && elapsed >= rampDownStart {
			// Linear interpolation: targetLoad → 0 over rampDown.
			progress := float64(elapsed-rampDownStart) / float64(rampDown)
			if progress > 1.0 {
				progress = 1.0
			}
			load = targetLoad * (1.0 - progress)
		}

		burnTime := time.Duration(float64(window) * load)
		sleepTime := window - burnTime

		// ── Burn phase ────────────────────────────────────────────
		burnDeadline := time.Now().Add(burnTime)
		for time.Now().Before(burnDeadline) {
			select {
			case <-ctx.Done():
				return
			default:
				// Tight loop — intentionally empty; the loop IS the workload.
			}
		}

		// ── Sleep phase ───────────────────────────────────────────
		if sleepTime > 0 {
			t := time.NewTimer(sleepTime)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}