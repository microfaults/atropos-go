package cpu

import (
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"atropos-go/internal/fault"
	"atropos-go/internal/fault/resource"
	"atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Stress is a CPU-pressure fault that consumes a configurable fraction
// of available CPU capacity using duty-cycle spinning.
//
// Maps to iBench SoI11/12/15 (integer, FP, vector processing pressure)
// but at the system level rather than microarchitectural.
//
// Implements fault.Fault and fault.EventAware.
type Stress struct {
	resource.Config
	emit fault.EventEmitter
}

// Detail carries CPU-specific diagnostics from a completed fault.
type Detail struct {
	AvailableCPUs float64
	Workers       int
	PerWorkerLoad float64
}

// SetEventEmitter implements fault.EventAware.
func (s *Stress) SetEventEmitter(fn fault.EventEmitter) {
	s.emit = fn
}

// emitEvent is a nil-safe helper.
func (s *Stress) emitEvent(name string, attrs ...attribute.KeyValue) {
	if s.emit != nil {
		s.emit(name, attrs...)
	}
}

// Validate checks that the CPU stress config is valid.
func (s *Stress) Validate() error {
	return s.Config.Validate()
}

// Start begins CPU stress in the background and returns immediately.
//
// The stress distributes load across goroutines pinned to OS threads.
// Each worker runs a duty-cycle loop: burn for (load × window), sleep
// the rest. Load follows the ramp-up → steady → ramp-down timeline.
//
// Safety: CPU quota is auto-detected from cgroup (Docker/k8s) or
// runtime.NumCPU(). Duration is enforced via context.WithTimeout.
func (s *Stress) Start(ctx context.Context) (*fault.Handle, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	available := AvailableCPUs()

	// Total CPU units to consume = TargetLoad × available CPUs.
	// Workers = ceil(totalUnits), capped to NumCPU().
	totalUnits := s.TargetLoad * available
	workers := int(math.Ceil(totalUnits))
	if workers < 1 {
		workers = 1
	}
	if max := runtime.NumCPU(); workers > max {
		workers = max
	}

	perWorkerLoad := totalUnits / float64(workers)
	if perWorkerLoad > 1.0 {
		perWorkerLoad = 1.0
	}

	window := s.Config.EffectiveWindow()

	throttleCtx, cancel := context.WithTimeout(ctx, s.Duration)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()

		var wg sync.WaitGroup
		start := time.Now()

		// Phase-tracking goroutine: emits ramp events at transitions.
		if s.emit != nil && (s.RampUp > 0 || s.RampDown > 0) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.emitPhaseEvents(throttleCtx, start)
			}()
		}

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
				dutyCycle(throttleCtx, perWorkerLoad, s.Duration, s.RampUp, s.RampDown, window, start)
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)

		result := fault.Result{
			ActualDuration: elapsed,
			Detail: Detail{
				AvailableCPUs: available,
				Workers:       workers,
				PerWorkerLoad: perWorkerLoad,
			},
		}
		if ctx.Err() != nil && throttleCtx.Err() == context.Canceled {
			result.Err = ctx.Err()
		}

		handle.Send(result)
	}()

	return handle, nil
}

// emitPhaseEvents emits timestamped events at ramp phase boundaries.
func (s *Stress) emitPhaseEvents(ctx context.Context, globalStart time.Time) {
	rampDownStart := s.Duration - s.RampDown

	if s.RampUp > 0 {
		s.emitEvent(trace.EventResourceRampUpStart,
			attribute.Float64(trace.AttrResourceTargetLoad, s.TargetLoad),
			attribute.Int64(trace.AttrResourceRampUpMs, s.RampUp.Milliseconds()),
		)
		select {
		case <-time.After(s.RampUp):
			s.emitEvent(trace.EventResourceRampUpComplete)
		case <-ctx.Done():
			return
		}
	}

	s.emitEvent(trace.EventResourceSustainStart,
		attribute.Float64(trace.AttrResourceTargetLoad, s.TargetLoad),
	)

	if s.RampDown > 0 {
		sustainDuration := rampDownStart - s.RampUp
		if sustainDuration > 0 {
			select {
			case <-time.After(sustainDuration):
			case <-ctx.Done():
				return
			}
		}

		s.emitEvent(trace.EventResourceRampDownStart,
			attribute.Int64(trace.AttrResourceRampDownMs, s.RampDown.Milliseconds()),
		)
		select {
		case <-time.After(s.RampDown):
			s.emitEvent(trace.EventResourceRampDownComplete)
		case <-ctx.Done():
			return
		}
	}
}

// dutyCycle runs a single worker's burn/sleep loop on one OS thread.
func dutyCycle(ctx context.Context, targetLoad float64, totalDuration, rampUp, rampDown, window time.Duration, globalStart time.Time) {
	rampDownStart := totalDuration - rampDown

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Compute effective load based on timeline position.
		elapsed := time.Since(globalStart)
		load := targetLoad

		if rampUp > 0 && elapsed < rampUp {
			load = targetLoad * (float64(elapsed) / float64(rampUp))
		} else if rampDown > 0 && elapsed >= rampDownStart {
			progress := float64(elapsed-rampDownStart) / float64(rampDown)
			if progress > 1.0 {
				progress = 1.0
			}
			load = targetLoad * (1.0 - progress)
		}

		burnTime := time.Duration(float64(window) * load)
		sleepTime := window - burnTime

		// Burn phase: tight loop IS the workload.
		burnDeadline := time.Now().Add(burnTime)
		if dl, ok := ctx.Deadline(); ok && dl.Before(burnDeadline) {
			burnDeadline = dl
		}
		for time.Now().Before(burnDeadline) {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		// Sleep phase.
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

// Compile-time interface checks.
var (
	_ fault.Fault      = (*Stress)(nil)
	_ fault.EventAware = (*Stress)(nil)
)
