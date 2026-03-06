package fault

import (
	"context"
	"fmt"
	"time"
)

// FaultConfig holds parameters common to all faults.
// Every fault has a bounded duration and optional linear ramp phases.
type FaultConfig struct {
	// Duration is the total time the fault runs (including ramp-up/down).
	Duration time.Duration

	// RampUp is the time to linearly increase from 0 to target intensity.
	// Zero means instant jump.
	RampUp time.Duration

	// RampDown is the time to linearly decrease from target intensity to 0.
	// Zero means instant drop.
	RampDown time.Duration
}

// Validate checks that the base fault config is self-consistent.
func (c *FaultConfig) Validate() error {
	if c.Duration <= 0 {
		return fmt.Errorf("fault: duration must be > 0, got %s", c.Duration)
	}
	if c.RampUp < 0 {
		return fmt.Errorf("fault: ramp_up must be >= 0, got %s", c.RampUp)
	}
	if c.RampUp >= c.Duration {
		return fmt.Errorf("fault: ramp_up (%s) must be < duration (%s)", c.RampUp, c.Duration)
	}
	if c.RampDown < 0 {
		return fmt.Errorf("fault: ramp_down must be >= 0, got %s", c.RampDown)
	}
	if c.RampDown >= c.Duration {
		return fmt.Errorf("fault: ramp_down (%s) must be < duration (%s)", c.RampDown, c.Duration)
	}
	if c.RampUp+c.RampDown >= c.Duration {
		return fmt.Errorf("fault: ramp_up (%s) + ramp_down (%s) must be < duration (%s)", c.RampUp, c.RampDown, c.Duration)
	}
	return nil
}

// Fault is the interface all fault types implement.
//
// The lifecycle is: create concrete fault → Validate() → Start(ctx) → Handle.
// The orchestrator can work with any Fault without knowing its concrete type.
type Fault interface {
	// Validate checks that the fault config is valid before starting.
	Validate() error

	// Start begins fault injection in the background and returns immediately.
	// The returned Handle provides non-blocking control over the running fault.
	Start(ctx context.Context) (*Handle, error)
}

// Handle provides non-blocking control over a running fault.
// It is returned by Fault.Start and lets the caller wait for completion
// or request early cancellation without blocking.
type Handle struct {
	done     chan Result
	cancel   context.CancelFunc
	onResult func(Result)
}

// NewHandle creates a Handle wired to the given cancel func.
// Fault implementations use this to build their handle.
func NewHandle(cancel context.CancelFunc) *Handle {
	return &Handle{
		done:   make(chan Result, 1),
		cancel: cancel,
	}
}

// SetOnResult registers a callback that fires synchronously when Send
// is called, before the result enters the Done channel. Used by the
// interceptor to end OTel spans on fault completion.
func (h *Handle) SetOnResult(fn func(Result)) {
	h.onResult = fn
}

// Done returns a channel that receives exactly one Result when the fault
// finishes (naturally, via Stop, or via context cancellation).
func (h *Handle) Done() <-chan Result {
	return h.done
}

// Stop requests early shutdown. Non-blocking; the result still arrives on Done().
func (h *Handle) Stop() {
	h.cancel()
}

// Send delivers the result to the Done channel. Called once by fault implementations.
// If an OnResult callback is set, it fires synchronously before the result
// is pushed to the channel.
func (h *Handle) Send(r Result) {
	if h.onResult != nil {
		h.onResult(r)
	}
	h.done <- r
}

// Result reports what happened during a fault.
type Result struct {
	// ActualDuration is how long the fault ran.
	ActualDuration time.Duration

	// Err is non-nil if the fault was cancelled or hit an error.
	Err error

	// Detail carries fault-specific diagnostics (e.g., cpu.Detail).
	// The orchestrator can ignore this; typed callers can assert.
	Detail any
}
