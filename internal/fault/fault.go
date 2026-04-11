package fault

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// FaultConfig holds duration and ramp parameters common to all faults.
type FaultConfig struct {
	Duration time.Duration // total time including ramp phases
	RampUp   time.Duration // linear ramp 0→target; 0 = instant
	RampDown time.Duration // linear ramp target→0; 0 = instant
}

// Validate checks duration and ramp consistency.
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
// Lifecycle: Validate() → Start(ctx) → Handle.
type Fault interface {
	Validate() error
	Start(ctx context.Context) (*Handle, error)
}

// Handle provides non-blocking control over a running fault.
//
// onResult is stored as an atomic.Pointer so that SetOnResult (called on
// the caller goroutine after Start returns) does not race with Send
// (called on the fault's internal goroutine). Without this guard the race
// detector flagged any fault whose goroutine finishes before Start's caller
// can register its callback, which is common for short-lived latency and
// error faults.
type Handle struct {
	done     chan Result
	cancel   context.CancelFunc
	onResult atomic.Pointer[func(Result)]
}

// NewHandle creates a Handle wired to the given cancel func.
func NewHandle(cancel context.CancelFunc) *Handle {
	return &Handle{
		done:   make(chan Result, 1),
		cancel: cancel,
	}
}

// SetOnResult registers a callback that fires synchronously on Send.
// If Send has already fired (e.g. for a near-instantaneous fault), the
// callback will not be invoked -- callers should register it before they
// expect the fault to complete.
func (h *Handle) SetOnResult(fn func(Result)) {
	h.onResult.Store(&fn)
}

// Done returns a channel that receives one Result on completion.
func (h *Handle) Done() <-chan Result {
	return h.done
}

// Stop requests early shutdown. Non-blocking.
func (h *Handle) Stop() {
	h.cancel()
}

// Send delivers the result. Fires OnResult callback synchronously first.
func (h *Handle) Send(r Result) {
	if fn := h.onResult.Load(); fn != nil {
		(*fn)(r)
	}
	h.done <- r
}

// Result reports what happened during a fault.
type Result struct {
	ActualDuration time.Duration
	Err            error
	Detail         any // fault-specific diagnostics; typed callers can assert
}
