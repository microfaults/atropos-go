package fault

import (
	"context"
	"fmt"
	"sync"
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
// Callbacks accumulate rather than replace: the interceptor registers a
// span-recording callback and the fault registry registers its
// bookkeeping callback on the same handle, and neither may clobber the
// other. A callback registered after the result was already delivered
// fires immediately on the registering goroutine -- the previous
// atomic-pointer implementation silently dropped it, which for a
// near-instant fault leaked the registry's tracking entry and made
// FaultRegistry.Close hang on its WaitGroup.
type Handle struct {
	done   chan Result
	cancel context.CancelFunc

	mu        sync.Mutex
	callbacks []func(Result)
	result    *Result
}

// NewHandle creates a Handle wired to the given cancel func.
func NewHandle(cancel context.CancelFunc) *Handle {
	return &Handle{
		done:   make(chan Result, 1),
		cancel: cancel,
	}
}

// SetOnResult registers a callback that fires synchronously on Send, or
// immediately if the result has already been delivered. Callbacks fire in
// registration order.
func (h *Handle) SetOnResult(fn func(Result)) {
	h.mu.Lock()
	if h.result != nil {
		r := *h.result
		h.mu.Unlock()
		fn(r)
		return
	}
	h.callbacks = append(h.callbacks, fn)
	h.mu.Unlock()
}

// Done returns a channel that receives one Result on completion.
func (h *Handle) Done() <-chan Result {
	return h.done
}

// Stop requests early shutdown by cancelling the fault's context.
// Non-blocking; the fault delivers its Result (with the truncated
// ActualDuration) through the normal Send path once it has cleaned up.
func (h *Handle) Stop() {
	h.cancel()
}

// Send delivers the result exactly once: callbacks fire synchronously
// first (in registration order), then the done channel receives. A second
// Send is a no-op.
func (h *Handle) Send(r Result) {
	h.mu.Lock()
	if h.result != nil {
		h.mu.Unlock()
		return
	}
	h.result = &r
	cbs := h.callbacks
	h.callbacks = nil
	h.mu.Unlock()
	for _, fn := range cbs {
		fn(r)
	}
	h.done <- r
}

// Result reports what happened during a fault.
type Result struct {
	ActualDuration time.Duration
	Err            error
	Detail         any // fault-specific diagnostics; typed callers can assert
}
