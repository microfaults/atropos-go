package inline

import (
	"context"
	"time"

	fault "atropos-go/internal/fault"
)

// Hang is an inline fault that blocks the current goroutine until
// the context is cancelled or the fault duration expires.
// This is the HTTP-level equivalent of a network blackhole.
//
// The caller's timeout is the only defense — exactly what makes
// blackholes the most dangerous failure mode for cascading failures.
type Hang struct {
	fault.FaultConfig
}

func (h *Hang) Validate() error {
	return h.FaultConfig.Validate()
}

func (h *Hang) Start(ctx context.Context) (*fault.Handle, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, h.Duration)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()
		start := time.Now()

		<-ctx.Done()

		handle.Send(fault.Result{
			ActualDuration: time.Since(start),
			Err:            ctx.Err(),
		})
	}()

	return handle, nil
}

var _ fault.Fault = (*Hang)(nil)
