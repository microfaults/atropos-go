package inline

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	fault "github.com/microfaults/atropos-go/internal/fault"
)

// Latency is an inline fault that introduces a delay in the current
// goroutine. Unlike network.Latency (a stream Toxic for the TCP proxy),
// this is a simple sleep for use at HTTP injection points.
type Latency struct {
	fault.FaultConfig

	// Delay is the base latency to introduce.
	Delay time.Duration

	// Jitter is the max additional random delay.
	// Actual delay is Delay + rand(Jitter).
	Jitter time.Duration
}

func (l *Latency) Validate() error {
	if l.Delay <= 0 && l.Jitter <= 0 {
		return fmt.Errorf("inline/latency: delay or jitter must be > 0")
	}
	return nil
}

func (l *Latency) Start(ctx context.Context) (*fault.Handle, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()
		start := time.Now()

		delay := l.Delay
		if l.Jitter > 0 {
			delay += time.Duration(rand.Int63n(int64(l.Jitter)))
		}

		select {
		case <-time.After(delay):
			handle.Send(fault.Result{ActualDuration: time.Since(start)})
		case <-ctx.Done():
			handle.Send(fault.Result{ActualDuration: time.Since(start), Err: ctx.Err()})
		}
	}()

	return handle, nil
}
