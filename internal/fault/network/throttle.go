package network

import (
	"context"
	"io"
	"time"
)

// Throttle rate-limits throughput to BytesPerSec using a simple
// token-based approach. No external dependencies.
type Throttle struct {
	// BytesPerSec is the maximum throughput.
	BytesPerSec int64
}

func (t *Throttle) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	rate := float64(t.BytesPerSec)
	buf := make([]byte, 32*1024)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			// How long should transmitting n bytes take at this rate?
			transmitTime := time.Duration(float64(n) / rate * float64(time.Second))
			start := time.Now()

			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}

			// Sleep for the remaining time to enforce the rate.
			elapsed := time.Since(start)
			if sleep := transmitTime - elapsed; sleep > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleep):
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return nil
			}
			return readErr
		}
	}
}
