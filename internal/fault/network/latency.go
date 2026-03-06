package network

import (
	"context"
	"io"
	"math/rand"
	"time"
)

// Latency adds a fixed delay plus optional random jitter before
// forwarding each chunk of data.
type Latency struct {
	// Delay is the base latency added per chunk.
	Delay time.Duration

	// Jitter is the max additional random delay per chunk.
	// Actual jitter is uniform in [0, Jitter].
	Jitter time.Duration
}

func (l *Latency) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	buf := make([]byte, 32*1024)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			delay := l.Delay
			if l.Jitter > 0 {
				delay += time.Duration(rng.Int63n(int64(l.Jitter)))
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			if _, err := dst.Write(buf[:n]); err != nil {
				return err
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
