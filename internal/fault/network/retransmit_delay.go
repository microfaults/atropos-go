package network

import (
	"context"
	"io"
	"math/rand"
	"time"
)

// RetransmitDelay simulates the *observable effect* of TCP packet loss at the
// proxy layer. It does not actually drop bytes — userspace TCP cannot, since
// data is retransmitted by the kernel and arrives reliably regardless. Instead,
// chunks flagged as "lost" are delayed by Delay (the simulated retransmit
// round-trip), and consecutive losses past ResetThreshold trigger a TCP RST
// (modeling TCP giving up after too many retransmissions).
//
// Uses ~1 MSS (1460 byte) reads for packet-granularity probability.
//
// Previously named Loss; renamed because the toxic does not lose bytes,
// only delays them. For true drop semantics use kernel-level tc/netem.
type RetransmitDelay struct {
	// Rate is the probability a chunk is "lost" [0.0, 1.0).
	Rate float64

	// Delay is the simulated retransmission delay for lost chunks.
	// Default: 200ms.
	Delay time.Duration

	// ResetThreshold: consecutive lost chunks before RST. 0 = never reset.
	ResetThreshold int
}

func (r *RetransmitDelay) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	delay := r.Delay
	if delay == 0 {
		delay = 200 * time.Millisecond
	}

	buf := make([]byte, 1460) // ~1 MSS for packet-granularity
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	consecutive := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			if rng.Float64() < r.Rate {
				consecutive++
				if r.ResetThreshold > 0 && consecutive >= r.ResetThreshold {
					return &RSTError{Reason: "consecutive_retransmit_threshold"}
				}
				// Simulate retransmission delay.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			} else {
				consecutive = 0
			}

			// Always forward — TCP retransmits, data is delayed not lost.
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
