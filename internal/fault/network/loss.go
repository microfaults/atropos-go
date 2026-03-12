package network

import (
	"context"
	"io"
	"math/rand"
	"time"
)

// Loss simulates packet loss at the TCP proxy level.
//
// Since TCP retransmits, we can't truly drop data. Instead we simulate
// the observable effects:
//   - "Lost" chunks are delayed by RetransmitDelay (simulating retransmission)
//   - Consecutive losses exceeding ResetThreshold trigger a TCP RST
//     (simulating TCP giving up after too many retransmissions)
//
// Uses ~1 MSS (1460 byte) reads for packet-granularity probability.
type Loss struct {
	// Rate is the probability a chunk is "lost" [0.0, 1.0).
	Rate float64

	// RetransmitDelay is the simulated retransmission delay for lost chunks.
	// Default: 200ms.
	RetransmitDelay time.Duration

	// ResetThreshold: consecutive lost chunks before RST. 0 = never reset.
	ResetThreshold int
}

func (l *Loss) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	retransmit := l.RetransmitDelay
	if retransmit == 0 {
		retransmit = 200 * time.Millisecond
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
			if rng.Float64() < l.Rate {
				consecutive++
				if l.ResetThreshold > 0 && consecutive >= l.ResetThreshold {
					return &RSTError{Reason: "packet_loss_threshold"}
				}
				// Simulate retransmission delay.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retransmit):
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
