package network

import (
	"context"
	"io"
	"net"
	"time"
)

// RST sends a TCP RST (connection reset) after a byte threshold,
// a time threshold, or whichever comes first.
//
// Mechanically: Go's SetLinger(0) + Close causes the kernel to
// send RST instead of FIN.
type RST struct {
	// AfterBytes is the number of bytes to forward before RST.
	// Zero means don't trigger on bytes.
	AfterBytes int64

	// AfterDuration is how long to wait before RST.
	// Zero means don't trigger on time.
	AfterDuration time.Duration
}

// Hijack handles the duration-only case (no byte counting needed).
func (r *RST) Hijack(ctx context.Context, client, upstream *net.TCPConn) (bool, error) {
	// Only hijack if we have both connections and only duration trigger.
	if upstream == nil || r.AfterBytes > 0 {
		return false, nil
	}
	if r.AfterDuration <= 0 {
		// Immediate RST.
		return true, &RSTError{Reason: "immediate"}
	}

	select {
	case <-time.After(r.AfterDuration):
		return true, &RSTError{Reason: "duration"}
	case <-ctx.Done():
		return true, ctx.Err()
	}
}

// Pipe forwards data, counting bytes and/or watching the clock.
// Returns *RSTError when the threshold is hit.
func (r *RST) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	var timer <-chan time.Time
	if r.AfterDuration > 0 {
		t := time.NewTimer(r.AfterDuration)
		defer t.Stop()
		timer = t.C
	}

	var forwarded int64
	buf := make([]byte, 32*1024)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer:
			return &RSTError{Reason: "duration", BytesForwarded: forwarded}
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			forwarded += int64(n)

			if r.AfterBytes > 0 && forwarded >= r.AfterBytes {
				return &RSTError{Reason: "bytes", BytesForwarded: forwarded}
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
