package network

import (
	"context"
	"fmt"
	"io"
	"net"
)

// Direction indicates which half of a proxied connection a toxic applies to.
type Direction int

const (
	Upstream   Direction = iota // client → server
	Downstream                  // server → client
)

func (d Direction) String() string {
	switch d {
	case Upstream:
		return "upstream"
	case Downstream:
		return "downstream"
	default:
		return fmt.Sprintf("direction(%d)", int(d))
	}
}

// Toxic transforms a unidirectional byte stream between proxy endpoints.
//
// Implementations must respect ctx cancellation and return promptly.
// Implementations must not close src or dst.
type Toxic interface {
	// Pipe reads from src, applies the toxic effect, and writes to dst.
	// Returns nil or io.EOF on clean completion, ctx.Err() on cancellation,
	// or *RSTError to signal the proxy to send a TCP RST.
	Pipe(ctx context.Context, src io.Reader, dst io.Writer) error
}

// ConnToxic is an optional interface for toxics that need raw TCP
// connection access (e.g., blackhole, TCP RST).
//
// If Hijack returns hijacked=true, the normal Pipe path is skipped —
// the toxic owns the connection lifecycle.
type ConnToxic interface {
	Hijack(ctx context.Context, client, upstream *net.TCPConn) (hijacked bool, err error)
}

// RSTError is a sentinel that tells the proxy to send a TCP RST
// (via SetLinger(0) + Close) instead of a clean FIN.
type RSTError struct {
	Reason         string
	BytesForwarded int64
}

func (e *RSTError) Error() string {
	return fmt.Sprintf("rst: %s (forwarded %d bytes)", e.Reason, e.BytesForwarded)
}

// passthrough is the no-op toxic: just copy bytes through.
func passthrough(ctx context.Context, src io.Reader, dst io.Writer) error {
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(dst, src)
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
