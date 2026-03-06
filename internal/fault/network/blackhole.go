package network

import (
	"context"
	"io"
	"net"
)

// Blackhole accepts TCP connections but never responds.
// The client sees a live connection that produces no data —
// only their own timeout can save them. This is the most
// dangerous failure mode for cascading failures.
//
// Implements both Toxic and ConnToxic. When used as a ConnToxic,
// it hijacks the connection before upstream is dialed.
type Blackhole struct{}

// Hijack takes ownership of the connection. If upstream is nil (pre-dial),
// it consumes client data and blocks until ctx expires.
func (b *Blackhole) Hijack(ctx context.Context, client, upstream *net.TCPConn) (bool, error) {
	if upstream != nil {
		// Only hijack in the pre-dial phase (upstream == nil).
		return false, nil
	}

	// Consume client writes so their send buffer doesn't block.
	go io.Copy(io.Discard, client)

	// Hold the connection open until the fault expires.
	<-ctx.Done()
	return true, nil
}

// Pipe is the stream-level fallback: consume src, write nothing to dst.
func (b *Blackhole) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	go io.Copy(io.Discard, src)
	<-ctx.Done()
	return ctx.Err()
}
