package network

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const dialTimeout = 5 * time.Second

// handlePassthrough dials upstream and copies bidirectionally with no toxics.
func handlePassthrough(ctx context.Context, client *net.TCPConn, upstream string) {
	defer client.Close()

	server, err := net.DialTimeout("tcp", upstream, dialTimeout)
	if err != nil {
		return
	}
	defer server.Close()

	bidirectionalCopy(ctx, client, server.(*net.TCPConn))
}

// handleAffected dials upstream and applies configured toxics.
func (p *Proxy) handleAffected(ctx context.Context, client *net.TCPConn) {
	defer client.Close()

	// Check for ConnToxic hijackers before dialing upstream.
	for _, tl := range p.Toxics {
		ct, ok := tl.Toxic.(ConnToxic)
		if !ok {
			continue
		}
		// Blackhole-style: hijack before we even have an upstream conn.
		hijacked, err := ct.Hijack(ctx, client, nil)
		if hijacked || err != nil {
			return
		}
	}

	server, err := net.DialTimeout("tcp", p.Upstream, dialTimeout)
	if err != nil {
		return
	}
	serverTCP := server.(*net.TCPConn)
	defer serverTCP.Close()

	// Check ConnToxic hijackers that need both connections (e.g., RST after duration).
	for _, tl := range p.Toxics {
		ct, ok := tl.Toxic.(ConnToxic)
		if !ok {
			continue
		}
		hijacked, err := ct.Hijack(ctx, client, serverTCP)
		if hijacked || err != nil {
			applyRST(client, serverTCP, err)
			return
		}
	}

	// Split toxics by direction.
	var upToxic, downToxic Toxic
	for _, tl := range p.Toxics {
		switch tl.Direction {
		case Upstream:
			upToxic = tl.Toxic
		case Downstream:
			downToxic = tl.Toxic
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// client → [toxic] → server
	go func() {
		defer wg.Done()
		var err error
		if upToxic != nil {
			err = upToxic.Pipe(ctx, client, serverTCP)
		} else {
			err = passthrough(ctx, client, serverTCP)
		}
		applyRST(client, serverTCP, err)
		serverTCP.CloseWrite()
	}()

	// server → [toxic] → client
	go func() {
		defer wg.Done()
		var err error
		if downToxic != nil {
			err = downToxic.Pipe(ctx, serverTCP, client)
		} else {
			err = passthrough(ctx, serverTCP, client)
		}
		applyRST(client, serverTCP, err)
		client.CloseWrite()
	}()

	wg.Wait()
}

// bidirectionalCopy copies data between two TCP connections until one side
// closes or the context is cancelled.
func bidirectionalCopy(ctx context.Context, a, b *net.TCPConn) {
	var wg sync.WaitGroup
	wg.Add(2)

	half := func(dst, src *net.TCPConn) {
		defer wg.Done()
		io.Copy(dst, src)
		dst.CloseWrite()
	}

	go half(a, b)
	go half(b, a)

	go func() {
		<-ctx.Done()
		a.Close()
		b.Close()
	}()

	wg.Wait()
}

// applyRST checks if err is a *RSTError and sends TCP RST on both connections.
func applyRST(client, server *net.TCPConn, err error) {
	var rstErr *RSTError
	if errors.As(err, &rstErr) {
		client.SetLinger(0)
		if server != nil {
			server.SetLinger(0)
		}
	}
}
