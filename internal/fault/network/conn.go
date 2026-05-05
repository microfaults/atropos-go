package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
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

// checkHijackers iterates ConnToxic hijackers for a given phase. Returns true if
// a toxic hijacked the connection. server may be nil for pre-dial checks.
func (p *Proxy) checkHijackers(ctx context.Context, client, server *net.TCPConn, connID int64, phase string) bool {
	for _, tl := range p.Toxics {
		ct, ok := tl.Toxic.(ConnToxic)
		if !ok {
			continue
		}
		p.emitEvent(trace.EventNetToxicHijack,
			attribute.Int64(trace.AttrNetConnID, connID),
			attribute.String(trace.AttrNetToxicPhase, phase),
			attribute.String(trace.AttrNetToxicType, fmt.Sprintf("%T", ct)),
		)
		hijacked, err := ct.Hijack(ctx, client, server)
		if hijacked || err != nil {
			if server != nil {
				applyRST(client, server, err)
			}
			return true
		}
	}
	return false
}

// streamWorker pipes data through an optional toxic, emitting errors and cleaning up.
func (p *Proxy) streamWorker(ctx context.Context, src, dst *net.TCPConn, toxic Toxic, client, server *net.TCPConn, connID int64, direction string) {
	var err error
	if toxic != nil {
		err = toxic.Pipe(ctx, src, dst)
	} else {
		err = passthrough(ctx, src, dst)
	}
	if err != nil {
		p.emitEvent(trace.EventNetConnError,
			attribute.Int64(trace.AttrNetConnID, connID),
			attribute.String("error", err.Error()),
			attribute.String("atropos.net.direction", direction),
		)
	}
	applyRST(client, server, err)
	dst.CloseWrite()
}

// handleAffected dials upstream and applies configured toxics.
func (p *Proxy) handleAffected(ctx context.Context, client *net.TCPConn, connID int64) {
	defer client.Close()
	connStart := time.Now()

	if p.checkHijackers(ctx, client, nil, connID, "pre_dial") {
		p.emitEvent(trace.EventNetConnClosed,
			attribute.Int64(trace.AttrNetConnID, connID),
			attribute.Int64(trace.AttrNetConnDurationMs, time.Since(connStart).Milliseconds()),
			attribute.String("atropos.net.close_reason", "hijack_pre_dial"),
		)
		return
	}

	dialStart := time.Now()
	server, err := net.DialTimeout("tcp", p.Upstream, dialTimeout)
	dialDuration := time.Since(dialStart)

	p.emitEvent(trace.EventNetUpstreamDial,
		attribute.Int64(trace.AttrNetConnID, connID),
		attribute.String(trace.AttrNetUpstreamAddr, p.Upstream),
		attribute.Int64(trace.AttrNetDialDurationMs, dialDuration.Milliseconds()),
		attribute.Bool("atropos.net.upstream.error", err != nil),
	)

	if err != nil {
		p.emitEvent(trace.EventNetConnError,
			attribute.Int64(trace.AttrNetConnID, connID),
			attribute.String("error", err.Error()),
		)
		return
	}
	serverTCP := server.(*net.TCPConn)
	defer serverTCP.Close()

	if p.checkHijackers(ctx, client, serverTCP, connID, "post_dial") {
		p.emitEvent(trace.EventNetConnClosed,
			attribute.Int64(trace.AttrNetConnID, connID),
			attribute.Int64(trace.AttrNetConnDurationMs, time.Since(connStart).Milliseconds()),
			attribute.String("atropos.net.close_reason", "hijack_post_dial"),
		)
		return
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
	go func() {
		defer wg.Done()
		p.streamWorker(ctx, client, serverTCP, upToxic, client, serverTCP, connID, "upstream")
	}()
	go func() {
		defer wg.Done()
		p.streamWorker(ctx, serverTCP, client, downToxic, client, serverTCP, connID, "downstream")
	}()
	wg.Wait()

	p.emitEvent(trace.EventNetConnClosed,
		attribute.Int64(trace.AttrNetConnID, connID),
		attribute.Int64(trace.AttrNetConnDurationMs, time.Since(connStart).Milliseconds()),
	)
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
