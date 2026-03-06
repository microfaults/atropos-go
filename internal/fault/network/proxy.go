package network

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	fault "atropos-go/internal/fault"
)

// Proxy is a TCP fault that sits between client and upstream,
// applying toxic effects to connections passing through it.
//
// Implements fault.Fault.
type Proxy struct {
	Config
}

// Detail carries diagnostics from a completed proxy fault.
type Detail struct {
	ListenAddr       string
	UpstreamAddr     string
	TotalConnections int64
	AffectedConns    int64
	PassthroughConns int64
}

// Validate checks that the proxy config is valid.
func (p *Proxy) Validate() error {
	return p.Config.Validate()
}

// Start begins the proxy in the background and returns immediately.
func (p *Proxy) Start(ctx context.Context) (*fault.Handle, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	proxyCtx, cancel := context.WithTimeout(ctx, p.Duration)
	handle := fault.NewHandle(cancel)

	// External backend path.
	if p.External != nil {
		go p.runExternal(proxyCtx, cancel, handle)
		return handle, nil
	}

	// Built-in TCP proxy path.
	listener, err := net.Listen("tcp", p.Listen)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("network: listen on %s: %w", p.Listen, err)
	}

	go p.run(proxyCtx, cancel, listener.(*net.TCPListener), handle)
	return handle, nil
}

func (p *Proxy) runExternal(ctx context.Context, cancel context.CancelFunc, handle *fault.Handle) {
	defer cancel()
	start := time.Now()

	if err := p.External.Apply(ctx, p.Listen, p.Upstream, p.Toxics); err != nil {
		handle.Send(fault.Result{ActualDuration: time.Since(start), Err: err})
		return
	}

	// Block until duration expires or context cancelled.
	<-ctx.Done()

	// Best-effort cleanup.
	removeCtx, removeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer removeCancel()
	_ = p.External.Remove(removeCtx)

	handle.Send(fault.Result{ActualDuration: time.Since(start)})
}

func (p *Proxy) run(ctx context.Context, cancel context.CancelFunc, listener *net.TCPListener, handle *fault.Handle) {
	defer cancel()
	defer listener.Close()

	var (
		wg          sync.WaitGroup
		totalConns  atomic.Int64
		affected    atomic.Int64
		passthrough atomic.Int64
		start       = time.Now()
		scope       = p.Config.effectiveScope()
		rng         = rand.New(rand.NewSource(time.Now().UnixNano()))
	)

	// Close listener when context expires to unblock Accept.
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// Context cancelled or listener closed — done accepting.
			if ctx.Err() != nil {
				break
			}
			// Check if it's a permanent error.
			var netErr *net.OpError
			if errors.As(err, &netErr) && !netErr.Temporary() {
				break
			}
			continue
		}

		totalConns.Add(1)
		applyToxics := rng.Float64() < scope

		wg.Add(1)
		if applyToxics {
			affected.Add(1)
			go func(c *net.TCPConn) {
				defer wg.Done()
				p.handleAffected(ctx, c)
			}(conn)
		} else {
			passthrough.Add(1)
			go func(c *net.TCPConn) {
				defer wg.Done()
				handlePassthrough(ctx, c, p.Upstream)
			}(conn)
		}
	}

	wg.Wait()

	handle.Send(fault.Result{
		ActualDuration: time.Since(start),
		Detail: Detail{
			ListenAddr:       listener.Addr().String(),
			UpstreamAddr:     p.Upstream,
			TotalConnections: totalConns.Load(),
			AffectedConns:    affected.Load(),
			PassthroughConns: passthrough.Load(),
		},
	})
}

// Compile-time interface check.
var _ fault.Fault = (*Proxy)(nil)
