package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Proxy owns the listeners of one sidecar.
type Proxy struct {
	cfg     Config
	servers []*http.Server
	egress  []net.Listener
	ingress net.Listener

	transparentEgress  net.Listener
	transparentIngress net.Listener
}

// New builds a Proxy; Start binds its listeners.
func New(cfg Config) *Proxy { return &Proxy{cfg: cfg} }

// Start binds every configured listener and serves in the background. On
// any bind failure it closes what it already opened and returns the error,
// so a half-started sidecar never registers.
func (p *Proxy) Start(ctx context.Context) error {
	for _, l := range p.cfg.Egress {
		ln, err := net.Listen("tcp", l.Listen)
		if err != nil {
			p.closeListeners()
			return fmt.Errorf("proxy: egress listen %s: %w", l.Listen, err)
		}
		p.egress = append(p.egress, ln)
		p.serve(ctx, ln, newEgressHandler(p.cfg, l))
	}
	if p.cfg.Ingress != nil {
		ln, err := net.Listen("tcp", p.cfg.Ingress.Listen)
		if err != nil {
			p.closeListeners()
			return fmt.Errorf("proxy: ingress listen %s: %w", p.cfg.Ingress.Listen, err)
		}
		p.ingress = ln
		p.serve(ctx, ln, newIngressHandler(p.cfg, *p.cfg.Ingress))
	}
	if te := p.cfg.TransparentEgress; te != nil {
		ln, err := net.Listen("tcp", te.Listen)
		if err != nil {
			p.closeListeners()
			return fmt.Errorf("proxy: transparent egress listen %s: %w", te.Listen, err)
		}
		p.transparentEgress = ln
		sl := &splitListener{
			Listener: ln, lookup: p.cfg.originalDst(), raw: portSet(te.RawTCPPorts), logger: p.cfg.logger(),
			relay: func(c net.Conn, dst string) { relay(ctx, c, dst, p.cfg.Policies, p.cfg.logger()) },
		}
		p.serve(ctx, sl, newTransparentEgressHandler(p.cfg, ln.Addr().String()))
	}
	if ti := p.cfg.TransparentIngress; ti != nil {
		ln, err := net.Listen("tcp", ti.Listen)
		if err != nil {
			p.closeListeners()
			return fmt.Errorf("proxy: transparent ingress listen %s: %w", ti.Listen, err)
		}
		p.transparentIngress = ln
		sl := &splitListener{Listener: ln, lookup: p.cfg.originalDst(), logger: p.cfg.logger()}
		p.serve(ctx, sl, newTransparentIngressHandler(p.cfg, *ti))
	}
	return nil
}

func (p *Proxy) serve(ctx context.Context, ln net.Listener, h http.Handler) {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext:       connContext,
	}
	p.servers = append(p.servers, srv)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.cfg.logger().Error("atroposd: listener stopped", "addr", ln.Addr().String(), "error", err)
		}
	}()
}

func (p *Proxy) closeListeners() {
	for _, ln := range p.egress {
		ln.Close()
	}
	if p.ingress != nil {
		p.ingress.Close()
	}
	if p.transparentEgress != nil {
		p.transparentEgress.Close()
	}
	if p.transparentIngress != nil {
		p.transparentIngress.Close()
	}
}

// TransparentEgressAddr returns the bound transparent egress address, or "".
func (p *Proxy) TransparentEgressAddr() string {
	if p.transparentEgress == nil {
		return ""
	}
	return p.transparentEgress.Addr().String()
}

// TransparentIngressAddr returns the bound transparent ingress address, or "".
func (p *Proxy) TransparentIngressAddr() string {
	if p.transparentIngress == nil {
		return ""
	}
	return p.transparentIngress.Addr().String()
}

// EgressAddrs returns the bound egress addresses, in Config.Egress order.
func (p *Proxy) EgressAddrs() []string {
	out := make([]string, len(p.egress))
	for i, ln := range p.egress {
		out[i] = ln.Addr().String()
	}
	return out
}

// IngressAddr returns the bound ingress address, or "" when none.
func (p *Proxy) IngressAddr() string {
	if p.ingress == nil {
		return ""
	}
	return p.ingress.Addr().String()
}

// Shutdown drains every listener until ctx expires.
func (p *Proxy) Shutdown(ctx context.Context) error {
	var errs []error
	for _, srv := range p.servers {
		if err := srv.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
