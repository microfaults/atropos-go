package proxy

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// TransparentEgress is the single egress listener iptables REDIRECTs the
// app's outbound TCP to (design §5.2).
type TransparentEgress struct {
	Listen string // e.g. ":16001"
	// RawTCPPorts are original-destination ports of non-HTTP dependencies
	// (redis, kafka, postgres): connections to them are relayed at L4 with
	// armed toxics applied, and never parsed as HTTP.
	RawTCPPorts []int
}

// TransparentIngress is the single ingress listener iptables REDIRECTs the
// app's inbound ports to. The original destination port selects the app
// port to forward to.
type TransparentIngress struct {
	Listen       string // e.g. ":16006"
	AppHost      string // default "127.0.0.1"
	AllowedPorts []int  // original-destination ports accepted; empty = any
}

// splitListener wraps a transparent listener: Accept resolves each
// connection's original destination, relays raw-TCP destinations itself
// (they never reach the HTTP server), and returns the rest as
// *origDstConn for the HTTP server. A connection whose destination cannot
// be resolved is still returned, with the error attached, so the handler
// answers 502 with the reason instead of the connection vanishing.
type splitListener struct {
	net.Listener
	lookup OriginalDstFunc
	raw    map[int]bool
	relay  func(net.Conn, string)
	logger *slog.Logger
}

func (l *splitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		dst, err := l.lookup(c)
		if err != nil {
			l.logger.Warn("atroposd: original destination unavailable", "remote", c.RemoteAddr().String(), "error", err)
			return &origDstConn{Conn: c, err: err}, nil
		}
		if _, port, e := net.SplitHostPort(dst); e == nil {
			if p, e := strconv.Atoi(port); e == nil && l.raw[p] {
				go l.relay(c, dst)
				continue
			}
		}
		return &origDstConn{Conn: c, dst: dst}, nil
	}
}

func portSet(ports []int) map[int]bool {
	m := make(map[int]bool, len(ports))
	for _, p := range ports {
		m[p] = true
	}
	return m
}

// isSelf reports whether dst is this listener: the same port on a loopback
// address. Loopback traffic is not redirected (init.sh returns it), so a
// connection whose original destination is the listener itself was opened
// to the listener directly — forwarding it would loop.
func isSelf(dst, listenAddr string) bool {
	dh, dp, err := net.SplitHostPort(dst)
	if err != nil {
		return false
	}
	_, lp, err := net.SplitHostPort(listenAddr)
	if err != nil || dp != lp {
		return false
	}
	ip := net.ParseIP(dh)
	return ip != nil && ip.IsLoopback()
}

// newTransparentEgressHandler is the explicit egress handler with the
// upstream taken from the connection's original destination. URL.Host stays
// the name the app dialed (its Host header), so canonical_v2 keys and toxic
// targets see "productcatalogservice:3550", not a ClusterIP; the transport
// (NewTransparentTransport) dials the original destination.
func newTransparentEgressHandler(cfg Config, listenAddr string) http.Handler {
	transport := instrument.EgressTransport(timedTransport{cfg.base()}, cfg.Resolve)
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = pr.In.Host
			pr.Out.Host = pr.In.Host
			if pr.Out.Header.Get("Accept-Encoding") == "gzip" {
				pr.Out.Header.Del("Accept-Encoding") // see the explicit handler
			}
		},
		Transport:    transport,
		ErrorHandler: newErrorHandler(cfg.logger()),
	}
	propagator := otel.GetTextMapPropagator()
	return observe("egress", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dst, err := origDstFrom(r.Context())
		if err != nil {
			http.Error(w, "atroposd: no original destination for this connection (is the iptables REDIRECT in place?): "+err.Error(), http.StatusBadGateway)
			return
		}
		if isSelf(dst, listenAddr) {
			http.Error(w, "atroposd: refusing to proxy to itself: original destination "+dst+" is the egress listener (iptables loop?)", http.StatusBadGateway)
			return
		}
		if r.Host == "" {
			// HTTP/1.0 without Host: the only name we have is the address.
			r.Host = dst
		}
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		rp.ServeHTTP(w, r.WithContext(ctx))
	}))
}

// newTransparentIngressHandler is the explicit ingress handler with the app
// port taken from the connection's original destination.
func newTransparentIngressHandler(cfg Config, l TransparentIngress) http.Handler {
	appHost := l.AppHost
	if appHost == "" {
		appHost = "127.0.0.1"
	}
	allowed := portSet(l.AllowedPorts)
	propagator := otel.GetTextMapPropagator()
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			_, port, _ := net.SplitHostPort(mustOrigDst(pr.In))
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = net.JoinHostPort(appHost, port)
			pr.Out.Host = pr.In.Host
			propagator.Inject(pr.In.Context(), propagation.HeaderCarrier(pr.Out.Header))
		},
		Transport:    timedTransport{http.DefaultTransport},
		ErrorHandler: newErrorHandler(cfg.logger()),
	}
	inner := instrument.IngressMiddleware(rp, cfg.Service, cfg.Resolve())
	return observe("ingress", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dst, err := origDstFrom(r.Context())
		if err != nil {
			http.Error(w, "atroposd: no original destination for this connection (is the iptables REDIRECT in place?): "+err.Error(), http.StatusBadGateway)
			return
		}
		_, port, perr := net.SplitHostPort(dst)
		p, aerr := strconv.Atoi(port)
		if perr != nil || aerr != nil || (len(allowed) > 0 && !allowed[p]) {
			http.Error(w, "atroposd: original destination "+dst+" is not an inbound app port", http.StatusBadGateway)
			return
		}
		inner.ServeHTTP(w, r)
	}))
}

// mustOrigDst is origDstFrom for a request the handler already validated.
func mustOrigDst(r *http.Request) string {
	dst, _ := origDstFrom(r.Context())
	return dst
}
