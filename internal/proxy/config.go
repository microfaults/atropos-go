// Package proxy is the data plane of the atroposd sidecar: an egress
// listener per dependency (explicit mode) or one for all of them
// (transparent mode, M2) and an ingress listener in front of the app, each
// wrapping the exact middleware composition the in-process host uses
// (internal/instrument), so faults, cache-box keys, headers and metrics
// are identical across hosts.
package proxy

import (
	"log/slog"
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
)

// EgressListener is one explicit-mode egress port: the app dials Listen
// instead of Upstream, and the proxy forwards to Upstream with the Host
// header rewritten to Upstream so cache keys see the real dependency
// (design §5.2, flaw A2).
type EgressListener struct {
	Listen   string // e.g. "127.0.0.1:19090"
	Upstream string // e.g. "productcatalogservice:3550"
}

// IngressListener fronts the app: callers reach Listen; the proxy forwards
// to Upstream (the app's own port) preserving the Host header.
type IngressListener struct {
	Listen   string // e.g. ":16006"
	Upstream string // e.g. "127.0.0.1:8080"
}

// Config configures a Proxy.
type Config struct {
	// Service labels metrics and names the otelhttp server span.
	Service string
	// Resolve returns the interceptor every request is dispatched through.
	// Called per egress round-trip and once for the ingress listener.
	Resolve func() *interceptor.Interceptor
	// Egress lists the explicit-mode egress listeners.
	Egress []EgressListener
	// Ingress is the optional ingress listener.
	Ingress *IngressListener
	// Base is the transport the egress transport composition forwards
	// through; nil uses http.DefaultTransport. The daemon passes a
	// ToxicTransport (over NewTransparentTransport in transparent mode) so
	// armed network toxics apply to live calls.
	Base   http.RoundTripper
	Logger *slog.Logger

	// TransparentEgress and TransparentIngress are the iptables-redirected
	// listeners (design §5.2); nil disables each.
	TransparentEgress  *TransparentEgress
	TransparentIngress *TransparentIngress
	// OriginalDst resolves a redirected connection's original destination;
	// nil uses the platform implementation (SO_ORIGINAL_DST on linux).
	OriginalDst OriginalDstFunc
	// Policies is the armed toxic table raw-TCP relays consult; nil relays
	// without toxics.
	Policies *PolicyTable
}

func (c Config) originalDst() OriginalDstFunc {
	if c.OriginalDst != nil {
		return c.OriginalDst
	}
	return platformOriginalDst
}

func (c Config) base() http.RoundTripper {
	if c.Base != nil {
		return c.Base
	}
	return http.DefaultTransport
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}
