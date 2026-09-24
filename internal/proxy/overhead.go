package proxy

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
)

// overhead accumulates the upstream round-trip time of one proxied request
// so the handler can report proxy overhead = total − upstream (G-proxy).
type overhead struct{ upstreamNs atomic.Int64 }

func (o *overhead) upstream() time.Duration { return time.Duration(o.upstreamNs.Load()) }

type overheadKey struct{}

func withOverhead(ctx context.Context, o *overhead) context.Context {
	return context.WithValue(ctx, overheadKey{}, o)
}

func overheadFrom(ctx context.Context) *overhead {
	o, _ := ctx.Value(overheadKey{}).(*overhead)
	return o
}

// timedTransport is the innermost transport: it times the real upstream
// round trip and credits it to the request's overhead holder, so everything
// the proxy itself adds (evaluation, keying, buffering, copying) is what
// remains.
type timedTransport struct{ base http.RoundTripper }

func (t timedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.base.RoundTrip(r)
	if o := overheadFrom(r.Context()); o != nil {
		o.upstreamNs.Add(int64(time.Since(start)))
	}
	return resp, err
}

// observe wraps h with the inflight gauge and the overhead histogram for
// point ("egress" | "ingress"). The overhead holder it installs in the
// context is what timedTransport credits upstream time to.
func observe(point string, h http.Handler) http.Handler {
	inflight := metrics.ProxyInflight.WithLabelValues(point)
	hist := metrics.ProxyOverhead.WithLabelValues(point)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o := &overhead{}
		inflight.Inc()
		defer inflight.Dec()
		start := time.Now()
		h.ServeHTTP(w, r.WithContext(withOverhead(r.Context(), o)))
		d := time.Since(start) - o.upstream()
		if d < 0 {
			d = 0
		}
		hist.Observe(d.Seconds())
	})
}
