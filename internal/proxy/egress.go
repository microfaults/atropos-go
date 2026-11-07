package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"sync/atomic"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// newEgressHandler builds the handler for one explicit-mode egress listener.
//
// The app's outbound request arrives here as a server-side request (origin
// form: empty URL.Host, Host = the loopback address it dialed). The Rewrite
// rebuilds the client-side view the in-process host keys and evaluates:
// scheme, URL.Host and Host all set to the real upstream (design §5.2,
// §5.5). Trace context and baggage are extracted from the incoming headers
// before the reverse proxy clones the request, so the otelhttp client span
// inside instrument.EgressTransport is a child of the app's span and the
// atropos.workflow baggage member reaches the evaluator labels exactly as
// it does in-process.
func newEgressHandler(cfg Config, l EgressListener) http.Handler {
	transport := instrument.EgressTransport(timedTransport{cfg.base()}, cfg.Resolve)
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = l.Upstream
			pr.Out.Host = l.Upstream
			// Go's http.Transport adds "Accept-Encoding: gzip" on the wire
			// when the app's request had none (and transparently
			// decompresses the response). In-process that header is never
			// in r.Header, so canonical_v2 never folds it into the key;
			// here it arrives as a real header. Drop exactly that
			// auto-added value so the key (and the recorded, decompressed
			// body) match the in-process host. An app that sets a
			// different Accept-Encoding itself keeps it.
			if pr.Out.Header.Get("Accept-Encoding") == "gzip" {
				pr.Out.Header.Del("Accept-Encoding")
			}
		},
		Transport:    transport,
		ErrorHandler: newErrorHandler(cfg.logger()),
	}
	propagator := otel.GetTextMapPropagator()
	return observe("egress", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		rp.ServeHTTP(w, r.WithContext(ctx))
	}))
}

// newErrorHandler reports upstream failures as 502 with the error text,
// except a *HijackError, which it reproduces on the app-facing connection.
func newErrorHandler(logger *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		var hj *HijackError
		if errors.As(err, &hj) {
			reproduce(w, hj, logger)
			return
		}
		if errors.Is(err, context.Canceled) {
			return // the client went away; nothing to answer
		}
		logger.WarnContext(r.Context(), "atroposd: upstream error", "method", r.Method, "host", r.URL.Host, "path", r.URL.Path, "error", err)
		http.Error(w, "atroposd: upstream error: "+err.Error(), http.StatusBadGateway)
	}
}

// reproduce plays a connection-level toxic on the app-facing connection, so
// what the app observes is what a real network fault looks like, not an
// HTTP error the proxy invented (design §5.6).
func reproduce(w http.ResponseWriter, hj *HijackError, logger *slog.Logger) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		logger.Warn("atroposd: cannot hijack client connection for toxic", "policy", hj.Policy, "error", err)
		http.Error(w, "atroposd: toxic "+hj.Policy+" (not reproducible on this connection)", http.StatusBadGateway)
		return
	}
	defer conn.Close()

	// gone closes when the client hangs up; reading also drains anything
	// the client keeps sending.
	gone := make(chan struct{})
	go func() {
		io.Copy(io.Discard, conn)
		close(gone)
	}()

	switch hj.Mode {
	case HijackBlackhole:
		select {
		case <-hj.Hold.Done():
		case <-gone:
		}
	case HijackRST:
		defer hj.Resp.Body.Close()
		reset := func() {
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.SetLinger(0) // RST instead of FIN
			}
			conn.Close()
		}
		if hj.AfterBytes <= 0 && hj.AfterDuration <= 0 {
			reset()
			return
		}
		cw := &rstWriter{w: conn, limit: hj.AfterBytes}
		var timer <-chan time.Time
		if hj.AfterDuration > 0 {
			t := time.NewTimer(hj.AfterDuration)
			defer t.Stop()
			timer = t.C
			go func() {
				select {
				case <-timer:
					cw.trip()
				case <-gone:
				}
			}()
		}
		_ = hj.Resp.Write(cw)
		if cw.tripped.Load() {
			reset()
			return
		}
		if hj.AfterDuration > 0 {
			// The whole response fit before the clock ran out: hold the
			// connection, as the TCP proxy would, and reset it on time.
			select {
			case <-timer:
				reset()
			case <-gone:
			}
		}
	}
}

// rstWriter forwards up to limit bytes (0 = unlimited) and reports errRST
// once the limit is reached or trip was called.
type rstWriter struct {
	w       io.Writer
	limit   int64
	written int64
	tripped atomic.Bool
}

var errRST = errors.New("rst: triggered")

func (c *rstWriter) trip() { c.tripped.Store(true) }

func (c *rstWriter) Write(p []byte) (int, error) {
	if c.tripped.Load() {
		return 0, errRST
	}
	n := len(p)
	if c.limit > 0 && c.written+int64(n) > c.limit {
		n = int(c.limit - c.written)
	}
	if n > 0 {
		if _, err := c.w.Write(p[:n]); err != nil {
			return 0, err
		}
		c.written += int64(n)
	}
	if c.limit > 0 && c.written >= c.limit {
		c.trip()
		return n, errRST
	}
	if n < len(p) {
		return n, errRST
	}
	return n, nil
}
