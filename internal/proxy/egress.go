package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httputil"

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

// newErrorHandler reports upstream failures as 502 with the error text.
// Task 5 extends it to reproduce RST/blackhole toxics on the client leg.
func newErrorHandler(logger *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		logger.Warn("atroposd: upstream error", "method", r.Method, "host", r.URL.Host, "path", r.URL.Path, "error", err)
		http.Error(w, "atroposd: upstream error: "+err.Error(), http.StatusBadGateway)
	}
}
