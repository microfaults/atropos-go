package proxy

import (
	"net/http"
	"net/http/httputil"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// newIngressHandler builds the handler for the ingress listener: the exact
// in-process ingress composition (metrics → otelhttp server span → fault
// check) around a reverse proxy to the app. The hop to the app carries the
// sidecar's server span context, so an app with its own OTel SDK nests its
// server span under the sidecar's (design §5.7); the Host header is
// preserved.
func newIngressHandler(cfg Config, l IngressListener) http.Handler {
	propagator := otel.GetTextMapPropagator()
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = l.Upstream
			pr.Out.Host = pr.In.Host
			propagator.Inject(pr.In.Context(), propagation.HeaderCarrier(pr.Out.Header))
		},
		Transport:    timedTransport{http.DefaultTransport},
		ErrorHandler: newErrorHandler(cfg.logger()),
	}
	return observe("ingress", instrument.IngressMiddleware(rp, cfg.Service, cfg.Resolve()))
}
