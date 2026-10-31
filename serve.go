package atropos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// Config is the input to Serve — everything a service supplies to embed the
// instrument. Only Service (and, for a service with HTTP routes, Handler)
// needs to be set; every other field has an env-derived or safe default so
// the embed is offline-safe out of the box.
type Config struct {
	// Service is the logical service name. Required. It becomes the OTel
	// service.name resource attribute, the manteion registration identity,
	// and the label on ingress metrics.
	Service string

	// Version is the service version reported both to OTel
	// (service.version) and to manteion for experiment attribution.
	// Default: MANTEION_SERVICE_VERSION.
	Version string

	// Environment is the OTel deployment.environment resource attribute.
	// Default: "development".
	Environment string

	// Routes is the HTTP route inventory published to manteion at
	// registration time for the workflow-builder catalog. Optional —
	// a service without routes (or offline) registers without them.
	Routes []Route

	// Handler serves the service's own routes. Every request that does not
	// match a control/observability path is passed through the fault/OTel
	// ingress middleware and then to this handler. A nil Handler serves
	// only the control surface (e.g. a gRPC service mounting the returned
	// handler on an admin port).
	Handler http.Handler

	// ManteionURL is the control-plane base URL. Default: MANTEION_URL.
	// Empty (both here and in the env) means offline mode: tracing, faults
	// armed via /admin/*, and the cache-box endpoints all still work; there
	// is just no control plane to sync with. A non-empty URL that cannot be
	// reached within the init timeout is an error — an instrument that
	// silently runs offline while the experimenter believes it is connected
	// corrupts experiments.
	ManteionURL string

	// InstanceID overrides the SDK instance identity. Default precedence:
	// MANTEION_INSTANCE_ID > hostname > Service. The resolved identity is
	// used verbatim for manteion registration, cache pushes, and the
	// fidelity endpoint — the three places manteion correlates an instance.
	InstanceID string

	// NetworkResolver maps a logical network-fault target (e.g. "redis") to
	// a (listen, upstream) proxy address pair. Required only to run
	// network-category faults (host=proxy); with nil such rules are skipped
	// per rule, logged and counted in atropos_rules_unsupported_total.
	NetworkResolver NetworkResolver

	// TracerProvider, if set, is registered globally instead of building an
	// OTLP exporter — the seam for tests and for hosts that own their OTel
	// setup. Default: an OTLP exporter per OTEL_EXPORTER_OTLP_ENDPOINT /
	// COLLECTOR_SERVICE_ADDR (falling back to localhost).
	TracerProvider oteltrace.TracerProvider

	// Logger receives SDK logs. Default: slog.Default().
	Logger *slog.Logger
}

// ShutdownFunc tears the embed down: control-plane deregistration, recorder
// drain, final cache-push flush, and OTel span flush, in that order. Safe to
// call more than once; calls after the first are no-ops returning the first
// call's error.
type ShutdownFunc func(context.Context) error

// Serve wires the complete instrument around the service's handler and
// returns the composed handler to listen on:
//
//	h, shutdown, err := atropos.Serve(ctx, atropos.Config{
//	    Service: "productcatalogservice",
//	    Version: "0.1.0",
//	    Routes:  []atropos.Route{{Method: "GET", Path: "/products"}},
//	    Handler: businessMux,
//	})
//	if err != nil { log.Fatal(err) }
//	defer shutdown(ctx)
//	http.ListenAndServe(":"+port, h)
//
// Inside, in order: OTel init; one instance identity resolved once and used
// for registration, cache pushes, and the fidelity endpoint; the
// record/replay cache-box and its push client bound to one fidelity
// registry; the host rule evaluator composed with the admin fault slot (host
// rules always win); the manteion connection (poll + SSE + drain tracking),
// skipped cleanly when no URL is configured; and the control surface:
//
//	GET  /metrics                    Prometheus metrics
//	     /atropos/health             SDK health/connectivity
//	     /admin/fault[/{id}]         arm/inspect/clear admin faults
//	     /admin/rules                inspect/replace the compiled rule set
//	     /admin/cachebox[/delay]     cache-box stats / freeze delay / clear
//	POST /cachebox/preload/*         staged replay-set preload (W4)
//	GET  /cachebox/fidelity          per-phase fidelity counters (W6)
//
// Control paths are served outside the fault/OTel middleware: a fault rule
// can never delay or error the instrument's own control and observability
// plane. Everything else flows through the middleware into cfg.Handler.
//
// Serve is init-time API: call it once per process, before traffic flows.
func Serve(ctx context.Context, cfg Config) (http.Handler, ShutdownFunc, error) {
	if cfg.Service == "" {
		return nil, nil, errors.New("atropos: Config.Service is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	version := cfg.Version
	if version == "" {
		version = os.Getenv("MANTEION_SERVICE_VERSION")
	}
	manteionURL := cfg.ManteionURL
	if manteionURL == "" {
		manteionURL = os.Getenv("MANTEION_URL")
	}
	instanceID := instrument.ResolveInstanceID(cfg.InstanceID, cfg.Service)

	// Telemetry first: everything below emits spans through the global
	// tracer provider this installs.
	otelShutdown, err := instrument.InitTelemetry(ctx, instrument.TelemetryConfig{
		ServiceName:    cfg.Service,
		ServiceVersion: version,
		Environment:    cfg.Environment,
		TracerProvider: cfg.TracerProvider,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("atropos: init telemetry: %w", err)
	}

	// Mixed-fleet guard (design §5.12): a pod is instrumented by exactly one
	// host. When the pod's atroposd sidecar owns the instrument, the embed
	// stays passive — tracing only, no registration, no rules, no control
	// surface — so egress is not evaluated, spanned and recorded twice.
	if os.Getenv("ATROPOS_HOST") == instrument.HostSidecar {
		logger.Info("atropos: ATROPOS_HOST=sidecar — in-process instrument passive (tracing only); the sidecar registers, evaluates rules and serves the control surface",
			"service", cfg.Service, "instance", instanceID)
		var h http.Handler = http.NotFoundHandler()
		if cfg.Handler != nil {
			h = ingressMiddleware(cfg.Handler, cfg.Service, currentInterceptor())
		}
		var once sync.Once
		var shutdownErr error
		return h, func(sctx context.Context) error {
			once.Do(func() { shutdownErr = otelShutdown(sctx) })
			return shutdownErr
		}, nil
	}

	// The instrument core: fidelity registry, cache-box + push, host
	// evaluator composed before the admin slot, the process-lifetime fault
	// registry (never swapped, A1), the interceptor, the drain tracker.
	core := instrument.NewCore(instrument.CoreConfig{
		Service:      cfg.Service,
		Instance:     instanceID,
		ManteionURL:  manteionURL,
		Registry:     defaultRegistry,
		Logger:       logger,
		Host:         instrument.HostInProcess,
		Capabilities: instrument.AllCapabilities(),
	})
	// Install the core's interceptor as the package-level dispatcher the
	// middleware, EgressTransport and the grpc subpackage resolve.
	defaultInterceptor.Store(core.Interceptor())

	mc, err := instrument.ConnectManteion(ctx, instrument.ConnectConfig{
		Core:            core,
		URL:             manteionURL,
		ServiceVersion:  version,
		Routes:          cfg.Routes,
		NetworkResolver: cfg.NetworkResolver,
		Logger:          logger,
	})
	if err != nil {
		// Fail closed rather than run silently offline with a configured
		// control plane. Unwind what was built.
		core.Stop()
		_ = otelShutdown(ctx)
		return nil, nil, fmt.Errorf("atropos: connect manteion: %w", err)
	}
	globalClient.Store(mc)

	mux := instrument.ControlSurface(instrument.ControlConfig{
		Core:            core,
		Client:          mc,
		NetworkResolver: cfg.NetworkResolver,
		Host:            instrument.HostInProcess,
	})
	if cfg.Handler != nil {
		mux.Handle("/", ingressMiddleware(cfg.Handler, cfg.Service, currentInterceptor()))
	}

	var once sync.Once
	var shutdownErr error
	shutdown := func(sctx context.Context) error {
		once.Do(func() {
			// Order matters: stop syncing rules first; then drain the
			// recorder INTO the push client; then flush the push client's
			// final batch; then flush spans.
			_ = mc.Close(sctx) // nil-safe in offline mode
			core.Stop()
			globalClient.CompareAndSwap(mc, nil)
			shutdownErr = otelShutdown(sctx)
		})
		return shutdownErr
	}
	return mux, shutdown, nil
}
