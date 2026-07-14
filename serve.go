package atropos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"

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
	// a (listen, upstream) proxy address pair. Required only to accept
	// network-category faults; nil rejects them at decode time.
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
	instanceID := resolveInstanceID(cfg)

	// Telemetry first: everything below emits spans through the global
	// tracer provider this installs.
	initOpts := []Option{
		WithServiceName(cfg.Service),
		WithServiceVersion(version),
	}
	if cfg.Environment != "" {
		initOpts = append(initOpts, WithEnvironment(cfg.Environment))
	}
	if cfg.TracerProvider != nil {
		initOpts = append(initOpts, WithTracerProvider(cfg.TracerProvider))
	}
	otelShutdown, err := Init(ctx, initOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("atropos: init telemetry: %w", err)
	}

	// One fidelity registry, handed to both the push client and the
	// cache-box at construction: record-side and push-side counts must land
	// in the single registry the drain report and fidelity endpoint
	// snapshot. Constructing both around it removes the order-sensitive
	// bind step the hand-wired embeds needed.
	fidelity := cachebox.NewFidelityRegistry()
	var push *CachePushClient
	cbCfg := CacheBoxConfig{Fidelity: fidelity}
	if manteionURL != "" {
		push = NewCachePushClient(CachePushConfig{
			BaseURL:  manteionURL,
			Service:  cfg.Service,
			Instance: instanceID,
			Logger:   logger,
			Fidelity: fidelity,
		})
		cbCfg.Push = push.PushFunc()
	}
	cb := NewCacheBox(cbCfg)

	// The host evaluator receives manteion's compiled rules; the demo
	// evaluator holds admin/manual fault slots. Composed host-first: an
	// armed admin fault can fill gaps but never shadow an experiment's
	// rules — a demo fault that outranked a recording or freeze rule would
	// silently corrupt the phase's measurement (invariant 7).
	host := NewStaticEvaluator()
	demo := &DemoEvaluator{}
	Configure(
		WithEvaluator(NewMultiEvaluator(host, demo)),
		WithCacheBoxCoordinator(cb),
	)

	targets := ApplyTargets{
		Evaluator:       host,
		DemoEval:        demo,
		CacheBox:        cb,
		NetworkResolver: cfg.NetworkResolver,
	}
	if push != nil {
		targets.CacheDrain = NewCacheDrainTracker(cb, push, logger)
	}

	RegisterRoutes(cfg.Routes...)

	connectOpts := []ManteionOption{
		WithInstanceID(instanceID),
		WithApplyTargets(targets),
		WithManteionServiceVersion(version),
		WithLogger(logger),
	}
	if cfg.ManteionURL != "" {
		connectOpts = append(connectOpts, WithManteionURL(cfg.ManteionURL))
	}
	mc, err := ConnectManteion(ctx, cfg.Service, connectOpts...)
	if err != nil {
		// Fail closed rather than run silently offline with a configured
		// control plane. Unwind what was built.
		cb.Stop()
		if push != nil {
			push.Stop()
		}
		_ = otelShutdown(ctx)
		return nil, nil, fmt.Errorf("atropos: connect manteion: %w", err)
	}

	handler := controlMux(cfg, host, demo, cb, instanceID)

	var once sync.Once
	var shutdownErr error
	shutdown := func(sctx context.Context) error {
		once.Do(func() {
			// Order matters: stop syncing rules first; then drain the
			// recorder INTO the push client; then flush the push client's
			// final batch; then flush spans.
			_ = mc.Close(sctx) // nil-safe in offline mode
			cb.Stop()
			if push != nil {
				push.Stop()
			}
			globalClient.CompareAndSwap(mc, nil)
			shutdownErr = otelShutdown(sctx)
		})
		return shutdownErr
	}
	return handler, shutdown, nil
}

// resolveInstanceID picks the identity manteion correlates this SDK
// instance under. It MUST be identical across the register call, the
// cache-push envelopes/drain reports, and the fidelity endpoint, so it is
// resolved exactly once, in Serve, and threaded to all three. Precedence:
// Config.InstanceID > MANTEION_INSTANCE_ID > hostname (the pod name in k8s)
// > service name.
func resolveInstanceID(cfg Config) string {
	if cfg.InstanceID != "" {
		return cfg.InstanceID
	}
	if id := os.Getenv("MANTEION_INSTANCE_ID"); id != "" {
		return id
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return cfg.Service
}

// controlMux mounts the control/observability surface at the exact paths
// manteion addresses, with the business handler as the middleware-wrapped
// fallback. Control paths deliberately bypass the fault/OTel middleware.
func controlMux(cfg Config, host *StaticEvaluator, demo *DemoEvaluator, cb *CacheBox, instanceID string) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", MetricsHandler())
	mux.Handle("/atropos/health", HealthHandler())

	// Bare path and subtree both: the subtree pattern alone would
	// 301-redirect the bare path and drop freeze/thaw verbs.
	faultAdmin := FaultAdminHandlerWith(demo, cfg.NetworkResolver)
	mux.Handle("/admin/fault", faultAdmin)
	mux.Handle("/admin/fault/", faultAdmin)

	var decodeOpts []DecodeOption
	if cfg.NetworkResolver != nil {
		decodeOpts = append(decodeOpts, WithNetworkResolver(cfg.NetworkResolver))
	}
	mux.Handle("/admin/rules", RulesAdminHandler(host, decodeOpts...))

	cbAdmin := CacheBoxAdminHandler(cb)
	mux.Handle("/admin/cachebox", cbAdmin)
	mux.Handle("/admin/cachebox/", cbAdmin)
	mux.Handle("/cachebox/preload/", CacheBoxPreloadHandler(cb))
	mux.Handle("GET /cachebox/fidelity", CacheBoxFidelityHandler(cb, cfg.Service, instanceID))

	if cfg.Handler != nil {
		mux.Handle("/", IngressMiddleware(cfg.Handler, cfg.Service))
	}
	return mux
}
