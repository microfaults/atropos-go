package atroposd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/proxy"
)

// Addrs are the bound addresses Run reports through Config.OnReady.
type Addrs struct {
	Control string
	Ingress string   // explicit ingress listener, "" when none
	Egress  []string // explicit egress listeners, Config.Egress order

	TransparentEgress  string // "" unless capture is transparent
	TransparentIngress string // "" unless transparent with inbound ports
}

// checkCapture refuses to start a transparent sidecar whose redirect is not
// in place (design §5.2, "no capture, no instance"): the init container
// writes the marker after installing the iptables rules. A test hook for
// the original-destination lookup stands in for the redirect.
func checkCapture(cfg Config) error {
	if cfg.Capture != CaptureTransparent || cfg.OriginalDst != nil {
		return nil
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("transparent capture requires linux (SO_ORIGINAL_DST), running on %s", runtime.GOOS)
	}
	if _, err := os.Stat(cfg.CaptureMarker); err != nil {
		return fmt.Errorf("transparent capture is not in place: marker %s: %w (the init container writes it after installing the iptables REDIRECT; refusing to register a half-instrumented pod)", cfg.CaptureMarker, err)
	}
	return nil
}

// Run hosts the instrument out of process until ctx is cancelled, then
// tears it down in dependency order: proxied traffic drains, the control
// server stops, the control-plane client deregisters, the core flushes its
// recorder and push client, spans flush.
//
// Order at startup is deliberate: the control port is bound before anything
// else (a port conflict must fail before the instance registers, and the
// advertised address must be live once it has), the proxy listeners next
// (same reason), and the control plane last, so a configured-but-
// unreachable manteion is a startup error with nothing half-running — an
// instrument that pretends to be connected corrupts experiments.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("atroposd: config: %w", err)
	}
	if err := checkCapture(cfg); err != nil {
		return fmt.Errorf("atroposd: %w", err)
	}
	instanceID := instrument.ResolveInstanceID(cfg.InstanceID, cfg.Service)

	otelShutdown, err := instrument.InitTelemetry(ctx, instrument.TelemetryConfig{
		ServiceName:    cfg.Service,
		ServiceVersion: cfg.Version,
		Environment:    cfg.Environment,
		TracerProvider: cfg.TracerProvider,
	})
	if err != nil {
		return fmt.Errorf("atroposd: init telemetry: %w", err)
	}

	// The core, with the two sidecar-only behaviours: network toxics with
	// host=inline become armed proxy policies, and background faults arm
	// when their rule applies rather than on the first matching request.
	table := proxy.NewPolicyTable()
	core := instrument.NewCore(instrument.CoreConfig{
		Service:       cfg.Service,
		Instance:      instanceID,
		ManteionURL:   cfg.ManteionURL,
		Logger:        logger,
		EagerArm:      true,
		DecodeOptions: []instrument.DecodeOption{instrument.WithInlineToxicHost(proxy.InlineToxicFactory(table))},
		Host:          instrument.HostSidecar,
		// Resource faults are never executed in the sidecar (design §5.6);
		// the node agent (M4) adds CapResource when its socket is present.
		Capabilities: []string{instrument.CapInline, instrument.CapNetwork, instrument.CapCacheBox},
	})

	controlLn, err := net.Listen("tcp", cfg.ControlListen)
	if err != nil {
		core.Stop()
		_ = otelShutdown(ctx)
		return fmt.Errorf("atroposd: control listen %s: %w", cfg.ControlListen, err)
	}

	pcfg := proxy.Config{
		Service:  cfg.Service,
		Resolve:  core.Interceptor,
		Base:     &proxy.ToxicTransport{Table: table},
		Policies: table,
		Logger:   logger,
	}
	switch cfg.Capture {
	case CaptureTransparent:
		pcfg.Base = &proxy.ToxicTransport{Table: table, Base: proxy.NewTransparentTransport()}
		pcfg.OriginalDst = cfg.OriginalDst
		pcfg.TransparentEgress = &proxy.TransparentEgress{Listen: cfg.EgressListen, RawTCPPorts: cfg.RawTCPPorts}
		if len(cfg.InboundPorts) > 0 {
			pcfg.TransparentIngress = &proxy.TransparentIngress{Listen: cfg.IngressListen, AppHost: cfg.AppHost, AllowedPorts: cfg.InboundPorts}
		}
	default:
		for _, e := range cfg.Egress {
			pcfg.Egress = append(pcfg.Egress, proxy.EgressListener{Listen: e.Listen, Upstream: e.Upstream})
		}
		if cfg.IngressListen != "" {
			pcfg.Ingress = &proxy.IngressListener{Listen: cfg.IngressListen, Upstream: cfg.InboundUpstream}
		}
	}
	p := proxy.New(pcfg)
	if err := p.Start(ctx); err != nil {
		controlLn.Close()
		core.Stop()
		_ = otelShutdown(ctx)
		return fmt.Errorf("atroposd: %w", err)
	}

	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		advertise = instrument.DefaultAdvertiseAddr(controlLn.Addr().String())
	}
	mc, err := instrument.ConnectManteion(ctx, instrument.ConnectConfig{
		Core:           core,
		URL:            cfg.ManteionURL,
		ServiceVersion: cfg.Version,
		Routes:         cfg.wireRoutes(),
		Address:        advertise,
		Logger:         logger,
	})
	if err != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(sctx)
		controlLn.Close()
		core.Stop()
		_ = otelShutdown(sctx)
		return fmt.Errorf("atroposd: connect manteion: %w", err)
	}

	controlSrv := &http.Server{
		Handler:           instrument.ControlSurface(instrument.ControlConfig{Core: core, Client: mc, Host: instrument.HostSidecar, Capture: cfg.Capture}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := controlSrv.Serve(controlLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("atroposd: control listener stopped", "error", err)
		}
	}()

	addrs := Addrs{
		Control: controlLn.Addr().String(), Ingress: p.IngressAddr(), Egress: p.EgressAddrs(),
		TransparentEgress: p.TransparentEgressAddr(), TransparentIngress: p.TransparentIngressAddr(),
	}
	logger.Info("atroposd ready",
		"service", cfg.Service, "instance", instanceID, "capture", cfg.Capture,
		"control", addrs.Control, "ingress", addrs.Ingress, "egress", addrs.Egress,
		"transparent_egress", addrs.TransparentEgress, "transparent_ingress", addrs.TransparentIngress,
		"manteion", cfg.ManteionURL, "advertise", advertise)
	if cfg.OnReady != nil {
		cfg.OnReady(addrs)
	}

	<-ctx.Done()

	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var errs []error
	errs = append(errs, p.Shutdown(sctx))          // drain proxied requests
	errs = append(errs, controlSrv.Shutdown(sctx)) // then stop answering the control plane
	errs = append(errs, mc.Close(sctx))            // deregister, stop polling (nil-safe offline)
	core.Stop()                                    // drain recorder into push, final flush
	errs = append(errs, otelShutdown(sctx))        // flush spans
	return errors.Join(errs...)
}
