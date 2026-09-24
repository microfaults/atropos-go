package instrument

import (
	"log/slog"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// CoreConfig is the input to NewCore.
type CoreConfig struct {
	// Service and Instance are the identity every component stamps: push
	// envelopes, drain reports, the fidelity endpoint.
	Service  string
	Instance string
	// ManteionURL, when non-empty, builds the cache push client and the
	// drain tracker. Empty = offline: recording still counts into the
	// fidelity registry, nothing is pushed.
	ManteionURL string
	// Registry, if set, is adopted as the process-lifetime fault registry;
	// nil builds a fresh one. The in-process host passes its package-level
	// registry so reconfiguration never orphans running faults (A1).
	Registry *interceptor.FaultRegistry
	// InjectionHook defaults to metrics.RecordFaultInjection.
	InjectionHook interceptor.InjectionHook
	Logger        *slog.Logger
}

// Core is the instrument: one shared fidelity registry, the cache-box and
// its push client, the host rule evaluator composed before the admin fault
// slot (host rules always win), the fault registry, the interceptor the
// middleware layers dispatch through, and the drain tracker. Every host of
// the SDK (the in-process Serve embed, the atroposd sidecar) builds exactly
// one Core.
type Core struct {
	service  string
	instance string

	fidelity *cachebox.FidelityRegistry
	cacheBox *cachebox.CacheBox
	push     *cachePushClient // nil when offline
	host     *evaluator.StaticEvaluator
	demo     *demoEvaluator
	registry *interceptor.FaultRegistry
	ic       *interceptor.Interceptor
	drain    *cacheDrainTracker // nil when offline
	logger   *slog.Logger
}

// NewCore wires the instrument. The construction order is what the
// hand-wired embeds used to get wrong: the fidelity registry exists first
// and is handed to both the push client and the cache-box, so record-side
// and push-side counts land in the one registry the drain report and the
// fidelity endpoint snapshot.
func NewCore(cfg CoreConfig) *Core {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	hook := cfg.InjectionHook
	if hook == nil {
		hook = metrics.RecordFaultInjection
	}
	reg := cfg.Registry
	if reg == nil {
		reg = interceptor.NewFaultRegistry()
	}

	fidelity := cachebox.NewFidelityRegistry()
	var push *cachePushClient
	cbCfg := cachebox.Config{Fidelity: fidelity}
	if cfg.ManteionURL != "" {
		push = newCachePushClient(cachePushConfig{
			BaseURL:  cfg.ManteionURL,
			Service:  cfg.Service,
			Instance: cfg.Instance,
			Logger:   logger,
			Fidelity: fidelity,
		})
		cbCfg.Push = push.PushFunc()
	}
	cb := cachebox.New(cbCfg)

	// Host-first composition: an armed admin fault can fill gaps but never
	// shadow an experiment's rules (invariant 7).
	host := evaluator.NewStaticEvaluator()
	demo := &demoEvaluator{}
	ic := interceptor.New(evaluator.NewMultiEvaluator(host, demo), trace.NewOTelTracer(),
		interceptor.WithInjectionHook(hook),
		interceptor.WithRegistry(reg),
		interceptor.WithCacheBox(cb),
	)

	c := &Core{
		service: cfg.Service, instance: cfg.Instance,
		fidelity: fidelity, cacheBox: cb, push: push,
		host: host, demo: demo, registry: reg, ic: ic, logger: logger,
	}
	if push != nil {
		c.drain = newCacheDrainTracker(cb, push, logger)
	}
	return c
}

// Interceptor is the policy dispatcher hosts route ingress/egress through.
func (c *Core) Interceptor() *interceptor.Interceptor { return c.ic }

// Service and Instance return the identity the core was built with.
func (c *Core) Service() string  { return c.service }
func (c *Core) Instance() string { return c.instance }

// targets is the applyTargets the control plane mutates for this core.
func (c *Core) targets(resolver wire.NetworkResolver) applyTargets {
	return applyTargets{
		Evaluator:       c.host,
		DemoEval:        c.demo,
		CacheBox:        c.cacheBox,
		NetworkResolver: resolver,
		CacheDrain:      c.drain,
		Registry:        c.registry,
	}
}

// Stop drains the recorder into the push client, then flushes the push
// client's final batch. Order matters; idempotent.
func (c *Core) Stop() {
	c.cacheBox.Stop()
	if c.push != nil {
		c.push.Stop()
	}
}
