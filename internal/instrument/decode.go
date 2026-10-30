package instrument

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/cpu"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/disk"
	iostress "git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/io"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/memory"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

type decodeConfig struct {
	resolve      wire.NetworkResolver
	inlineToxic  InlineToxicHost
	resourceHost ResourceHost
	capabilities map[string]bool // nil = everything
}

// ResourceHost builds the fault that executes a resource-category fault on
// behalf of a host that must not run it itself: the atroposd sidecar
// delegates to the node agent (design §5.14). name is the rule or slot the
// fault belongs to; req carries the category, type, params and mechanism.
type ResourceHost func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error)

// WithResourceHost installs the resource host; every resource fault then
// decodes through it.
func WithResourceHost(h ResourceHost) DecodeOption {
	return func(c *decodeConfig) { c.resourceHost = h }
}

// resourceMechanism reads the optional mechanism param shared by the
// resource fault types.
func resourceMechanism(params json.RawMessage) string {
	var m struct {
		Mechanism string `json:"mechanism"`
	}
	_ = json.Unmarshal(params, &m)
	return m.Mechanism
}

// InlineToxicHost builds the fault that hosts a network toxic whose
// NetworkEnvelope.Host is "inline". A host that owns the proxied
// connections (the atroposd sidecar) supplies one; the in-process host does
// not, and inline network faults stay rejected there. name is the rule or
// slot the fault belongs to (its fault-registry key).
type InlineToxicHost func(name string, env *wire.NetworkEnvelope, toxic network.Toxic, base fault.FaultConfig) (fault.Fault, error)

// DecodeOption is the exported name of decodeOption, for hosts that supply
// decode hooks through CoreConfig.DecodeOptions.
type DecodeOption = decodeOption

// WithInlineToxicHost installs the inline network-toxic host.
func WithInlineToxicHost(h InlineToxicHost) DecodeOption {
	return func(c *decodeConfig) { c.inlineToxic = h }
}

// decodeOption configures the behaviour of decodeCompiledRule(s).
type decodeOption func(*decodeConfig)

// withNetworkResolver supplies the resolver that maps a logical target name
// (e.g. "redis") to a listen and upstream address pair for network fault
// proxies. Required when decoding rules whose Network.Host=="proxy".
func withNetworkResolver(r wire.NetworkResolver) decodeOption {
	return func(c *decodeConfig) { c.resolve = r }
}

func buildDecodeConfig(opts []decodeOption) *decodeConfig {
	cfg := &decodeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

// decodeCompiledRules converts wire-format CompiledRules into evaluator
// StaticRules that can be loaded into the host StaticEvaluator.
func decodeCompiledRules(compiled []wire.CompiledRule, opts ...decodeOption) ([]evaluator.StaticRule, error) {
	sorted := make([]wire.CompiledRule, len(compiled))
	copy(sorted, compiled)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority > sorted[j].Priority
	})

	rules := make([]evaluator.StaticRule, 0, len(sorted))
	for _, cr := range sorted {
		sr, err := decodeCompiledRule(cr, opts...)
		if err != nil {
			var unsup *UnsupportedFaultError
			if errors.As(err, &unsup) {
				// Skip this rule only: one rule this host cannot execute must
				// not wedge the whole desired state (D7: counted + logged).
				metrics.RulesUnsupportedTotal.WithLabelValues(unsup.Category).Inc()
				slog.Default().Warn("rule skipped: not executable by this host",
					"rule", cr.Name, "category", unsup.Category, "fault_type", unsup.FaultType)
				continue
			}
			return nil, fmt.Errorf("rule %q: %w", cr.Name, err)
		}
		rules = append(rules, sr)
	}
	return rules, nil
}

// decodeCompiledRule converts a single wire.CompiledRule into a wire.StaticRule.
func decodeCompiledRule(cr wire.CompiledRule, opts ...decodeOption) (evaluator.StaticRule, error) {
	cfg := buildDecodeConfig(opts)

	sr := evaluator.StaticRule{
		Name:   cr.Name,
		Point:  parseInjectionPoint(cr.InjectionPoint),
		Labels: cr.Labels,
	}

	sr.Decision.Mode = parseMode(cr.Mode)
	sr.Decision.Reason = "compiled"
	sr.Decision.Name = cr.Name
	sr.Decision.StartPolicy = parseStartPolicy(cr.StartPolicy)

	if cr.Composition != nil {
		// Carried on the wire, not executable here yet (v6: parallel /
		// sequential dispatch with direction inheritance). Skipped per rule
		// like an unsupported category, never failing the set.
		return evaluator.StaticRule{}, &UnsupportedFaultError{Category: CapComposition, FaultType: cr.Composition.ExecutionMode}
	}

	if cr.Fault != nil {
		f, err := decodeFault(cr.Name, cr.Fault, cfg)
		if err != nil {
			return evaluator.StaticRule{}, err
		}
		sr.Decision.Fault = f
	}

	if cr.CacheBox != nil {
		sr.Decision.CacheBox = parseCacheBoxMode(cr.CacheBox.Mode)
		sr.Decision.CacheBoxKeyStrategy = cr.CacheBox.KeyStrategy
		sr.Decision.CacheBoxContext = cr.CacheBox.Context
	}

	return sr, nil
}

func parseInjectionPoint(s string) evaluator.InjectionPoint {
	switch s {
	case "ingress":
		return evaluator.Ingress
	case "egress":
		return evaluator.Egress
	case "transient":
		return evaluator.Transient
	case "custom":
		return evaluator.Custom
	default:
		return evaluator.Ingress
	}
}

func parseMode(s string) evaluator.Mode {
	switch s {
	case "background":
		return evaluator.Background
	case "inline":
		return evaluator.Inline
	default:
		return evaluator.Inline
	}
}

func parseStartPolicy(s string) evaluator.StartPolicy {
	switch s {
	case "always_start":
		return evaluator.AlwaysStart
	default:
		return evaluator.DeduplicateByRule
	}
}

func decodeFault(name string, f *wire.CompiledFault, cfg *decodeConfig) (fault.Fault, error) {
	baseCfg := fault.FaultConfig{
		Duration: time.Duration(f.DurationMs) * time.Millisecond,
		RampUp:   time.Duration(f.RampUpMs) * time.Millisecond,
		RampDown: time.Duration(f.RampDownMs) * time.Millisecond,
	}

	// Absent params decode as an all-defaults empty object so types whose
	// fields are all optional (blackhole, error) need no params on the wire.
	params := f.Params
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}

	// Cross-category envelope checks.
	if f.Network != nil && f.EffectiveCategory() != "network" {
		return nil, fmt.Errorf("category %q must not carry a network envelope", f.EffectiveCategory())
	}
	if cfg.capabilities != nil && !cfg.capabilities[f.EffectiveCategory()] {
		return nil, &UnsupportedFaultError{Category: f.EffectiveCategory(), FaultType: f.FaultType}
	}

	switch f.EffectiveCategory() {
	case "inline":
		return decodeInlineFault(f.FaultType, params, baseCfg)
	case "network":
		if f.Network == nil {
			return nil, fmt.Errorf("network fault %q requires a network envelope", f.FaultType)
		}
		return decodeNetworkFault(name, f.FaultType, f.Network, params, baseCfg, cfg)
	case "resource":
		if cfg.resourceHost != nil {
			return cfg.resourceHost(name, f, baseCfg)
		}
		if resourceMechanism(params) == faultparams.MechanismQuota {
			// Lowering a cgroup limit needs the node agent's authority; an
			// in-process host has none.
			return nil, &UnsupportedFaultError{Category: "resource", FaultType: f.FaultType + "/quota"}
		}
		return decodeResourceFault(f.FaultType, params, baseCfg)
	default:
		return nil, fmt.Errorf("unknown fault category %q", f.Category)
	}
}

// decodeInlineFault dispatches by fault_type within the "inline" category.
// Param shapes are the exported faultparams structs — the decode contract
// and the control-plane validation schema are the same types by construction.
func decodeInlineFault(faultType string, params json.RawMessage, baseCfg fault.FaultConfig) (fault.Fault, error) {
	switch faultType {
	case "latency":
		var p faultparams.InlineLatency
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode latency params: %w", err)
		}
		delay, err := time.ParseDuration(p.Delay)
		if err != nil {
			return nil, fmt.Errorf("parse latency delay %q: %w", p.Delay, err)
		}
		var jitter time.Duration
		if p.Jitter != "" {
			jitter, err = time.ParseDuration(p.Jitter)
			if err != nil {
				return nil, fmt.Errorf("parse latency jitter %q: %w", p.Jitter, err)
			}
		}
		if baseCfg.Duration == 0 {
			baseCfg.Duration = delay + jitter
		}
		return &inline.Latency{
			FaultConfig: baseCfg,
			Delay:       delay,
			Jitter:      jitter,
		}, nil

	case "error":
		var p faultparams.InlineError
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode error params: %w", err)
		}
		if baseCfg.Duration == 0 {
			baseCfg.Duration = 1
		}
		// Defaults shared with the (former) admin path: a bare error fault
		// is a 500 "injected fault".
		if p.StatusCode == 0 {
			p.StatusCode = http.StatusInternalServerError
		}
		if p.Message == "" {
			p.Message = "injected fault"
		}
		return &inline.Error{
			FaultConfig: baseCfg,
			StatusCode:  p.StatusCode,
			Message:     p.Message,
		}, nil

	case "hang":
		var p faultparams.InlineHang
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode hang params: %w", err)
		}
		dur, err := time.ParseDuration(p.Duration)
		if err != nil {
			return nil, fmt.Errorf("parse hang duration %q: %w", p.Duration, err)
		}
		if baseCfg.Duration == 0 {
			baseCfg.Duration = dur
		}
		return &inline.Hang{
			FaultConfig: baseCfg,
		}, nil

	default:
		return nil, fmt.Errorf("unknown inline fault type %q", faultType)
	}
}

// DecodeResourceFault builds a resource-category fault from its wire
// parameters. Exported for the node agent's stress child, which runs the
// same implementations placed in the target container's cgroup.
func DecodeResourceFault(faultType string, params json.RawMessage, baseCfg fault.FaultConfig) (fault.Fault, error) {
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}
	return decodeResourceFault(faultType, params, baseCfg)
}

func decodeResourceFault(faultType string, params json.RawMessage, baseCfg fault.FaultConfig) (fault.Fault, error) {
	switch faultType {
	case "cpu":
		var p faultparams.ResourceCPU
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode cpu params: %w", err)
		}
		rc := resource.Config{
			FaultConfig: baseCfg,
			TargetLoad:  p.TargetLoad,
		}
		if p.Window != "" {
			w, err := time.ParseDuration(p.Window)
			if err != nil {
				return nil, fmt.Errorf("parse cpu window %q: %w", p.Window, err)
			}
			rc.Window = w
		}
		return &cpu.Stress{Config: rc}, nil

	case "memory":
		var p faultparams.ResourceMemory
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode memory params: %w", err)
		}
		return &memory.Stress{Config: memory.Config{
			FaultConfig:   baseCfg,
			TargetLoad:    p.TargetLoad,
			ChunkSize:     p.ChunkSize,
			Thrashing:     p.Thrashing,
			ThrashWorkers: p.ThrashWorkers,
		}}, nil

	case "disk":
		var p faultparams.ResourceDisk
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode disk params: %w", err)
		}
		dc := disk.Config{
			FaultConfig:  baseCfg,
			WriteRate:    p.WriteRate,
			MaxDiskUsage: p.MaxDiskUsage,
			ChunkSize:    p.ChunkSize,
			Path:         p.Path,
		}
		if dc.WriteRate == 0 {
			dc.WriteRate = disk.DefaultWriteRate
		}
		if dc.MaxDiskUsage == 0 {
			dc.MaxDiskUsage = disk.DefaultMaxDisk
		}
		return &disk.Stress{Config: dc}, nil

	case "io":
		var p faultparams.ResourceIO
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode io params: %w", err)
		}
		var ioMode iostress.IOMode
		switch p.Mode {
		case "write":
			ioMode = iostress.ModeWrite
		case "read_write":
			ioMode = iostress.ModeReadWrite
		default:
			ioMode = iostress.ModeRead
		}
		ic := iostress.Config{
			FaultConfig: baseCfg,
			ReadRate:    p.ReadRate,
			FileSize:    p.FileSize,
			FileCount:   p.FileCount,
			Workers:     p.Workers,
			Path:        p.Path,
			Mode:        ioMode,
		}
		if ic.ReadRate == 0 {
			ic.ReadRate = iostress.DefaultReadRate
		}
		if ic.FileSize == 0 {
			ic.FileSize = iostress.DefaultFileSize
		}
		if ic.FileCount == 0 {
			ic.FileCount = iostress.DefaultFileCount
		}
		if ic.Workers == 0 {
			ic.Workers = iostress.DefaultWorkers
		}
		return &iostress.Stress{Config: ic}, nil

	default:
		return nil, fmt.Errorf("unknown resource fault type %q", faultType)
	}
}

func decodeNetworkFault(name, faultType string, env *wire.NetworkEnvelope, params json.RawMessage, baseCfg fault.FaultConfig, cfg *decodeConfig) (fault.Fault, error) {
	host := env.Host
	if host == "" {
		host = "proxy"
	}

	switch host {
	case "proxy":
		return decodeNetworkProxyFault(faultType, env, params, baseCfg, cfg.resolve)
	case "inline":
		// The toxic runs on the proxied connection of a host that owns
		// one (the atroposd sidecar, via WithInlineToxicHost). The
		// in-process host has no such connection and rejects it.
		if cfg.inlineToxic == nil {
			return nil, fmt.Errorf(
				"network fault %q with host=%q: this host does not own proxied connections (in-process toxic host is not yet supported; use the atroposd sidecar)",
				faultType, host,
			)
		}
		toxic, err := decodeNetworkToxic(faultType, params)
		if err != nil {
			return nil, err
		}
		return cfg.inlineToxic(name, env, toxic, baseCfg)
	default:
		return nil, fmt.Errorf("network fault %q: unknown host %q", faultType, host)
	}
}

func decodeNetworkProxyFault(faultType string, env *wire.NetworkEnvelope, params json.RawMessage, baseCfg fault.FaultConfig, resolve wire.NetworkResolver) (fault.Fault, error) {
	if resolve == nil {
		return nil, fmt.Errorf("network fault %q with host=proxy requires a wire.NetworkResolver (use withNetworkResolver)", faultType)
	}
	if env.Target == "" {
		return nil, fmt.Errorf("network fault %q with host=proxy requires network.target", faultType)
	}

	listen, upstream, err := resolve(env.Target)
	if err != nil {
		return nil, fmt.Errorf("resolve network target %q: %w", env.Target, err)
	}

	dir := network.Upstream
	if env.Direction == "downstream" {
		dir = network.Downstream
	}

	toxic, err := decodeNetworkToxic(faultType, params)
	if err != nil {
		return nil, err
	}

	return &network.Proxy{Config: network.Config{
		FaultConfig: baseCfg,
		Listen:      listen,
		Upstream:    upstream,
		Scope:       env.Scope,
		Toxics:      []network.ToxicLink{{Toxic: toxic, Direction: dir}},
	}}, nil
}

func decodeNetworkToxic(faultType string, params json.RawMessage) (network.Toxic, error) {
	switch faultType {
	case "latency":
		var p faultparams.NetworkLatency
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode network latency params: %w", err)
		}
		delay, err := time.ParseDuration(p.Delay)
		if err != nil {
			return nil, fmt.Errorf("parse network latency delay %q: %w", p.Delay, err)
		}
		var jitter time.Duration
		if p.Jitter != "" {
			jitter, err = time.ParseDuration(p.Jitter)
			if err != nil {
				return nil, fmt.Errorf("parse network latency jitter %q: %w", p.Jitter, err)
			}
		}
		return &network.Latency{Delay: delay, Jitter: jitter}, nil

	case "retransmit_delay":
		var p faultparams.NetworkRetransmitDelay
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode network retransmit_delay params: %w", err)
		}
		r := &network.RetransmitDelay{Rate: p.Rate, ResetThreshold: p.ResetThreshold}
		if p.Delay != "" {
			d, err := time.ParseDuration(p.Delay)
			if err != nil {
				return nil, fmt.Errorf("parse network retransmit_delay delay %q: %w", p.Delay, err)
			}
			r.Delay = d
		}
		return r, nil

	case "blackhole":
		return &network.Blackhole{}, nil

	case "drip":
		var p faultparams.NetworkDrip
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode network drip params: %w", err)
		}
		d := &network.Drip{ChunkSize: p.ChunkSize}
		if p.Interval != "" {
			iv, err := time.ParseDuration(p.Interval)
			if err != nil {
				return nil, fmt.Errorf("parse network drip interval %q: %w", p.Interval, err)
			}
			d.Interval = iv
		}
		return d, nil

	case "rst":
		var p faultparams.NetworkRST
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode network rst params: %w", err)
		}
		r := &network.RST{AfterBytes: p.AfterBytes}
		if p.AfterDuration != "" {
			d, err := time.ParseDuration(p.AfterDuration)
			if err != nil {
				return nil, fmt.Errorf("parse network rst after_duration %q: %w", p.AfterDuration, err)
			}
			r.AfterDuration = d
		}
		return r, nil

	case "throttle":
		var p faultparams.NetworkThrottle
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("decode network throttle params: %w", err)
		}
		return &network.Throttle{BytesPerSec: p.BytesPerSec}, nil

	default:
		return nil, fmt.Errorf("unknown network fault type %q", faultType)
	}
}

func parseCacheBoxMode(s string) evaluator.CacheBoxAction {
	switch s {
	case "passthrough":
		return evaluator.CacheBoxPassthrough
	case "replay":
		return evaluator.CacheBoxReplay
	case "replay_with_delay":
		return evaluator.CacheBoxReplayDelay
	default:
		return evaluator.CacheBoxNone
	}
}
