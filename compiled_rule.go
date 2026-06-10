package atropos

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/cpu"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/disk"
	iostress "git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/io"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/memory"
)

// CompiledRule is the wire format received from manteion's poll/register
// endpoints. Manteion resolves FaultSpec references and inlines config;
// the SDK decodes and constructs evaluator rules locally.
//
// Exactly one of Fault, Composition, or CacheBox must be set.
type CompiledRule struct {
	Name           string               `json:"name"`
	InjectionPoint string               `json:"injection_point,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Mode           string               `json:"mode"`
	Priority       int                  `json:"priority"`
	StartPolicy    string               `json:"start_policy,omitempty"`
	Fault          *CompiledFault       `json:"fault,omitempty"`
	Composition    *CompiledComposition `json:"composition,omitempty"`
	CacheBox       *CompiledCacheBox    `json:"cachebox,omitempty"`
}

// CompiledCacheBox is a resolved cache-box action for a rule.
type CompiledCacheBox struct {
	Mode        string `json:"mode"`         // "passthrough" | "replay" | "replay_with_delay"
	KeyStrategy string `json:"key_strategy"` // "exact" | "exact_with_host" | "exact_with_body"
}

// CompiledFault is a resolved fault spec on the wire. It is the same struct
// as FaultRequest (admin.go) — the platform's single fault wire shape — with
// ID simply unused in the compiled-rule context (manteion inlines specs, so
// rule-attached faults need no slot identity).
type CompiledFault = FaultRequest

// NetworkEnvelope is the network-category-only envelope. It separates the
// "where does the toxic live and to which traffic does it apply" question
// from the toxic-specific params (delay, rate, etc.).
type NetworkEnvelope struct {
	// Host selects where the toxic runs.
	//   "proxy"  → TCP proxy sidecar (current behaviour; requires NetworkResolver)
	//   "inline" → in-process RoundTripper wrapping response.Body (not yet supported)
	Host string `json:"host"`

	// Target is the logical name of the upstream service, resolved by
	// NetworkResolver into a (listen, upstream) address pair.
	// Required when Host=="proxy"; ignored when Host=="inline".
	Target string `json:"target,omitempty"`

	// Direction selects which half of a proxied connection to apply the
	// toxic to. Required when Host=="proxy". For Host=="inline" only
	// "downstream" (response body) is meaningful.
	Direction string `json:"direction,omitempty"`

	// Scope is the fraction of connections (proxy) or requests (inline)
	// to which the toxic applies. Zero means 1.0 (all).
	Scope float64 `json:"scope,omitempty"`
}

// CompiledComposition is a resolved FaultComposition tree with all specs
// inlined. Composition execution is not yet supported by the SDK evaluator
// (deferred to v6); DecodeCompiledRule errors if a rule references one.
type CompiledComposition struct {
	Name          string                      `json:"name"`
	ExecutionMode string                      `json:"execution_mode"`
	DurationMs    int64                       `json:"duration_ms,omitempty"`
	RampUpMs      int64                       `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                       `json:"ramp_down_ms,omitempty"`
	Members       []CompiledCompositionMember `json:"members"`
}

// CompiledCompositionMember is a resolved member — either a leaf fault or a
// nested composition.
type CompiledCompositionMember struct {
	Direction   string               `json:"direction,omitempty"`
	Fault       *CompiledFault       `json:"fault,omitempty"`
	Composition *CompiledComposition `json:"composition,omitempty"`
}

type decodeConfig struct {
	resolve NetworkResolver
}

// DecodeOption configures the behaviour of DecodeCompiledRule(s).
type DecodeOption func(*decodeConfig)

// WithNetworkResolver supplies the resolver that maps a logical target name
// (e.g. "redis") to a listen and upstream address pair for network fault
// proxies. Required when decoding rules whose Network.Host=="proxy".
func WithNetworkResolver(r NetworkResolver) DecodeOption {
	return func(c *decodeConfig) { c.resolve = r }
}

func buildDecodeConfig(opts []DecodeOption) *decodeConfig {
	cfg := &decodeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

// DecodeCompiledRules converts wire-format CompiledRules into StaticRules
// that can be loaded into a StaticEvaluator.
func DecodeCompiledRules(compiled []CompiledRule, opts ...DecodeOption) ([]StaticRule, error) {
	sorted := make([]CompiledRule, len(compiled))
	copy(sorted, compiled)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority > sorted[j].Priority
	})

	rules := make([]StaticRule, 0, len(sorted))
	for _, cr := range sorted {
		sr, err := DecodeCompiledRule(cr, opts...)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", cr.Name, err)
		}
		rules = append(rules, sr)
	}
	return rules, nil
}

// DecodeCompiledRule converts a single CompiledRule into a StaticRule.
func DecodeCompiledRule(cr CompiledRule, opts ...DecodeOption) (StaticRule, error) {
	cfg := buildDecodeConfig(opts)

	sr := StaticRule{
		Name:   cr.Name,
		Point:  parseInjectionPoint(cr.InjectionPoint),
		Labels: cr.Labels,
	}

	sr.Decision.Mode = parseMode(cr.Mode)
	sr.Decision.Reason = "compiled"
	sr.Decision.Name = cr.Name
	sr.Decision.StartPolicy = parseStartPolicy(cr.StartPolicy)

	if cr.Fault != nil {
		f, err := decodeFault(cr.Fault, cfg)
		if err != nil {
			return StaticRule{}, err
		}
		sr.Decision.Fault = f
	}

	if cr.Composition != nil {
		// v6 work: SDK-side composition execution (parallel/sequential
		// fault dispatch with direction inheritance). Today manteion can
		// compose and validate, but the SDK can't run them.
		return StaticRule{}, fmt.Errorf(
			"composition rules are not yet supported by the SDK evaluator (v6)",
		)
	}

	if cr.CacheBox != nil {
		sr.Decision.CacheBox = parseCacheBoxMode(cr.CacheBox.Mode)
		sr.Decision.CacheBoxKeyStrategy = cr.CacheBox.KeyStrategy
	}

	return sr, nil
}

func parseInjectionPoint(s string) InjectionPoint {
	switch s {
	case "ingress":
		return Ingress
	case "egress":
		return Egress
	case "transient":
		return Transient
	case "custom":
		return Custom
	default:
		return Ingress
	}
}

func parseMode(s string) Mode {
	switch s {
	case "background":
		return Background
	case "inline":
		return Inline
	default:
		return Inline
	}
}

func parseStartPolicy(s string) StartPolicy {
	switch s {
	case "always_start":
		return AlwaysStart
	default:
		return DeduplicateByRule
	}
}

func decodeFault(f *CompiledFault, cfg *decodeConfig) (Fault, error) {
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
	if f.Network != nil && f.effectiveCategory() != "network" {
		return nil, fmt.Errorf("category %q must not carry a network envelope", f.effectiveCategory())
	}

	switch f.effectiveCategory() {
	case "inline":
		return decodeInlineFault(f.FaultType, params, baseCfg)
	case "network":
		if f.Network == nil {
			return nil, fmt.Errorf("network fault %q requires a network envelope", f.FaultType)
		}
		return decodeNetworkFault(f.FaultType, f.Network, params, baseCfg, cfg.resolve)
	case "resource":
		return decodeResourceFault(f.FaultType, params, baseCfg)
	default:
		return nil, fmt.Errorf("unknown fault category %q", f.Category)
	}
}

// decodeInlineFault dispatches by fault_type within the "inline" category.
// Param shapes are the exported faultparams structs — the decode contract
// and the control-plane validation schema are the same types by construction.
func decodeInlineFault(faultType string, params json.RawMessage, baseCfg fault.FaultConfig) (Fault, error) {
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

func decodeResourceFault(faultType string, params json.RawMessage, baseCfg fault.FaultConfig) (Fault, error) {
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

func decodeNetworkFault(faultType string, env *NetworkEnvelope, params json.RawMessage, baseCfg fault.FaultConfig, resolve NetworkResolver) (Fault, error) {
	host := env.Host
	if host == "" {
		host = "proxy"
	}

	switch host {
	case "proxy":
		return decodeNetworkProxyFault(faultType, env, params, baseCfg, resolve)
	case "inline":
		// v6 work: in-process Toxic host that wraps response.Body in
		// the egress RoundTripper. Schema lands now; execution lands
		// when ToxicTransport is implemented.
		return nil, fmt.Errorf(
			"network fault %q with host=%q: in-process toxic host is not yet supported (v6)",
			faultType, host,
		)
	default:
		return nil, fmt.Errorf("network fault %q: unknown host %q", faultType, host)
	}
}

func decodeNetworkProxyFault(faultType string, env *NetworkEnvelope, params json.RawMessage, baseCfg fault.FaultConfig, resolve NetworkResolver) (Fault, error) {
	if resolve == nil {
		return nil, fmt.Errorf("network fault %q with host=proxy requires a NetworkResolver (use WithNetworkResolver)", faultType)
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

func parseCacheBoxMode(s string) CacheBoxAction {
	switch s {
	case "passthrough":
		return CacheBoxPassthrough
	case "replay":
		return CacheBoxReplay
	case "replay_with_delay":
		return CacheBoxReplayDelay
	default:
		return CacheBoxNone
	}
}
