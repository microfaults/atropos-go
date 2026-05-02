package atropos

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/microfaults/atropos-go/internal/fault"
	"github.com/microfaults/atropos-go/internal/fault/inline"
	"github.com/microfaults/atropos-go/internal/fault/network"
	"github.com/microfaults/atropos-go/internal/fault/resource"
	"github.com/microfaults/atropos-go/internal/fault/resource/cpu"
	"github.com/microfaults/atropos-go/internal/fault/resource/disk"
	iostress "github.com/microfaults/atropos-go/internal/fault/resource/io"
	"github.com/microfaults/atropos-go/internal/fault/resource/memory"
)

// CompiledRule is the wire format received from manteion's poll/register
// endpoints. Manteion resolves FaultSpec references and inlines config;
// the SDK decodes and constructs evaluator rules locally.
type CompiledRule struct {
	Name           string               `json:"name"`
	InjectionPoint string               `json:"injection_point,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Mode           string               `json:"mode"`
	Priority       int                  `json:"priority"`
	StartPolicy    string               `json:"start_policy,omitempty"`
	Fault          *CompiledFault       `json:"fault,omitempty"`
	Composition    *CompiledComposition `json:"composition,omitempty"`
}

// CompiledFault is a resolved fault spec with config inlined.
type CompiledFault struct {
	Category   string          `json:"category"`
	FaultType  string          `json:"fault_type"`
	Config     json.RawMessage `json:"config"`
	DurationMs int64           `json:"duration_ms,omitempty"`
	RampUpMs   int64           `json:"ramp_up_ms,omitempty"`
	RampDownMs int64           `json:"ramp_down_ms,omitempty"`
}

// CompiledComposition is a resolved FaultComposition tree with all specs
// inlined. Composition execution is not yet supported by the SDK evaluator;
// DecodeCompiledRule errors if a rule references one.
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
// proxies. Required when decoding rules that contain network-category faults.
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
		// DecodeCompiledRules wraps this with the rule name, so the sub-message
		// only needs to explain why. Cleaner than double-naming the rule.
		return StaticRule{}, fmt.Errorf(
			"composition rules are not yet supported by the SDK evaluator",
		)
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

	switch f.Category {
	case "inline":
		return decodeInlineFault(f.FaultType, f.Config, baseCfg)
	case "network":
		if cfg.resolve == nil {
			return nil, fmt.Errorf("network fault %q requires a NetworkResolver (use WithNetworkResolver)", f.FaultType)
		}
		return decodeNetworkFault(f.FaultType, f.Config, baseCfg, cfg.resolve)
	case "resource":
		return decodeResourceFault(f.FaultType, f.Config, baseCfg)
	default:
		return nil, fmt.Errorf("unknown fault category %q", f.Category)
	}
}

// decodeInlineFault dispatches by fault_type within the "inline" category
// (not by the CompiledFault wire type — those names are deliberately different).
func decodeInlineFault(faultType string, config json.RawMessage, baseCfg fault.FaultConfig) (Fault, error) {
	switch faultType {
	case "latency":
		var cfg struct {
			Delay  string `json:"delay"`
			Jitter string `json:"jitter"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode latency config: %w", err)
		}
		delay, err := time.ParseDuration(cfg.Delay)
		if err != nil {
			return nil, fmt.Errorf("parse latency delay %q: %w", cfg.Delay, err)
		}
		var jitter time.Duration
		if cfg.Jitter != "" {
			jitter, err = time.ParseDuration(cfg.Jitter)
			if err != nil {
				return nil, fmt.Errorf("parse latency jitter %q: %w", cfg.Jitter, err)
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
		var cfg struct {
			StatusCode int    `json:"status_code"`
			Message    string `json:"message"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode error config: %w", err)
		}
		if baseCfg.Duration == 0 {
			baseCfg.Duration = 1
		}
		return &inline.Error{
			FaultConfig: baseCfg,
			StatusCode:  cfg.StatusCode,
			Message:     cfg.Message,
		}, nil

	case "hang":
		var cfg struct {
			Duration string `json:"duration"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode hang config: %w", err)
		}
		dur, err := time.ParseDuration(cfg.Duration)
		if err != nil {
			return nil, fmt.Errorf("parse hang duration %q: %w", cfg.Duration, err)
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

func decodeResourceFault(faultType string, config json.RawMessage, baseCfg fault.FaultConfig) (Fault, error) {
	switch faultType {
	case "cpu":
		var cfg struct {
			TargetLoad float64 `json:"target_load"`
			Window     string  `json:"window"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode cpu config: %w", err)
		}
		rc := resource.Config{
			FaultConfig: baseCfg,
			TargetLoad:  cfg.TargetLoad,
		}
		if cfg.Window != "" {
			w, err := time.ParseDuration(cfg.Window)
			if err != nil {
				return nil, fmt.Errorf("parse cpu window %q: %w", cfg.Window, err)
			}
			rc.Window = w
		}
		return &cpu.Stress{Config: rc}, nil

	case "memory":
		var cfg struct {
			TargetLoad    float64 `json:"target_load"`
			ChunkSize     int     `json:"chunk_size"`
			Thrashing     bool    `json:"thrashing"`
			ThrashWorkers int     `json:"thrash_workers"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode memory config: %w", err)
		}
		return &memory.Stress{Config: memory.Config{
			FaultConfig:   baseCfg,
			TargetLoad:    cfg.TargetLoad,
			ChunkSize:     cfg.ChunkSize,
			Thrashing:     cfg.Thrashing,
			ThrashWorkers: cfg.ThrashWorkers,
		}}, nil

	case "disk":
		var cfg struct {
			WriteRate    int64  `json:"write_rate"`
			MaxDiskUsage int64  `json:"max_disk_usage"`
			ChunkSize    int64  `json:"chunk_size"`
			Path         string `json:"path"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode disk config: %w", err)
		}
		dc := disk.Config{
			FaultConfig:  baseCfg,
			WriteRate:    cfg.WriteRate,
			MaxDiskUsage: cfg.MaxDiskUsage,
			ChunkSize:    cfg.ChunkSize,
			Path:         cfg.Path,
		}
		if dc.WriteRate == 0 {
			dc.WriteRate = disk.DefaultWriteRate
		}
		if dc.MaxDiskUsage == 0 {
			dc.MaxDiskUsage = disk.DefaultMaxDisk
		}
		return &disk.Stress{Config: dc}, nil

	case "io":
		var cfg struct {
			ReadRate  int64  `json:"read_rate"`
			FileSize  int    `json:"file_size"`
			FileCount int    `json:"file_count"`
			Workers   int    `json:"workers"`
			Path      string `json:"path"`
			Mode      string `json:"mode"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode io config: %w", err)
		}
		var ioMode iostress.IOMode
		switch cfg.Mode {
		case "write":
			ioMode = iostress.ModeWrite
		case "read_write":
			ioMode = iostress.ModeReadWrite
		default:
			ioMode = iostress.ModeRead
		}
		ic := iostress.Config{
			FaultConfig: baseCfg,
			ReadRate:    cfg.ReadRate,
			FileSize:    cfg.FileSize,
			FileCount:   cfg.FileCount,
			Workers:     cfg.Workers,
			Path:        cfg.Path,
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

func decodeNetworkFault(faultType string, config json.RawMessage, baseCfg fault.FaultConfig, resolve NetworkResolver) (Fault, error) {
	var envelope struct {
		Target    string  `json:"target"`
		Direction string  `json:"direction"`
		Scope     float64 `json:"scope"`
	}
	if err := json.Unmarshal(config, &envelope); err != nil {
		return nil, fmt.Errorf("decode network envelope: %w", err)
	}

	listen, upstream, err := resolve(envelope.Target)
	if err != nil {
		return nil, fmt.Errorf("resolve network target %q: %w", envelope.Target, err)
	}

	dir := network.Upstream
	if envelope.Direction == "downstream" {
		dir = network.Downstream
	}

	toxic, err := decodeNetworkToxic(faultType, config)
	if err != nil {
		return nil, err
	}

	return &network.Proxy{Config: network.Config{
		FaultConfig: baseCfg,
		Listen:      listen,
		Upstream:    upstream,
		Scope:       envelope.Scope,
		Toxics:      []network.ToxicLink{{Toxic: toxic, Direction: dir}},
	}}, nil
}

func decodeNetworkToxic(faultType string, config json.RawMessage) (network.Toxic, error) {
	switch faultType {
	case "latency":
		var cfg struct {
			Delay  string `json:"delay"`
			Jitter string `json:"jitter"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode network latency config: %w", err)
		}
		delay, err := time.ParseDuration(cfg.Delay)
		if err != nil {
			return nil, fmt.Errorf("parse network latency delay %q: %w", cfg.Delay, err)
		}
		var jitter time.Duration
		if cfg.Jitter != "" {
			jitter, err = time.ParseDuration(cfg.Jitter)
			if err != nil {
				return nil, fmt.Errorf("parse network latency jitter %q: %w", cfg.Jitter, err)
			}
		}
		return &network.Latency{Delay: delay, Jitter: jitter}, nil

	case "loss":
		var cfg struct {
			Rate            float64 `json:"rate"`
			RetransmitDelay string  `json:"retransmit_delay"`
			ResetThreshold  int     `json:"reset_threshold"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode network loss config: %w", err)
		}
		l := &network.Loss{Rate: cfg.Rate, ResetThreshold: cfg.ResetThreshold}
		if cfg.RetransmitDelay != "" {
			d, err := time.ParseDuration(cfg.RetransmitDelay)
			if err != nil {
				return nil, fmt.Errorf("parse network loss retransmit_delay %q: %w", cfg.RetransmitDelay, err)
			}
			l.RetransmitDelay = d
		}
		return l, nil

	case "blackhole":
		return &network.Blackhole{}, nil

	case "drip":
		var cfg struct {
			ChunkSize int    `json:"chunk_size"`
			Interval  string `json:"interval"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode network drip config: %w", err)
		}
		d := &network.Drip{ChunkSize: cfg.ChunkSize}
		if cfg.Interval != "" {
			iv, err := time.ParseDuration(cfg.Interval)
			if err != nil {
				return nil, fmt.Errorf("parse network drip interval %q: %w", cfg.Interval, err)
			}
			d.Interval = iv
		}
		return d, nil

	case "rst":
		var cfg struct {
			AfterBytes    int64  `json:"after_bytes"`
			AfterDuration string `json:"after_duration"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode network rst config: %w", err)
		}
		r := &network.RST{AfterBytes: cfg.AfterBytes}
		if cfg.AfterDuration != "" {
			d, err := time.ParseDuration(cfg.AfterDuration)
			if err != nil {
				return nil, fmt.Errorf("parse network rst after_duration %q: %w", cfg.AfterDuration, err)
			}
			r.AfterDuration = d
		}
		return r, nil

	case "throttle":
		var cfg struct {
			BytesPerSec int64 `json:"bytes_per_sec"`
		}
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("decode network throttle config: %w", err)
		}
		return &network.Throttle{BytesPerSec: cfg.BytesPerSec}, nil

	default:
		return nil, fmt.Errorf("unknown network fault type %q", faultType)
	}
}
