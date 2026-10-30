package instrument

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func TestDecodeCompiledRules_Latency(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name:           "inject-latency",
		InjectionPoint: "egress",
		Labels:         map[string]string{"svc": "cart"},
		Mode:           "inline",
		Priority:       10,
		Fault: &wire.CompiledFault{
			Category:  "inline",
			FaultType: "latency",
			Params:    json.RawMessage(`{"delay":"200ms","jitter":"50ms"}`),
		},
	}}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("decodeCompiledRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}

	sr := rules[0]
	if sr.Name != "inject-latency" {
		t.Errorf("Name: %q", sr.Name)
	}
	if sr.Point != evaluator.Egress {
		t.Errorf("Point: %v", sr.Point)
	}
	if sr.Decision.Mode != evaluator.Inline {
		t.Errorf("Mode: %v", sr.Decision.Mode)
	}
	if sr.Decision.Fault == nil {
		t.Fatal("expected Fault to be set")
	}

	lat, ok := sr.Decision.Fault.(*inline.Latency)
	if !ok {
		t.Fatalf("expected *inline.Latency, got %T", sr.Decision.Fault)
	}
	if lat.Delay != 200_000_000 {
		t.Errorf("Delay: %v", lat.Delay)
	}
	if lat.Jitter != 50_000_000 {
		t.Errorf("Jitter: %v", lat.Jitter)
	}
}

func TestDecodeCompiledRules_Error(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "inject-error",
		Mode: "inline",
		Fault: &wire.CompiledFault{
			Category:  "inline",
			FaultType: "error",
			Params:    json.RawMessage(`{"status_code":503,"message":"down"}`),
		},
	}}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("decodeCompiledRules: %v", err)
	}

	errFault, ok := rules[0].Decision.Fault.(*inline.Error)
	if !ok {
		t.Fatalf("expected *inline.Error, got %T", rules[0].Decision.Fault)
	}
	if errFault.StatusCode != 503 {
		t.Errorf("StatusCode: %d", errFault.StatusCode)
	}
}

func TestDecodeCompiledRules_Hang(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "inject-hang",
		Mode: "inline",
		Fault: &wire.CompiledFault{
			Category:  "inline",
			FaultType: "hang",
			Params:    json.RawMessage(`{"duration":"5s"}`),
		},
	}}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("decodeCompiledRules: %v", err)
	}

	_, ok := rules[0].Decision.Fault.(*inline.Hang)
	if !ok {
		t.Fatalf("expected *inline.Hang, got %T", rules[0].Decision.Fault)
	}
}

func TestDecodeCompiledRules_NoFault(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name:           "metadata-only",
		InjectionPoint: "ingress",
		Mode:           "background",
	}}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("decodeCompiledRules: %v", err)
	}
	if rules[0].Decision.Fault != nil {
		t.Error("expected nil Fault for metadata-only rule")
	}
	if rules[0].Decision.Mode != evaluator.Background {
		t.Errorf("Mode: %v", rules[0].Decision.Mode)
	}
}

func TestDecodeCompiledRules_JSONRoundtrip(t *testing.T) {
	original := []wire.CompiledRule{{
		Name:           "roundtrip-test",
		InjectionPoint: "egress",
		Labels:         map[string]string{"env": "staging"},
		Mode:           "inline",
		Priority:       5,
		Fault: &wire.CompiledFault{
			Category:   "inline",
			FaultType:  "latency",
			Params:     json.RawMessage(`{"delay":"100ms"}`),
			DurationMs: 150,
		},
	}}

	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded []wire.CompiledRule
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rules, err := decodeCompiledRules(decoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if rules[0].Name != "roundtrip-test" {
		t.Errorf("Name lost in roundtrip: %q", rules[0].Name)
	}
	if rules[0].Decision.Fault == nil {
		t.Fatal("Fault lost in roundtrip")
	}
	lat := rules[0].Decision.Fault.(*inline.Latency)
	if lat.FaultConfig.Duration != 150_000_000 {
		t.Errorf("Duration from DurationMs: %v", lat.FaultConfig.Duration)
	}
}

func TestDecodeCompiledRules_UnknownCategory(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "bad",
		Mode: "inline",
		Fault: &wire.CompiledFault{
			Category:  "quantum",
			FaultType: "entangle",
			Params:    json.RawMessage(`{}`),
		},
	}}

	_, err := decodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error for unknown category")
	}
}

func TestDecodeCompiledRules_ResourceTypes(t *testing.T) {
	tests := []struct {
		name       string
		ftype      string
		params     string
		durationMs int64
		rampUpMs   int64
		rampDownMs int64
	}{
		{"cpu", "cpu", `{"target_load":0.7,"window":"200ms"}`, 30000, 5000, 5000},
		{"memory", "memory", `{"target_load":0.5,"thrashing":true,"thrash_workers":4}`, 10000, 0, 0},
		{"disk", "disk", `{"write_rate":10485760,"max_disk_usage":536870912}`, 15000, 0, 0},
		{"io", "io", `{"read_rate":102400,"file_size":4096,"file_count":64,"workers":2}`, 10000, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compiled := []wire.CompiledRule{{
				Name: tc.name + "-stress",
				Mode: "background",
				Fault: &wire.CompiledFault{
					Category:   "resource",
					FaultType:  tc.ftype,
					Params:     json.RawMessage(tc.params),
					DurationMs: tc.durationMs,
					RampUpMs:   tc.rampUpMs,
					RampDownMs: tc.rampDownMs,
				},
			}}
			rules, err := decodeCompiledRules(compiled)
			if err != nil {
				t.Fatalf("decodeCompiledRules: %v", err)
			}
			if rules[0].Decision.Fault == nil {
				t.Fatal("expected Fault to be set")
			}
			if err := rules[0].Decision.Fault.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestDecodeCompiledRules_UnknownResourceType(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "bad",
		Mode: "background",
		Fault: &wire.CompiledFault{
			Category:   "resource",
			FaultType:  "gpu",
			Params:     json.RawMessage(`{}`),
			DurationMs: 5000,
		},
	}}

	_, err := decodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error for unknown resource type")
	}
}

func stubResolver(listen, upstream string) wire.NetworkResolver {
	return func(target string) (string, string, error) {
		return listen, upstream, nil
	}
}

func TestDecodeCompiledRules_NetworkTypes(t *testing.T) {
	tests := []struct {
		name   string
		ftype  string
		params string
		listen string
	}{
		{"latency", "latency", `{"delay":"100ms","jitter":"20ms"}`, ":19090"},
		{"blackhole", "blackhole", `{}`, ":19091"},
		{"retransmit_delay", "retransmit_delay", `{"rate":0.3,"delay":"100ms","reset_threshold":5}`, ":19092"},
		{"drip", "drip", `{"chunk_size":100,"interval":"50ms"}`, ":19093"},
		{"rst", "rst", `{"after_bytes":4096,"after_duration":"2s"}`, ":19094"},
		{"throttle", "throttle", `{"bytes_per_sec":1024}`, ":19095"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compiled := []wire.CompiledRule{{
				Name: "net-" + tc.name,
				Mode: "background",
				Fault: &wire.CompiledFault{
					Category:   "network",
					FaultType:  tc.ftype,
					Network:    &wire.NetworkEnvelope{Host: "proxy", Target: "redis", Direction: "upstream"},
					Params:     json.RawMessage(tc.params),
					DurationMs: 10000,
				},
			}}
			rules, err := decodeCompiledRules(compiled, withNetworkResolver(stubResolver(tc.listen, "localhost:6379")))
			if err != nil {
				t.Fatalf("decodeCompiledRules: %v", err)
			}
			if rules[0].Decision.Fault == nil {
				t.Fatal("expected Fault to be set")
			}
		})
	}
}

func TestDecodeCompiledRules_NetworkRequiresEnvelope(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "net-no-envelope",
		Mode: "background",
		Fault: &wire.CompiledFault{
			Category:   "network",
			FaultType:  "blackhole",
			Params:     json.RawMessage(`{}`),
			DurationMs: 5000,
		},
	}}
	_, err := decodeCompiledRules(compiled, withNetworkResolver(stubResolver(":1", "localhost:6379")))
	if err == nil {
		t.Fatal("expected error: missing network envelope")
	}
}

func TestDecodeCompiledRules_NonNetworkRejectsEnvelope(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "inline-with-envelope",
		Mode: "inline",
		Fault: &wire.CompiledFault{
			Category:  "inline",
			FaultType: "latency",
			Network:   &wire.NetworkEnvelope{Host: "proxy", Target: "redis"},
			Params:    json.RawMessage(`{"delay":"10ms"}`),
		},
	}}
	_, err := decodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error: envelope on non-network category")
	}
}

func TestDecodeCompiledRules_NetworkInlineHostNotYetSupported(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "net-inline-host",
		Mode: "inline",
		Fault: &wire.CompiledFault{
			Category:   "network",
			FaultType:  "latency",
			Network:    &wire.NetworkEnvelope{Host: "inline"},
			Params:     json.RawMessage(`{"delay":"50ms"}`),
			DurationMs: 5000,
		},
	}}
	_, err := decodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error: inline host without WithInlineToxicHost")
	}
	if !strings.Contains(err.Error(), "does not own proxied connections") {
		t.Errorf("error should say the host cannot run inline toxics, got: %v", err)
	}
}

func TestDecodeCompiledRules_NetworkNoResolver(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "net-no-resolver",
		Mode: "background",
		Fault: &wire.CompiledFault{
			Category:   "network",
			FaultType:  "blackhole",
			Network:    &wire.NetworkEnvelope{Host: "proxy", Target: "redis"},
			Params:     json.RawMessage(`{}`),
			DurationMs: 5000,
		},
	}}
	_, err := decodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error: no resolver provided")
	}
}

func TestDecodeCompiledRules_UnknownNetworkType(t *testing.T) {
	compiled := []wire.CompiledRule{{
		Name: "bad",
		Mode: "background",
		Fault: &wire.CompiledFault{
			Category:   "network",
			FaultType:  "quantum_tunnel",
			Network:    &wire.NetworkEnvelope{Host: "proxy", Target: "redis"},
			Params:     json.RawMessage(`{}`),
			DurationMs: 5000,
		},
	}}
	_, err := decodeCompiledRules(compiled, withNetworkResolver(stubResolver(":19099", "localhost:6379")))
	if err == nil {
		t.Fatal("expected error for unknown network type")
	}
}

// TestDecodeCompiledRules_CompositionSkippedAndCounted: a composition rule
// is skipped per rule -- counted, logged -- and never fails the set it
// arrives in (design review §5; protocol §3). Categories keep their
// semantics: see TestDecode_UnsupportedCategoryIsSkippedAndCounted and
// TestDecodeCompiledRules_UnknownCategory.
func TestDecodeCompiledRules_CompositionSkippedAndCounted(t *testing.T) {
	before := unsupportedCount(t, "composition")
	compiled := []wire.CompiledRule{
		{Name: "lat", Mode: "inline", Priority: 10, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
		{Name: "comp-rule", Mode: "inline", Priority: 5, Composition: &wire.CompiledComposition{
			Name: "x", ExecutionMode: "parallel",
			Members: []wire.CompiledCompositionMember{
				{Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
				{Fault: &wire.CompiledFault{Category: "inline", FaultType: "error", Params: json.RawMessage(`{"status_code":500}`)}},
			}}},
		{Name: "err", Mode: "inline", Priority: 1, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "error", Params: json.RawMessage(`{"status_code":500}`)}},
	}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("a composition must not fail the set: %v", err)
	}
	if len(rules) != 2 || rules[0].Name != "lat" || rules[1].Name != "err" {
		t.Fatalf("rules = %+v, want [lat err] with the composition skipped", rules)
	}
	if got := unsupportedCount(t, "composition") - before; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{composition} += %v, want 1", got)
	}
}

func TestDecodeCompiledRules_PrioritySorted(t *testing.T) {
	compiled := []wire.CompiledRule{
		{Name: "low", Mode: "inline", Priority: 1, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`),
		}},
		{Name: "high", Mode: "inline", Priority: 10, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"20ms"}`),
		}},
		{Name: "mid", Mode: "inline", Priority: 5, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"30ms"}`),
		}},
	}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("decodeCompiledRules: %v", err)
	}
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}

	want := []string{"high", "mid", "low"}
	for i, name := range want {
		if rules[i].Name != name {
			t.Errorf("rules[%d].Name = %q, want %q", i, rules[i].Name, name)
		}
	}
}

func TestDecodeCompiledRules_PriorityStable(t *testing.T) {
	compiled := []wire.CompiledRule{
		{Name: "first", Mode: "inline", Priority: 5, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`),
		}},
		{Name: "second", Mode: "inline", Priority: 5, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"20ms"}`),
		}},
	}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("decodeCompiledRules: %v", err)
	}

	if rules[0].Name != "first" || rules[1].Name != "second" {
		t.Errorf("equal priority should preserve input order: got %q, %q", rules[0].Name, rules[1].Name)
	}
}

// TestDecode_InlineToxicHostReceivesRuleNameEnvelopeAndToxic pins the
// sidecar's decode seam: with WithInlineToxicHost installed, a host=inline
// network fault is handed to the hook with its rule name, envelope and the
// decoded toxic, and the hook's fault is what the rule carries.
func TestDecode_InlineToxicHostReceivesRuleNameEnvelopeAndToxic(t *testing.T) {
	var gotName, gotTarget string
	var gotToxic network.Toxic
	stub := &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Second}, Delay: time.Millisecond}
	hook := func(name string, env *wire.NetworkEnvelope, toxic network.Toxic, base fault.FaultConfig) (fault.Fault, error) {
		gotName, gotTarget, gotToxic = name, env.Target, toxic
		return stub, nil
	}
	rules, err := decodeCompiledRules([]wire.CompiledRule{{
		Name: "throttle-redis", InjectionPoint: "egress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "network", FaultType: "throttle", DurationMs: 5000,
			Network: &wire.NetworkEnvelope{Host: "inline", Target: "redis:6379", Direction: "downstream"},
			Params:  json.RawMessage(`{"bytes_per_sec":1024}`)},
	}}, WithInlineToxicHost(hook))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotName != "throttle-redis" || gotTarget != "redis:6379" {
		t.Fatalf("hook got name=%q target=%q", gotName, gotTarget)
	}
	if th, ok := gotToxic.(*network.Throttle); !ok || th.BytesPerSec != 1024 {
		t.Fatalf("hook got toxic %#v, want *network.Throttle{1024}", gotToxic)
	}
	if rules[0].Decision.Fault != stub {
		t.Fatal("the hook's fault must be what the decoded rule carries")
	}
}
