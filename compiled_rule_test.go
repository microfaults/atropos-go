package atropos

import (
	"encoding/json"
	"strings"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
)

func TestDecodeCompiledRules_Latency(t *testing.T) {
	compiled := []CompiledRule{{
		Name:           "inject-latency",
		InjectionPoint: "egress",
		Labels:         map[string]string{"svc": "cart"},
		Mode:           "inline",
		Priority:       10,
		Fault: &CompiledFault{
			Category:  "inline",
			FaultType: "latency",
			Config:    json.RawMessage(`{"delay":"200ms","jitter":"50ms"}`),
		},
	}}

	rules, err := DecodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("DecodeCompiledRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}

	sr := rules[0]
	if sr.Name != "inject-latency" {
		t.Errorf("Name: %q", sr.Name)
	}
	if sr.Point != Egress {
		t.Errorf("Point: %v", sr.Point)
	}
	if sr.Decision.Mode != Inline {
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
	compiled := []CompiledRule{{
		Name: "inject-error",
		Mode: "inline",
		Fault: &CompiledFault{
			Category:  "inline",
			FaultType: "error",
			Config:    json.RawMessage(`{"status_code":503,"message":"down"}`),
		},
	}}

	rules, err := DecodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("DecodeCompiledRules: %v", err)
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
	compiled := []CompiledRule{{
		Name: "inject-hang",
		Mode: "inline",
		Fault: &CompiledFault{
			Category:  "inline",
			FaultType: "hang",
			Config:    json.RawMessage(`{"duration":"5s"}`),
		},
	}}

	rules, err := DecodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("DecodeCompiledRules: %v", err)
	}

	_, ok := rules[0].Decision.Fault.(*inline.Hang)
	if !ok {
		t.Fatalf("expected *inline.Hang, got %T", rules[0].Decision.Fault)
	}
}

func TestDecodeCompiledRules_NoFault(t *testing.T) {
	compiled := []CompiledRule{{
		Name:           "metadata-only",
		InjectionPoint: "ingress",
		Mode:           "background",
	}}

	rules, err := DecodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("DecodeCompiledRules: %v", err)
	}
	if rules[0].Decision.Fault != nil {
		t.Error("expected nil Fault for metadata-only rule")
	}
	if rules[0].Decision.Mode != Background {
		t.Errorf("Mode: %v", rules[0].Decision.Mode)
	}
}

func TestDecodeCompiledRules_JSONRoundtrip(t *testing.T) {
	original := []CompiledRule{{
		Name:           "roundtrip-test",
		InjectionPoint: "egress",
		Labels:         map[string]string{"env": "staging"},
		Mode:           "inline",
		Priority:       5,
		Fault: &CompiledFault{
			Category:   "inline",
			FaultType:  "latency",
			Config:     json.RawMessage(`{"delay":"100ms"}`),
			DurationMs: 150,
		},
	}}

	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded []CompiledRule
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rules, err := DecodeCompiledRules(decoded)
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
	compiled := []CompiledRule{{
		Name: "bad",
		Mode: "inline",
		Fault: &CompiledFault{
			Category:  "quantum",
			FaultType: "entangle",
			Config:    json.RawMessage(`{}`),
		},
	}}

	_, err := DecodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error for unknown category")
	}
}

func TestDecodeCompiledRules_ResourceTypes(t *testing.T) {
	tests := []struct {
		name       string
		ftype      string
		config     string
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
			compiled := []CompiledRule{{
				Name: tc.name + "-stress",
				Mode: "background",
				Fault: &CompiledFault{
					Category:   "resource",
					FaultType:  tc.ftype,
					Config:     json.RawMessage(tc.config),
					DurationMs: tc.durationMs,
					RampUpMs:   tc.rampUpMs,
					RampDownMs: tc.rampDownMs,
				},
			}}
			rules, err := DecodeCompiledRules(compiled)
			if err != nil {
				t.Fatalf("DecodeCompiledRules: %v", err)
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
	compiled := []CompiledRule{{
		Name: "bad",
		Mode: "background",
		Fault: &CompiledFault{
			Category:   "resource",
			FaultType:  "gpu",
			Config:     json.RawMessage(`{}`),
			DurationMs: 5000,
		},
	}}

	_, err := DecodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error for unknown resource type")
	}
}

func stubResolver(listen, upstream string) NetworkResolver {
	return func(target string) (string, string, error) {
		return listen, upstream, nil
	}
}

func TestDecodeCompiledRules_NetworkTypes(t *testing.T) {
	tests := []struct {
		name   string
		ftype  string
		config string
		listen string
	}{
		{"latency", "latency", `{"target":"redis","direction":"upstream","delay":"100ms","jitter":"20ms"}`, ":19090"},
		{"blackhole", "blackhole", `{"target":"redis"}`, ":19091"},
		{"retransmit_delay", "retransmit_delay", `{"target":"redis","rate":0.3,"delay":"100ms","reset_threshold":5}`, ":19092"},
		{"drip", "drip", `{"target":"redis","chunk_size":100,"interval":"50ms"}`, ":19093"},
		{"rst", "rst", `{"target":"redis","after_bytes":4096,"after_duration":"2s"}`, ":19094"},
		{"throttle", "throttle", `{"target":"redis","bytes_per_sec":1024}`, ":19095"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compiled := []CompiledRule{{
				Name: "net-" + tc.name,
				Mode: "background",
				Fault: &CompiledFault{
					Category:   "network",
					FaultType:  tc.ftype,
					Config:     json.RawMessage(tc.config),
					DurationMs: 10000,
				},
			}}
			rules, err := DecodeCompiledRules(compiled, WithNetworkResolver(stubResolver(tc.listen, "localhost:6379")))
			if err != nil {
				t.Fatalf("DecodeCompiledRules: %v", err)
			}
			if rules[0].Decision.Fault == nil {
				t.Fatal("expected Fault to be set")
			}
		})
	}
}

func TestDecodeCompiledRules_NetworkNoResolver(t *testing.T) {
	compiled := []CompiledRule{{
		Name: "net-no-resolver",
		Mode: "background",
		Fault: &CompiledFault{
			Category:   "network",
			FaultType:  "blackhole",
			Config:     json.RawMessage(`{"target":"redis"}`),
			DurationMs: 5000,
		},
	}}
	_, err := DecodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error: no resolver provided")
	}
}

func TestDecodeCompiledRules_UnknownNetworkType(t *testing.T) {
	compiled := []CompiledRule{{
		Name: "bad",
		Mode: "background",
		Fault: &CompiledFault{
			Category:   "network",
			FaultType:  "quantum_tunnel",
			Config:     json.RawMessage(`{"target":"redis"}`),
			DurationMs: 5000,
		},
	}}
	_, err := DecodeCompiledRules(compiled, WithNetworkResolver(stubResolver(":19099", "localhost:6379")))
	if err == nil {
		t.Fatal("expected error for unknown network type")
	}
}

func TestDecodeCompiledRules_CompositionRejected(t *testing.T) {
	compiled := []CompiledRule{{
		Name: "comp-rule",
		Mode: "inline",
		Composition: &CompiledComposition{
			Name: "x", ExecutionMode: "parallel",
			Members: []CompiledCompositionMember{
				{Fault: &CompiledFault{Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"10ms"}`)}},
				{Fault: &CompiledFault{Category: "inline", FaultType: "error", Config: json.RawMessage(`{"status_code":500}`)}},
			},
		},
	}}

	_, err := DecodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error for composition rule")
	}
	// Assert the guard fired specifically — not some other error path.
	// DecodeCompiledRules wraps the guard's sub-message with "rule %q:".
	if !strings.Contains(err.Error(), "comp-rule") {
		t.Errorf("error should name the rule, got: %v", err)
	}
	if !strings.Contains(err.Error(), "composition") {
		t.Errorf("error should mention composition, got: %v", err)
	}
}

func TestDecodeCompiledRules_PrioritySorted(t *testing.T) {
	compiled := []CompiledRule{
		{Name: "low", Mode: "inline", Priority: 1, Fault: &CompiledFault{
			Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"10ms"}`),
		}},
		{Name: "high", Mode: "inline", Priority: 10, Fault: &CompiledFault{
			Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"20ms"}`),
		}},
		{Name: "mid", Mode: "inline", Priority: 5, Fault: &CompiledFault{
			Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"30ms"}`),
		}},
	}

	rules, err := DecodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("DecodeCompiledRules: %v", err)
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
	compiled := []CompiledRule{
		{Name: "first", Mode: "inline", Priority: 5, Fault: &CompiledFault{
			Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"10ms"}`),
		}},
		{Name: "second", Mode: "inline", Priority: 5, Fault: &CompiledFault{
			Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"20ms"}`),
		}},
	}

	rules, err := DecodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("DecodeCompiledRules: %v", err)
	}

	if rules[0].Name != "first" || rules[1].Name != "second" {
		t.Errorf("equal priority should preserve input order: got %q, %q", rules[0].Name, rules[1].Name)
	}
}
