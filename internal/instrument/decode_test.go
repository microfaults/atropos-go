package instrument

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules
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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules

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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules

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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules
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

	out := decodeRuleSet(decoded)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules

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

	out := decodeRuleSet(compiled)
	if len(out.Rules) != 0 || len(out.Errors) != 1 {
		t.Fatalf("rules=%d errors=%v, want the rule skipped with one error", len(out.Rules), out.Errors)
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
			out := decodeRuleSet(compiled)
			if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
				t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
			}
			rules := out.Rules
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

	out := decodeRuleSet(compiled)
	if len(out.Rules) != 0 || len(out.Errors) != 1 {
		t.Fatalf("rules=%d errors=%v, want the rule skipped with one error", len(out.Rules), out.Errors)
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
			out := decodeRuleSet(compiled, withNetworkResolver(stubResolver(tc.listen, "localhost:6379")))
			if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
				t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
			}
			rules := out.Rules
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
	out := decodeRuleSet(compiled, withNetworkResolver(stubResolver(":1", "localhost:6379")))
	if len(out.Rules) != 0 || len(out.Errors) != 1 {
		t.Fatalf("rules=%d errors=%v, want the rule skipped with one error", len(out.Rules), out.Errors)
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
	out := decodeRuleSet(compiled)
	if len(out.Rules) != 0 || len(out.Errors) != 1 {
		t.Fatalf("rules=%d errors=%v, want the rule skipped with one error", len(out.Rules), out.Errors)
	}
}

// TestDecodeCompiledRules_NetworkRulesTheHostCannotRunAreSkippedAndCounted:
// a network rule needs something the in-process host may lack -- an inline
// toxic host for host=inline (the sidecar owns proxied connections), a
// NetworkResolver for host=proxy. Without it the rule is skipped per rule,
// counted under atropos_rules_unsupported_total{category="network"} and
// logged, exactly like a quota resource fault; it never fails the set it
// arrives in. A host that has the missing piece decodes the same rule.
func TestDecodeCompiledRules_NetworkRulesTheHostCannotRunAreSkippedAndCounted(t *testing.T) {
	toxicHost := WithInlineToxicHost(func(name string, env *wire.NetworkEnvelope, toxic network.Toxic, base fault.FaultConfig) (fault.Fault, error) {
		return &inline.Latency{FaultConfig: base, Delay: time.Millisecond}, nil
	})
	resolver := withNetworkResolver(stubResolver(":19096", "localhost:6379"))
	networkRule := func(host, faultType, params string) wire.CompiledRule {
		return wire.CompiledRule{Name: "net-rule", Mode: "background", Fault: &wire.CompiledFault{
			Category: "network", FaultType: faultType, DurationMs: 5000, Params: json.RawMessage(params),
			Network: &wire.NetworkEnvelope{Host: host, Target: "redis"},
		}}
	}
	sibling := wire.CompiledRule{Name: "lat", Mode: "inline", Fault: &wire.CompiledFault{
		Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}}

	for _, tc := range []struct {
		name        string
		rule        wire.CompiledRule
		opts        []decodeOption
		wantSkipped string // the UnsupportedFaultError.FaultType, or "" when the rule applies
	}{
		{"inline host without an inline toxic host", networkRule("inline", "latency", `{"delay":"50ms"}`), nil, "latency/inline"},
		{"inline host with an inline toxic host", networkRule("inline", "latency", `{"delay":"50ms"}`), []decodeOption{toxicHost}, ""},
		{"proxy host without a resolver", networkRule("proxy", "blackhole", `{}`), nil, "blackhole/proxy"},
		{"default host (proxy) without a resolver", networkRule("", "blackhole", `{}`), nil, "blackhole/proxy"},
		{"proxy host with a resolver", networkRule("proxy", "blackhole", `{}`), []decodeOption{resolver}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// On its own the rule says what it cannot do, as a typed error.
			_, err := decodeCompiledRule(tc.rule, tc.opts...)
			var unsup *UnsupportedFaultError
			switch {
			case tc.wantSkipped == "" && err != nil:
				t.Fatalf("decodeCompiledRule: %v, want the rule to decode", err)
			case tc.wantSkipped != "" && (!errors.As(err, &unsup) || unsup.Category != "network" || unsup.FaultType != tc.wantSkipped):
				t.Fatalf("decodeCompiledRule error = %v, want *UnsupportedFaultError{network, %s}", err, tc.wantSkipped)
			}

			// In a set it is skipped and counted; the rest of the set applies.
			before := unsupportedCount(t, "network")
			out := decodeRuleSet([]wire.CompiledRule{tc.rule, sibling}, tc.opts...)
			if len(out.Errors) != 0 {
				t.Fatalf("a rule the host cannot run must not fail the set: errors=%v", out.Errors)
			}
			rules := out.Rules
			wantRules, wantCount := 2, 0.0
			wantUnsupported := []wire.UnsupportedRule{}
			if tc.wantSkipped != "" {
				wantRules, wantCount = 1, 1
				wantUnsupported = []wire.UnsupportedRule{{Rule: "net-rule", Category: "network"}}
			}
			if len(rules) != wantRules || rules[len(rules)-1].Name != "lat" {
				t.Fatalf("rules = %+v, want %d with the sibling applied", rules, wantRules)
			}
			if !reflect.DeepEqual(out.Unsupported, wantUnsupported) {
				t.Fatalf("unsupported = %+v, want %+v", out.Unsupported, wantUnsupported)
			}
			if got := unsupportedCount(t, "network") - before; got != wantCount {
				t.Fatalf("atropos_rules_unsupported_total{network} += %v, want %v", got, wantCount)
			}
		})
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
	out := decodeRuleSet(compiled, withNetworkResolver(stubResolver(":19099", "localhost:6379")))
	if len(out.Rules) != 0 || len(out.Errors) != 1 {
		t.Fatalf("rules=%d errors=%v, want the rule skipped with one error", len(out.Rules), out.Errors)
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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 {
		t.Fatalf("a composition must not fail the set: errors=%v", out.Errors)
	}
	rules := out.Rules
	if len(rules) != 2 || rules[0].Name != "lat" || rules[1].Name != "err" {
		t.Fatalf("rules = %+v, want [lat err] with the composition skipped", rules)
	}
	if got := unsupportedCount(t, "composition") - before; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{composition} += %v, want 1", got)
	}
}

// TestDecodeRuleSet_CompositionSkippedAndCounted: a composition rule lands in
// Unsupported under "composition" (and in the metric), never in Errors, and
// the rest of the set applies.
func TestDecodeRuleSet_CompositionSkippedAndCounted(t *testing.T) {
	before := unsupportedCount(t, "composition")
	compiled := []wire.CompiledRule{{
		Name: "comp-rule", Mode: "inline",
		Composition: &wire.CompiledComposition{Name: "x", ExecutionMode: "parallel", Members: []wire.CompiledCompositionMember{
			{Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
		}},
	}, {
		Name: "lat", InjectionPoint: "ingress", Mode: "inline",
		Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)},
	}}
	out := decodeRuleSet(compiled)
	if len(out.Rules) != 1 || out.Rules[0].Name != "lat" || len(out.Errors) != 0 {
		t.Fatalf("out = %+v, want only lat applied and no errors", out)
	}
	if len(out.Unsupported) != 1 || out.Unsupported[0] != (wire.UnsupportedRule{Rule: "comp-rule", Category: CapComposition}) {
		t.Fatalf("unsupported = %+v", out.Unsupported)
	}
	if got := unsupportedCount(t, "composition") - before; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{composition} += %v, want 1", got)
	}
}

// TestDecodeRuleSet_DecodeErrorSkipsOnlyThatRule: a malformed rule is
// reported in Errors and skipped; it never fails the set it arrives in.
func TestDecodeRuleSet_DecodeErrorSkipsOnlyThatRule(t *testing.T) {
	compiled := []wire.CompiledRule{
		{Name: "bad", Mode: "inline", Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}},
		{Name: "good", Mode: "inline", Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}},
	}
	out := decodeRuleSet(compiled)
	if len(out.Rules) != 1 || out.Rules[0].Name != "good" {
		t.Fatalf("rules = %+v, want only good", out.Rules)
	}
	if len(out.Errors) != 1 || !strings.HasPrefix(out.Errors[0], "rule bad: decode: ") || !strings.Contains(out.Errors[0], "bogus") {
		t.Fatalf("errors = %v", out.Errors)
	}
	if out.Unsupported == nil || out.Errors == nil {
		t.Fatal("slices must be non-nil so the status report marshals [] not null")
	}
}

// TestDecodeRuleSet_MixedSetReportsEachRuleOnItsOwn: one set holds a rule the
// host does not support, a composition, a malformed rule and two good ones.
// Each gets its own outcome -- the good ones apply (priority sorted), the
// unsupported one and the composition are reported and counted under their
// own categories, the malformed one is reported as an error -- and none of
// them fails the set or takes a neighbour down.
func TestDecodeRuleSet_MixedSetReportsEachRuleOnItsOwn(t *testing.T) {
	latLow := wire.CompiledRule{Name: "lat-low", Mode: "inline", Priority: 1, Fault: &wire.CompiledFault{
		Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}}
	bad := wire.CompiledRule{Name: "bad", Mode: "inline", Priority: 4, Fault: &wire.CompiledFault{
		Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}}
	comp := wire.CompiledRule{Name: "comp", Mode: "inline", Priority: 6, Composition: &wire.CompiledComposition{
		Name: "c", ExecutionMode: "sequential", Members: []wire.CompiledCompositionMember{
			{Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}},
		}}}
	cpu := wire.CompiledRule{Name: "cpu", Mode: "background", Priority: 8, Fault: &wire.CompiledFault{
		Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)}}
	errHigh := wire.CompiledRule{Name: "err-high", Mode: "inline", Priority: 10, Fault: &wire.CompiledFault{
		Category: "inline", FaultType: "error", Params: json.RawMessage(`{"status_code":503}`)}}

	caps := WithCapabilities(CapInline, CapCacheBox)
	resourceBefore, compositionBefore := unsupportedCount(t, "resource"), unsupportedCount(t, "composition")

	out := decodeRuleSet([]wire.CompiledRule{latLow, bad, comp, cpu, errHigh}, caps)

	var applied []string
	for _, r := range out.Rules {
		applied = append(applied, r.Name)
	}
	if want := []string{"err-high", "lat-low"}; !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied rules = %v, want %v", applied, want)
	}

	wantUnsupported := []wire.UnsupportedRule{
		{Rule: "cpu", Category: "resource"},
		{Rule: "comp", Category: CapComposition},
	}
	if !reflect.DeepEqual(out.Unsupported, wantUnsupported) {
		t.Fatalf("unsupported = %+v, want %+v", out.Unsupported, wantUnsupported)
	}

	// The error entry is the rule's own decode failure, named and prefixed.
	_, cause := decodeCompiledRule(bad, caps)
	if cause == nil || !strings.Contains(cause.Error(), "bogus") {
		t.Fatalf("decodeCompiledRule(bad) error = %v, want one that names the bad value", cause)
	}
	if want := []string{fmt.Sprintf("rule %s: decode: %v", "bad", cause)}; !reflect.DeepEqual(out.Errors, want) {
		t.Fatalf("errors = %q, want %q", out.Errors, want)
	}

	if got := unsupportedCount(t, "resource") - resourceBefore; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{resource} += %v, want 1", got)
	}
	if got := unsupportedCount(t, "composition") - compositionBefore; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{composition} += %v, want 1", got)
	}
}

// skippedRules is one rule that does not decode, one this host cannot execute
// (a resource fault under inline-only capabilities) and one that applies.
func skippedRules() []wire.CompiledRule {
	return []wire.CompiledRule{
		{Name: "bad", Mode: "inline", Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}},
		{Name: "cpu", Mode: "background", Fault: &wire.CompiledFault{
			Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)}},
		{Name: "good", Mode: "inline", Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}},
	}
}

// The warnings decodeRuleSet raises for the rules it skips go to the logger its
// options carry -- the host's, not the process default -- with the rule named
// and, for an unsupported one, its category and fault type.
func TestDecodeRuleSet_SkippedRulesAreLoggedThroughTheGivenLogger(t *testing.T) {
	capture := &logCapture{}
	decodeRuleSet(skippedRules(), WithCapabilities(CapInline), withLogger(capture.logger()))

	e, ok := capture.find("rule skipped: decode error")
	if !ok {
		t.Fatalf("logger got %q, want the decode-error warning", capture.messages())
	}
	if e.level != slog.LevelWarn || e.attrs["rule"] != "bad" || !strings.Contains(fmt.Sprint(e.attrs["error"]), "bogus") {
		t.Fatalf("decode-error record = %v %v, want a Warn for rule bad naming the bogus value", e.level, e.attrs)
	}
	e, ok = capture.find("rule skipped: not executable by this host")
	if !ok {
		t.Fatalf("logger got %q, want the unsupported-rule warning", capture.messages())
	}
	if e.level != slog.LevelWarn || e.attrs["rule"] != "cpu" || e.attrs["category"] != "resource" || e.attrs["fault_type"] != "cpu" {
		t.Fatalf("unsupported record = %v %v, want a Warn for rule cpu, category resource, fault_type cpu", e.level, e.attrs)
	}
	if got := capture.messages(); len(got) != 2 {
		t.Fatalf("logger got %q, want exactly the two warnings", got)
	}
}

// With no logger option -- tests, and anything that builds the decode by hand --
// the same warnings keep going to slog.Default().
func TestDecodeRuleSet_WithoutALoggerFallsBackToSlogDefault(t *testing.T) {
	out := captureDefaultSlog(t)
	decodeRuleSet(skippedRules(), WithCapabilities(CapInline))
	for _, want := range []string{"rule skipped: decode error", "rule skipped: not executable by this host"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("the default logger wrote %q, want it to carry %q", out.String(), want)
		}
	}
}

// TestDecodeRuleSet_EmptyOutcomesAreNonNil: the status report carries these
// slices as they are, and an empty one must read [] (it clears the previous
// report), never null.
func TestDecodeRuleSet_EmptyOutcomesAreNonNil(t *testing.T) {
	for _, tc := range []struct {
		name     string
		compiled []wire.CompiledRule
	}{
		{"no rules", nil},
		{"only good rules", []wire.CompiledRule{{Name: "lat", Mode: "inline", Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := decodeRuleSet(tc.compiled)
			if out.Rules == nil {
				t.Fatal("Rules must be non-nil")
			}
			b, err := json.Marshal(struct {
				Unsupported []wire.UnsupportedRule `json:"unsupported"`
				Errors      []string               `json:"errors"`
			}{out.Unsupported, out.Errors})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if got, want := string(b), `{"unsupported":[],"errors":[]}`; got != want {
				t.Fatalf("report slices marshal as %s, want %s", got, want)
			}
		})
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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules
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

	out := decodeRuleSet(compiled)
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules

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
	out := decodeRuleSet([]wire.CompiledRule{{
		Name: "throttle-redis", InjectionPoint: "egress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "network", FaultType: "throttle", DurationMs: 5000,
			Network: &wire.NetworkEnvelope{Host: "inline", Target: "redis:6379", Direction: "downstream"},
			Params:  json.RawMessage(`{"bytes_per_sec":1024}`)},
	}}, WithInlineToxicHost(hook))
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 {
		t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported)
	}
	rules := out.Rules
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

// TestDecode_NetworkHostDefaultFollowsTheHost: an empty network.host is
// "inline" on a host that owns proxied connections -- the sidecar, which
// installs WithInlineToxicHost; design §5.6 makes inline the daemon default --
// and "proxy" on the in-process host, which has no such connection.
func TestDecode_NetworkHostDefaultFollowsTheHost(t *testing.T) {
	rule := wire.CompiledRule{Name: "net-default-host", Mode: "background", Fault: &wire.CompiledFault{
		Category: "network", FaultType: "blackhole", DurationMs: 5000, Params: json.RawMessage(`{}`),
		Network: &wire.NetworkEnvelope{Target: "redis"}, // Host deliberately left empty
	}}
	hosted := &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Second}, Delay: time.Millisecond}
	var hostedFor []string
	toxicHost := WithInlineToxicHost(func(name string, env *wire.NetworkEnvelope, toxic network.Toxic, base fault.FaultConfig) (fault.Fault, error) {
		hostedFor = append(hostedFor, name)
		return hosted, nil
	})
	resolver := withNetworkResolver(stubResolver(":19097", "localhost:6379"))

	t.Run("with an inline toxic host the default is inline", func(t *testing.T) {
		hostedFor = nil
		sr, err := decodeCompiledRule(rule, toxicHost)
		if err != nil {
			t.Fatalf("decodeCompiledRule: %v", err)
		}
		if sr.Decision.Fault != hosted || len(hostedFor) != 1 || hostedFor[0] != "net-default-host" {
			t.Fatalf("fault = %T, hosted for %v; want the inline toxic host to build it for net-default-host", sr.Decision.Fault, hostedFor)
		}
	})

	t.Run("inline wins over proxy when the host has both", func(t *testing.T) {
		hostedFor = nil
		sr, err := decodeCompiledRule(rule, toxicHost, resolver)
		if err != nil {
			t.Fatalf("decodeCompiledRule: %v", err)
		}
		if sr.Decision.Fault != hosted || len(hostedFor) != 1 {
			t.Fatalf("fault = %T, hosted for %v; want the inline toxic host", sr.Decision.Fault, hostedFor)
		}
	})

	t.Run("without one the default stays proxy", func(t *testing.T) {
		hostedFor = nil
		sr, err := decodeCompiledRule(rule, resolver)
		if err != nil {
			t.Fatalf("decodeCompiledRule: %v", err)
		}
		if _, ok := sr.Decision.Fault.(*network.Proxy); !ok || len(hostedFor) != 0 {
			t.Fatalf("fault = %T, hosted for %v; want a *network.Proxy", sr.Decision.Fault, hostedFor)
		}

		// And with nothing to resolve the proxy it is skipped as a proxy rule.
		_, err = decodeCompiledRule(rule)
		var unsup *UnsupportedFaultError
		if !errors.As(err, &unsup) || unsup.Category != "network" || unsup.FaultType != "blackhole/proxy" {
			t.Fatalf("decodeCompiledRule error = %v, want *UnsupportedFaultError{network, blackhole/proxy}", err)
		}
	})
}
