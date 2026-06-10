package faultparams

import (
	"encoding/json"
	"testing"
)

// TestRegistryCoverage pins the catalogue to exactly the fault types the SDK
// decoders dispatch on (compiled_rule.go). A new decoder case must land here
// in the same change, and vice versa.
func TestRegistryCoverage(t *testing.T) {
	want := map[string][]string{
		"inline":   {"error", "hang", "latency"},
		"network":  {"blackhole", "drip", "latency", "retransmit_delay", "rst", "throttle"},
		"resource": {"cpu", "disk", "io", "memory"},
	}
	got := map[string][]string{}
	for _, s := range All() {
		got[s.Category] = append(got[s.Category], s.Type)
	}
	for cat, types := range want {
		if len(got[cat]) != len(types) {
			t.Fatalf("category %s: got %v, want %v", cat, got[cat], types)
		}
		for i, typ := range types {
			if got[cat][i] != typ {
				t.Errorf("category %s[%d]: got %s, want %s", cat, i, got[cat][i], typ)
			}
			if _, ok := Lookup(cat, typ); !ok {
				t.Errorf("Lookup(%s, %s) not found", cat, typ)
			}
		}
	}
	if _, ok := Lookup("inline", "explode"); ok {
		t.Error("Lookup should reject unknown types")
	}
}

// TestValidateRaw_GoldenFixtures decodes the exact JSON shapes the SDK
// decoders accept (fixtures lifted from compiled_rule_test/admin_test usage).
func TestValidateRaw_GoldenFixtures(t *testing.T) {
	good := []struct{ cat, typ, raw string }{
		{"inline", "latency", `{"delay":"200ms","jitter":"50ms"}`},
		{"inline", "latency", `{"delay":"1.5s"}`},
		{"inline", "error", `{"status_code":503,"message":"service down"}`},
		{"inline", "error", `{}`}, // all-defaults: SDK fills 500/"injected fault"
		{"inline", "hang", `{"duration":"2s"}`},
		{"network", "latency", `{"delay":"100ms"}`},
		{"network", "retransmit_delay", `{"rate":0.3,"delay":"200ms","reset_threshold":5}`},
		{"network", "blackhole", `{}`},
		{"network", "blackhole", ``}, // absent params
		{"network", "drip", `{"chunk_size":1,"interval":"10ms"}`},
		{"network", "rst", `{"after_bytes":1024,"after_duration":"1s"}`},
		{"network", "throttle", `{"bytes_per_sec":1024}`},
		{"resource", "cpu", `{"target_load":0.7,"window":"100ms"}`},
		{"resource", "memory", `{"target_load":0.5,"chunk_size":1048576,"thrashing":true,"thrash_workers":2}`},
		{"resource", "disk", `{"write_rate":1048576,"max_disk_usage":10485760,"chunk_size":4096,"path":"/tmp"}`},
		{"resource", "io", `{"read_rate":102400,"file_size":4096,"file_count":16,"workers":4,"mode":"read_write"}`},
	}
	for _, c := range good {
		if err := ValidateRaw(c.cat, c.typ, json.RawMessage(c.raw)); err != nil {
			t.Errorf("ValidateRaw(%s/%s, %s) = %v, want nil", c.cat, c.typ, c.raw, err)
		}
	}

	bad := []struct{ cat, typ, raw, why string }{
		{"inline", "latency", `{"delay":"not-a-duration"}`, "unparseable delay"},
		{"inline", "latency", `{}`, "missing required delay"},
		{"inline", "latency", `{"delay":"100ms","dleay":"oops"}`, "unknown field"},
		{"inline", "error", `{"status_code":42}`, "status outside 100..599"},
		{"inline", "hang", `{}`, "missing duration"},
		{"network", "retransmit_delay", `{"rate":1.5}`, "rate > 1"},
		{"network", "throttle", `{"bytes_per_sec":0}`, "zero rate"},
		{"resource", "cpu", `{"target_load":0}`, "target_load 0"},
		{"resource", "cpu", `{"target_load":1.2}`, "target_load > 1"},
		{"resource", "io", `{"mode":"sideways"}`, "unknown io mode"},
		{"inline", "explode", `{}`, "unknown fault type"},
	}
	for _, c := range bad {
		if err := ValidateRaw(c.cat, c.typ, json.RawMessage(c.raw)); err == nil {
			t.Errorf("ValidateRaw(%s/%s, %s) = nil, want error (%s)", c.cat, c.typ, c.raw, c.why)
		}
	}
}
