package instrument

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"

	dto "github.com/prometheus/client_model/go"
)

func unsupportedCount(t *testing.T, category string) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := metrics.RulesUnsupportedTotal.WithLabelValues(category).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func threeRules() []wire.CompiledRule {
	return []wire.CompiledRule{
		{Name: "lat", InjectionPoint: "ingress", Mode: "inline",
			Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}},
		{Name: "cpu", InjectionPoint: "ingress", Mode: "background",
			Fault: &wire.CompiledFault{Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)}},
		{Name: "cb", InjectionPoint: "egress", Mode: "inline",
			CacheBox: &wire.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"}},
	}
}

func TestDecode_UnsupportedCategoryIsSkippedAndCounted(t *testing.T) {
	before := unsupportedCount(t, "resource")
	out := decodeRuleSet(threeRules(), WithCapabilities(CapInline, CapCacheBox))
	if len(out.Errors) != 0 {
		t.Fatalf("decode: errors=%v", out.Errors)
	}
	rules := out.Rules
	if len(rules) != 2 || rules[0].Name != "lat" || rules[1].Name != "cb" {
		t.Fatalf("rules = %+v, want the resource rule skipped", rules)
	}
	if len(out.Unsupported) != 1 || out.Unsupported[0] != (wire.UnsupportedRule{Rule: "cpu", Category: "resource"}) {
		t.Fatalf("unsupported = %+v, want the cpu rule reported under resource", out.Unsupported)
	}
	if got := unsupportedCount(t, "resource") - before; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{resource} += %v, want 1", got)
	}
}

func TestDecode_NoCapabilitiesMeansEverythingSupported(t *testing.T) {
	out := decodeRuleSet(threeRules())
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 || len(out.Rules) != 3 {
		t.Fatalf("rules=%d errors=%v unsupported=%v, want all three", len(out.Rules), out.Errors, out.Unsupported)
	}
}

func TestApply_UnsupportedActiveFaultDoesNotWedgeSync(t *testing.T) {
	demo := &demoEvaluator{}
	targets := applyTargets{
		Evaluator: newTestEvaluator(), DemoEval: demo, Registry: interceptor.NewFaultRegistry(),
		DecodeOpts: []decodeOption{WithCapabilities(CapInline)},
	}
	resp := wire.RegisterResponse{RuleSync: wire.RuleSync{
		Rules: threeRules(),
		ActiveFaults: []wire.FaultRequest{
			{ID: "slot-cpu", Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)},
			{ID: "slot-lat", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)},
		},
	}}
	report, err := apply(resp.RuleSync, targets, scopeAll)
	if err != nil {
		t.Fatalf("apply must not fail on an unsupported category: %v", err)
	}
	if ids := demo.ActiveIDs(); len(ids) != 1 || ids[0] != "slot-lat" {
		t.Fatalf("active slots = %v, want only the supported one", ids)
	}
	if got := len(targets.Evaluator.Rules()); got != 2 {
		t.Fatalf("rules applied = %d, want 2 (resource rule skipped)", got)
	}
	// Rules first, then slots; a slot is reported under its id.
	want := []wire.UnsupportedRule{{Rule: "cpu", Category: "resource"}, {Rule: "slot-cpu", Category: "resource"}}
	if !reflect.DeepEqual(report.Unsupported, want) || len(report.Errors) != 0 {
		t.Fatalf("report = %+v, want unsupported %+v and no errors", report, want)
	}
}

func TestRegister_CarriesHostAndCapabilities(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sdk/register" { // e.g. the status report register sends after applying
			w.WriteHeader(http.StatusNoContent)
			return
		}
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(wire.RegisterResponse{Status: "registered"})
	}))
	defer srv.Close()

	c := &ManteionClient{
		cfg: manteionConfig{url: srv.URL, serviceName: "svc", instanceID: "pod-1", pollInterval: 10 * time.Second,
			host: HostSidecar, capabilities: []string{CapInline, CapNetwork, CapCacheBox}},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    newTestTargets(),
		logger:     slog.New(slog.DiscardHandler),
	}
	if err := c.register(t.Context()); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !strings.Contains(string(body), `"host":"sidecar"`) || !strings.Contains(string(body), `"capabilities":["inline","network","cachebox"]`) {
		t.Fatalf("register body = %s, want host and capabilities", body)
	}

	c.cfg.host, c.cfg.capabilities = "", nil
	if err := c.register(t.Context()); err != nil {
		t.Fatalf("register: %v", err)
	}
	if strings.Contains(string(body), `"host"`) || strings.Contains(string(body), `"capabilities"`) {
		t.Fatalf("unset host/capabilities must be omitted: %s", body)
	}
}

func TestHealthHandler_StampsHostAndCapture(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthHandler(nil, HostSidecar, "transparent").ServeHTTP(rec, httptest.NewRequest("GET", "/atropos/health", nil))
	var h wire.HealthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 200 || h.Status != "offline" || h.Host != "sidecar" || h.Capture != "transparent" {
		t.Fatalf("code=%d health=%+v", rec.Code, h)
	}
}

func TestDecode_QuotaMechanismNeedsAResourceHost(t *testing.T) {
	rule := []wire.CompiledRule{{Name: "cpu-quota", InjectionPoint: "ingress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "resource", FaultType: "cpu", DurationMs: 1000,
			Params: json.RawMessage(`{"target_load":0.5,"mechanism":"quota"}`)}}}

	// In-process: skipped and counted, not executed, not a sync failure.
	before := unsupportedCount(t, "resource")
	out := decodeRuleSet(rule)
	counted := unsupportedCount(t, "resource") - before
	if len(out.Errors) != 0 || len(out.Rules) != 0 || counted != 1 {
		t.Fatalf("rules=%d errors=%v counted=%v", len(out.Rules), out.Errors, counted)
	}
	if len(out.Unsupported) != 1 || out.Unsupported[0] != (wire.UnsupportedRule{Rule: "cpu-quota", Category: "resource"}) {
		t.Fatalf("unsupported = %+v, want the quota rule reported under resource", out.Unsupported)
	}

	// With a resource host every resource fault goes through it.
	var gotName, gotMech string
	host := func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		gotName, gotMech = name, resourceMechanism(req.Params)
		return &inline.Latency{FaultConfig: base, Delay: time.Millisecond}, nil
	}
	out = decodeRuleSet(rule, WithResourceHost(host))
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 || len(out.Rules) != 1 || gotName != "cpu-quota" || gotMech != "quota" {
		t.Fatalf("rules=%d errors=%v unsupported=%v name=%q mech=%q", len(out.Rules), out.Errors, out.Unsupported, gotName, gotMech)
	}
}
