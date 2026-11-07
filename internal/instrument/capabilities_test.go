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
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// unsupportedCount sums atropos_rules_unsupported_total{category} over every
// service: the targets most tests build by hand carry no service.
func unsupportedCount(t *testing.T, category string) float64 {
	t.Helper()
	return unsupportedCountFor(t, category, "")
}

// unsupportedCountFor reads atropos_rules_unsupported_total{category,
// service}; an empty service sums over every service.
func unsupportedCountFor(t *testing.T, category, service string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, f := range families {
		if f.GetName() != "atropos_rules_unsupported_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			var cat, svc string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "category":
					cat = l.GetValue()
				case "service":
					svc = l.GetValue()
				}
			}
			if cat == category && (service == "" || svc == service) {
				sum += m.GetCounter().GetValue()
			}
		}
	}
	return sum
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

// The core labels the rules and slots it skips with its service: the
// manteion sync and the /admin/rules push both apply through core.targets.
func TestApply_UnsupportedCountsCarryTheCoreService(t *testing.T) {
	core := NewCore(CoreConfig{Service: "unsup-svc", Instance: "pod-1", Logger: slog.New(slog.DiscardHandler),
		Capabilities: []string{CapInline, CapCacheBox}})
	t.Cleanup(core.Stop)
	before := unsupportedCountFor(t, "resource", "unsup-svc")
	_, err := apply(wire.RuleSync{Rules: threeRules(), ActiveFaults: []wire.FaultRequest{
		{ID: "slot-cpu", Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)},
	}}, core.targets(nil), scopeAll)
	if err != nil {
		t.Fatal(err)
	}
	if got := unsupportedCountFor(t, "resource", "unsup-svc") - before; got != 2 {
		t.Fatalf("atropos_rules_unsupported_total{resource,service=unsup-svc} += %v, want 2 (the rule and the slot)", got)
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

func netemRule(name, faultType, params string) wire.CompiledRule {
	return wire.CompiledRule{Name: name, InjectionPoint: "egress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "network", FaultType: faultType, DurationMs: 5000,
			Network: &wire.NetworkEnvelope{Host: "inline", Target: "productcatalogservice:3550", Direction: "upstream"},
			Params:  json.RawMessage(params)}}
}

func TestDecode_NetemRoutesToTheNetworkHost(t *testing.T) {
	var got []string
	stub := &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Second}, Delay: time.Millisecond}
	host := func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		got = append(got, name+"/"+req.FaultType+"/"+req.Network.Target)
		if base.Duration != 5*time.Second {
			t.Errorf("base duration = %v", base.Duration)
		}
		return stub, nil
	}
	lat := netemRule("lat-toxic", "latency", `{"delay":"100ms"}`)
	lat.Fault.Network.Host = "proxy" // the in-process toxic path, through the resolver
	out := decodeRuleSet([]wire.CompiledRule{
		netemRule("lat-netem", "latency", `{"delay":"100ms","mechanism":"netem"}`),
		netemRule("loss", "packet_loss", `{"loss":10}`),
		lat,
	}, WithNetworkHost(host), withNetworkResolver(stubResolver(":19090", "localhost:3550")))
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 || len(out.Rules) != 3 {
		t.Fatalf("rules=%d errors=%v unsupported=%v", len(out.Rules), out.Errors, out.Unsupported)
	}
	if strings.Join(got, ",") != "lat-netem/latency/productcatalogservice:3550,loss/packet_loss/productcatalogservice:3550" {
		t.Fatalf("hook calls = %v", got)
	}
	if out.Rules[0].Decision.Fault != stub || out.Rules[1].Decision.Fault != stub {
		t.Fatal("the hook's fault must be what the netem rules carry")
	}
	if _, ok := out.Rules[2].Decision.Fault.(*network.Proxy); !ok {
		t.Fatalf("latency without mechanism must stay a toxic proxy fault, got %T", out.Rules[2].Decision.Fault)
	}
}

func TestDecode_NetemWithoutANetworkHostIsSkippedAndCounted(t *testing.T) {
	var logs strings.Builder
	before := unsupportedCount(t, "network")
	out := decodeRuleSet([]wire.CompiledRule{netemRule("loss", "packet_loss", `{"loss":10}`)},
		withNetworkResolver(stubResolver(":19090", "localhost:3550")),
		withLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	counted := unsupportedCount(t, "network") - before
	if len(out.Errors) != 0 || len(out.Rules) != 0 || counted != 1 {
		t.Fatalf("rules=%d errors=%v counted=%v (netem needs the node agent; never a sync failure)", len(out.Rules), out.Errors, counted)
	}
	if len(out.Unsupported) != 1 || out.Unsupported[0] != (wire.UnsupportedRule{Rule: "loss", Category: "network"}) {
		t.Fatalf("unsupported = %+v, want the netem rule reported under network", out.Unsupported)
	}
	if !strings.Contains(logs.String(), "fault_type=packet_loss/netem") {
		t.Fatalf("skip log must name the netem fault type, got %q", logs.String())
	}
}

func TestDecode_NetemNeedsTheNetemCapability(t *testing.T) {
	host := func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		return &inline.Latency{FaultConfig: base, Delay: time.Millisecond}, nil
	}
	rule := []wire.CompiledRule{netemRule("loss", "packet_loss", `{"loss":10}`)}
	var logs strings.Builder
	before := unsupportedCount(t, "network")
	out := decodeRuleSet(rule, WithNetworkHost(host), WithCapabilities(CapInline, CapNetwork, CapCacheBox),
		withLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	counted := unsupportedCount(t, "network") - before
	if len(out.Errors) != 0 || len(out.Rules) != 0 || counted != 1 ||
		len(out.Unsupported) != 1 || out.Unsupported[0] != (wire.UnsupportedRule{Rule: "loss", Category: "network"}) {
		t.Fatalf("without CapNetem: rules=%d errors=%v unsupported=%v counted=%v", len(out.Rules), out.Errors, out.Unsupported, counted)
	}
	if !strings.Contains(logs.String(), "fault_type=packet_loss/netem") {
		t.Fatalf("skip log must name the netem fault type, got %q", logs.String())
	}
	out = decodeRuleSet(rule, WithNetworkHost(host), WithCapabilities(CapInline, CapNetwork, CapCacheBox, CapNetem))
	if len(out.Errors) != 0 || len(out.Unsupported) != 0 || len(out.Rules) != 1 {
		t.Fatalf("with CapNetem: rules=%d errors=%v unsupported=%v", len(out.Rules), out.Errors, out.Unsupported)
	}
	for _, c := range AllCapabilities() {
		if c == CapNetem {
			t.Fatal("the in-process host must never advertise netem")
		}
	}
}
