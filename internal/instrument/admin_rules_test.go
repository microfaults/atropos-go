package instrument

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func TestRulesAdminHandler(t *testing.T) {
	t.Run("GET on empty evaluator returns 200 with empty array", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(applyTargets{Evaluator: eval})

		req := httptest.NewRequest(http.MethodGet, "/admin/rules", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var got []wire.StaticRule
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty rule list, got %d rules", len(got))
		}
	})

	t.Run("POST with valid compiled rules replaces evaluator rules", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(applyTargets{Evaluator: eval})

		compiled := wire.CompiledRule{
			Name:           "freeze-productcatalog",
			InjectionPoint: "egress",
			Mode:           "inline",
			CacheBox:       &wire.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"},
		}
		body, err := json.Marshal([]wire.CompiledRule{compiled})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
		}

		rules := eval.Rules()
		if len(rules) != 1 {
			t.Fatalf("expected 1 rule after POST, got %d", len(rules))
		}
		if rules[0].Name != compiled.Name {
			t.Fatalf("expected rule name %q, got %q", compiled.Name, rules[0].Name)
		}
		if rules[0].Point != evaluator.Egress {
			t.Fatalf("expected rule point %v, got %v", evaluator.Egress, rules[0].Point)
		}
		if rules[0].Decision.CacheBox != evaluator.CacheBoxReplay {
			t.Fatalf("expected evaluator.CacheBoxReplay, got %v", rules[0].Decision.CacheBox)
		}
	})

	t.Run("POST with malformed JSON returns 400", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(applyTargets{Evaluator: eval})

		req := httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader([]byte(`"not json"`)))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("POST without an Evaluator target is a 500", func(t *testing.T) {
		handler := rulesAdminHandler(applyTargets{}) // a host that cannot hold rules
		body := `[{"name":"r","injection_point":"egress","mode":"inline"}]`

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", strings.NewReader(body)))

		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "apply rules: ") {
			t.Fatalf("expected 500 naming the hard error, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("POST skips a network rule this host cannot run and applies the rest", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(applyTargets{Evaluator: eval}) // no NetworkResolver, no inline toxic host

		body, err := json.Marshal([]wire.CompiledRule{
			{Name: "net-proxy", Mode: "background", Fault: &wire.CompiledFault{
				Category: "network", FaultType: "blackhole", DurationMs: 5000, Params: json.RawMessage(`{}`),
				Network: &wire.NetworkEnvelope{Host: "proxy", Target: "redis"}}},
			{Name: "net-inline", Mode: "background", Fault: &wire.CompiledFault{
				Category: "network", FaultType: "latency", DurationMs: 5000, Params: json.RawMessage(`{"delay":"50ms"}`),
				Network: &wire.NetworkEnvelope{Host: "inline", Target: "redis"}}},
			{Name: "lat", Mode: "inline", Fault: &wire.CompiledFault{
				Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		before := unsupportedCount(t, "network")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
		}
		if rules := eval.Rules(); len(rules) != 1 || rules[0].Name != "lat" {
			t.Fatalf("installed rules = %+v, want only the supported one", rules)
		}
		if got := unsupportedCount(t, "network") - before; got != 2 {
			t.Fatalf("atropos_rules_unsupported_total{network} += %v, want 2", got)
		}
	})

	t.Run("DELETE returns 405", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(applyTargets{Evaluator: eval})

		req := httptest.NewRequest(http.MethodDelete, "/admin/rules", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

// TestRulesAdmin_PostPartialDecodeIs422ButAppliesTheRest: a pushed rule
// that does not decode is skipped per rule, as a polled one is. The rules
// that decode replace the set, and the 422 names the ones that did not.
func TestRulesAdmin_PostPartialDecodeIs422ButAppliesTheRest(t *testing.T) {
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "old", Point: evaluator.Ingress})
	handler := rulesAdminHandler(applyTargets{Evaluator: eval})
	body, err := json.Marshal([]wire.CompiledRule{
		{Name: "good", Mode: "inline", Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
		{Name: "bad", Mode: "inline", Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"notaduration"}`)}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || !strings.Contains(resp.Error, "rule bad: decode:") {
		t.Fatalf("body = %s (%v), want the skipped rule named", rec.Body.String(), err)
	}
	if rules := eval.Rules(); len(rules) != 1 || rules[0].Name != "good" {
		t.Fatalf("rules = %+v, want only the decodable rule, replacing the old set", rules)
	}
}

// TestRulesAdmin_PostTouchesOnlyRules pins the push's scope (Review Focus 5):
// a rule set pushed to /admin/rules goes through apply, but only its rules
// branch -- admin fault slots, the delay source and the installed replay set
// stay exactly as they were.
func TestRulesAdmin_PostTouchesOnlyRules(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	t.Cleanup(cb.Stop)
	cb.InstallReplaySet(cachebox.PhaseKey("exp-1", "phase-1"), map[string]*cachebox.Entry{"k": {Key: "k", StatusCode: 200}})
	cb.SetDelaySource(cachebox.NewDistributionDelaySource(1, 0.5, 1))
	demo := &demoEvaluator{}
	demo.Set(&evaluator.Decision{Name: "admin", Reason: "admin"}, &wire.FaultRequest{ID: "admin"})
	// A puller too: under any wider scope its Reconcile(nil) would clear the set.
	p := newReplaySetPuller(replaySetPullerConfig{BaseURL: "http://manteion.invalid", CacheBox: cb, Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(p.Close)
	handler := rulesAdminHandler(applyTargets{Evaluator: evaluator.NewStaticEvaluator(), DemoEval: demo, CacheBox: cb, ReplaySet: p})

	body, _ := json.Marshal([]wire.CompiledRule{{Name: "r", InjectionPoint: "egress", Mode: "inline", CacheBox: &wire.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"}}})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body.String())
	}
	if len(demo.ActiveIDs()) != 1 || cb.ReplaySetLen() != 1 || !cb.Stats().Delay.Installed {
		t.Fatalf("rules push must not touch slots/replay set/delay: slots=%v replay=%d delay=%+v", demo.ActiveIDs(), cb.ReplaySetLen(), cb.Stats().Delay)
	}
}

// TestRulesAdminHandler_PostObservesDrainTracker pins the single-apply-path
// contract (design review §5): a rule set pushed to /admin/rules drives the
// drain tracker exactly as a polled set does. Teardown pushes `null`
// (clear everything), which is the common drain trigger.
func TestRulesAdminHandler_PostObservesDrainTracker(t *testing.T) {
	var drains atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			drains.Add(1)
			_ = json.NewEncoder(w).Encode(wire.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	pusher := newCachePushClient(cachePushConfig{BaseURL: server.URL, Service: "cart", Instance: "pod-1"})
	defer pusher.Stop()
	cb := cachebox.New(cachebox.Config{Push: pusher.PushFunc()})
	defer cb.Stop()
	pusher.BindFidelity(cb.Fidelity())

	eval := evaluator.NewStaticEvaluator()
	handler := rulesAdminHandler(applyTargets{Evaluator: eval, CacheDrain: newCacheDrainTracker(cb, pusher, nil)})
	post := func(body string) int {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", strings.NewReader(body)))
		return rec.Code
	}
	recording, _ := json.Marshal([]wire.CompiledRule{recordingRule("r1", "exp-1", "phase-1")})

	if code := post(string(recording)); code != http.StatusNoContent {
		t.Fatalf("push recording rule: %d", code)
	}
	if got := drains.Load(); got != 0 {
		t.Fatalf("no phase ended yet, got %d drain reports", got)
	}
	// Teardown: manteion pushes null. The set is cleared AND the phase's
	// recording ends, exactly as the empty authoritative set does on poll.
	if code := post("null"); code != http.StatusNoContent {
		t.Fatalf("push null: %d", code)
	}
	if got := len(eval.Rules()); got != 0 {
		t.Fatalf("null push must clear the rule set, still %d rules", got)
	}
	if got := drains.Load(); got != 1 {
		t.Fatalf("null push must fire the drain report once, got %d", got)
	}
	// An explicit empty array is the same authoritative clear.
	if code := post(string(recording)); code != http.StatusNoContent {
		t.Fatalf("re-push recording rule: %d", code)
	}
	if code := post("[]"); code != http.StatusNoContent {
		t.Fatalf("push []: %d", code)
	}
	if got := drains.Load(); got != 2 {
		t.Fatalf("[] push must fire the drain report, got %d in total", got)
	}
}

// TestRulesAdminHandler_PostArmsEagerly: eager arming rides the same apply
// path, so an eagerly-arming host (the sidecar) gets its background faults
// started by a push exactly as by a poll -- once.
func TestRulesAdminHandler_PostArmsEagerly(t *testing.T) {
	var armed []string
	arm := func(point evaluator.InjectionPoint, d *evaluator.Decision) {
		armed = append(armed, point.String()+":"+d.Name)
	}
	eval := evaluator.NewStaticEvaluator()
	handler := rulesAdminHandler(applyTargets{Evaluator: eval, Arm: arm})
	body, _ := json.Marshal([]wire.CompiledRule{{
		Name: "burn", InjectionPoint: "ingress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)},
	}})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rec.Code, rec.Body.String())
	}
	if len(armed) != 1 || armed[0] != "ingress:burn" {
		t.Fatalf("armed = %v, want exactly [ingress:burn]", armed)
	}
}
