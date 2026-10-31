package instrument

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func TestNewCore_OfflineHasNoPushAndSharesFidelity(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(c.Stop)

	if c.push != nil {
		t.Fatal("offline core must not build a push client")
	}
	if c.drain != nil {
		t.Fatal("offline core must not build a drain tracker")
	}
	if c.cacheBox.Fidelity() != c.fidelity {
		t.Fatal("cache-box must count into the core's fidelity registry")
	}
	if c.Interceptor().CacheBox() != c.cacheBox {
		t.Fatal("interceptor must dispatch cache-box through the core's cache-box")
	}
	if c.registry == nil {
		t.Fatal("core must own a fault registry when none is supplied")
	}
	if c.Service() != "svc" || c.Instance() != "pod-1" {
		t.Fatalf("identity = %q/%q", c.Service(), c.Instance())
	}
}

func TestNewCore_ConnectedBuildsPushAndDrainAndAdoptsRegistry(t *testing.T) {
	reg := interceptor.NewFaultRegistry()
	t.Cleanup(reg.Close)
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", ManteionURL: "http://manteion:8080", Registry: reg})
	t.Cleanup(c.Stop)

	if c.push == nil {
		t.Fatal("connected core must build a push client")
	}
	if c.push.epoch <= 0 {
		t.Fatalf("push client epoch = %d, want the instance start time", c.push.epoch)
	}
	if c.registry != reg {
		t.Fatal("core must adopt the supplied registry, not replace it")
	}
	tg := c.targets(nil)
	if tg.Evaluator != c.host || tg.DemoEval != c.demo || tg.CacheBox != c.cacheBox || tg.Registry != reg || tg.CacheDrain == nil {
		t.Fatalf("targets() must expose the core's components: %+v", tg)
	}
}

func TestCore_HostRulesWinOverAdminSlot(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1"})
	t.Cleanup(c.Stop)
	c.host.SetRules([]evaluator.StaticRule{{
		Name: "host", Point: evaluator.Egress,
		Decision: evaluator.Decision{Name: "host", CacheBox: evaluator.CacheBoxReplay, Reason: "compiled"},
	}})
	c.demo.Set(&evaluator.Decision{Name: "admin", Reason: "admin"}, &wire.FaultRequest{ID: "admin"})

	if d := c.Interceptor().Evaluate(context.Background(), evaluator.Request{Point: evaluator.Egress}); d == nil || d.Name != "host" {
		t.Fatalf("egress decision = %+v, want host rule", d)
	}
	if d := c.Interceptor().Evaluate(context.Background(), evaluator.Request{Point: evaluator.Ingress}); d == nil || d.Reason != "admin" {
		t.Fatalf("ingress decision = %+v, want admin slot", d)
	}
}

// The targets a core hands out carry the core's logger: an /admin/rules push,
// which has no client behind it offline, reports the rules it skips to the
// host's logger.
func TestCore_PushedRulesWarnThroughTheCoresLogger(t *testing.T) {
	capture := &logCapture{}
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", Logger: capture.logger(), Capabilities: []string{CapInline}})
	t.Cleanup(c.Stop)

	body, err := json.Marshal(skippedRules())
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	rulesAdminHandler(c.targets(nil), nil).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))
	if rec.Code != http.StatusUnprocessableEntity { // the malformed rule makes the push a 422; the rest applies
		t.Fatalf("push: %d %s", rec.Code, rec.Body.String())
	}
	if e, ok := capture.find("rule skipped: not executable by this host"); !ok || e.attrs["rule"] != "cpu" {
		t.Fatalf("core logger got %q, want the unsupported-rule warning for rule cpu", capture.messages())
	}
	if e, ok := capture.find("rule skipped: decode error"); !ok || e.attrs["rule"] != "bad" {
		t.Fatalf("core logger got %q, want the decode-error warning for rule bad", capture.messages())
	}
}

// TestApply_PollAndPushSerializeOnTheCoreMutex: every targets() of one core
// carries the same apply mutex, so the manteion client's apply (poll and
// register) and the /admin/rules push never interleave. Interleaved, one
// apply could diff its rule set against a set the other is half-way through
// replacing, and stop or arm the wrong faults.
func TestApply_PollAndPushSerializeOnTheCoreMutex(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(c.Stop)
	poll, push := c.targets(nil), c.targets(nil)
	if poll.ApplyMu == nil || poll.ApplyMu != push.ApplyMu {
		t.Fatalf("targets() must carry one apply mutex per core: %p vs %p", poll.ApplyMu, push.ApplyMu)
	}

	// The arm hook runs inside apply (applyRuleSet arms each background
	// rule): two applies inside it at once is an interleaving.
	var inside, overlaps atomic.Int32
	arm := func(evaluator.InjectionPoint, *evaluator.Decision) {
		if inside.Add(1) > 1 {
			overlaps.Add(1)
		}
		time.Sleep(time.Millisecond)
		inside.Add(-1)
	}
	poll.Arm, push.Arm = arm, arm
	handler := rulesAdminHandler(push, nil)
	rules := []wire.CompiledRule{{Name: "burn", InjectionPoint: "ingress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)}}}
	body, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			doc := wire.RuleSync{Version: 1, Etag: "1-a", Rules: rules, ActiveFaults: []wire.FaultRequest{}}
			if _, err := apply(doc, poll, scopeAll); err != nil {
				t.Errorf("poll apply: %v", err)
			}
		})
		wg.Go(func() {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))
			if rec.Code != http.StatusNoContent {
				t.Errorf("push: %d %s", rec.Code, rec.Body.String())
			}
		})
	}
	wg.Wait()
	if n := overlaps.Load(); n != 0 {
		t.Fatalf("%d applies overlapped: poll and push must serialize on the core's apply mutex", n)
	}
}

// TestApply_NetworkIOGoesOutWithNoApplyLockHeld: applies serialize on the
// client's applyMu and the core's apply mutex, but neither is held across
// network I/O. The drain report of a recording phase a rule set ends goes out
// once the core's mutex -- and, from the sync, the client's too -- is
// released, and so does the status report after a sync apply. Held, a slow
// manteion would stall the poll loop's 304s, the watchdog and every
// /admin/rules push behind the report.
func TestApply_NetworkIOGoesOutWithNoApplyLockHeld(t *testing.T) {
	var mu sync.Mutex
	var locks []*sync.Mutex  // the apply locks each drain and status POST must find free
	var docs []wire.RuleSync // the poll's answers, in order
	posts, held := map[string]int{}, map[string]int{}
	check := func(path string) {
		mu.Lock()
		defer mu.Unlock()
		posts[path]++
		for _, l := range locks {
			if !l.TryLock() {
				held[path]++
				continue
			}
			l.Unlock()
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			check(r.URL.Path)
			_ = json.NewEncoder(w).Encode(wire.DrainReportResponse{Accepted: true})
		case "/api/v1/sdk/status":
			check(r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/sdk/rules":
			mu.Lock()
			doc := docs[0]
			docs = docs[1:]
			mu.Unlock()
			respond200(doc)(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	logger := slog.New(slog.DiscardHandler)
	core := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", ManteionURL: srv.URL, Logger: logger})
	t.Cleanup(core.Stop)
	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc", instanceID: "pod-1", pollInterval: 10 * time.Second},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    core.targets(nil),
		logger:     logger,
	}
	recording := []wire.CompiledRule{recordingRule("r1", "exp-1", "phase-1")}
	mu.Lock()
	locks = []*sync.Mutex{&core.applyMu, &c.applyMu}
	docs = []wire.RuleSync{
		{Version: 1, Etag: "1-a", Rules: recording, ActiveFaults: []wire.FaultRequest{}},
		{Version: 2, Etag: "2-b", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}}, // the phase's recording ends
	}
	mu.Unlock()
	for range 2 {
		if err := c.fetchRules(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	// The push records, then tears down: its drain report goes out with the
	// core's mutex released too.
	push := rulesAdminHandler(core.targets(nil), c)
	body, err := json.Marshal(recording)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{string(body), "null"} {
		rec := httptest.NewRecorder()
		push.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", strings.NewReader(b)))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("push %s: %d %s", b, rec.Code, rec.Body.String())
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if posts["/api/v1/sdk/cachebox/drain"] != 2 || posts["/api/v1/sdk/status"] != 2 {
		t.Fatalf("POSTs = %v, want 2 drain reports (the sync's and the push's teardown, each once) and 2 status reports (one per sync apply)", posts)
	}
	if len(held) != 0 {
		t.Fatalf("POSTs that found an apply lock held: %v, want none", held)
	}
}
