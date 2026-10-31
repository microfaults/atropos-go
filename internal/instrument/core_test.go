package instrument

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	handler := rulesAdminHandler(push)
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
