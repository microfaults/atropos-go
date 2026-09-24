package instrument

import (
	"context"
	"log/slog"
	"testing"

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
