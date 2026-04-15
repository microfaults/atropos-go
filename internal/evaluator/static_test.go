package evaluator

import (
	"context"
	"sync"
	"testing"
)

func TestStaticEvaluator_NoMatch(t *testing.T) {
	e := NewStaticEvaluator(StaticRule{
		Name:  "egress-rule",
		Point: Egress,
	})
	d := e.Evaluate(context.Background(), Request{Point: Ingress})
	if d != nil {
		t.Fatalf("expected nil decision on point mismatch, got %+v", d)
	}
}

func TestStaticEvaluator_PointMatchWildcardLabels(t *testing.T) {
	want := Decision{Reason: "wildcard", CacheBox: CacheBoxReplay}
	e := NewStaticEvaluator(StaticRule{
		Name:     "wildcard",
		Point:    Egress,
		Labels:   nil,
		Decision: want,
	})
	got := e.Evaluate(context.Background(), Request{
		Point:  Egress,
		Labels: map[string]string{"any": "thing"},
	})
	if got == nil {
		t.Fatal("expected match on wildcard labels")
	}
	if got.Reason != want.Reason || got.CacheBox != want.CacheBox {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestStaticEvaluator_LabelAndSemantics(t *testing.T) {
	e := NewStaticEvaluator(StaticRule{
		Name:  "narrow",
		Point: Egress,
		Labels: map[string]string{
			"atropos.http.host": "productcatalog:3550",
			"atropos.workflow":  "browse",
		},
		Decision: Decision{Reason: "narrow match", CacheBox: CacheBoxReplay},
	})

	// Missing a required label -> no match.
	got := e.Evaluate(context.Background(), Request{
		Point:  Egress,
		Labels: map[string]string{"atropos.http.host": "productcatalog:3550"},
	})
	if got != nil {
		t.Fatal("expected nil when only one label matches")
	}

	// Wrong value on a required label -> no match.
	got = e.Evaluate(context.Background(), Request{
		Point: Egress,
		Labels: map[string]string{
			"atropos.http.host": "productcatalog:3550",
			"atropos.workflow":  "checkout",
		},
	})
	if got != nil {
		t.Fatal("expected nil when label value mismatches")
	}

	// All labels present and matching -> match.
	got = e.Evaluate(context.Background(), Request{
		Point: Egress,
		Labels: map[string]string{
			"atropos.http.host": "productcatalog:3550",
			"atropos.workflow":  "browse",
			"extra.label":       "ignored", // extras are OK
		},
	})
	if got == nil {
		t.Fatal("expected match when all labels satisfied")
	}
	if got.CacheBox != CacheBoxReplay {
		t.Fatalf("unexpected decision: %+v", got)
	}
}

func TestStaticEvaluator_FirstMatchWins(t *testing.T) {
	e := NewStaticEvaluator(
		StaticRule{
			Name:     "first",
			Point:    Egress,
			Labels:   map[string]string{"svc": "payments"},
			Decision: Decision{Reason: "first", CacheBox: CacheBoxReplay},
		},
		StaticRule{
			Name:     "second",
			Point:    Egress,
			Labels:   map[string]string{"svc": "payments"},
			Decision: Decision{Reason: "second", CacheBox: CacheBoxPassthrough},
		},
	)
	d := e.Evaluate(context.Background(), Request{
		Point:  Egress,
		Labels: map[string]string{"svc": "payments"},
	})
	if d == nil || d.Reason != "first" {
		t.Fatalf("expected first match, got %+v", d)
	}
}

func TestStaticEvaluator_DecisionCopyIsolated(t *testing.T) {
	// Mutating the returned decision must not affect subsequent evaluations.
	e := NewStaticEvaluator(StaticRule{
		Name:     "copy-test",
		Point:    Egress,
		Decision: Decision{Reason: "original", CacheBox: CacheBoxReplay},
	})
	got := e.Evaluate(context.Background(), Request{Point: Egress})
	if got == nil {
		t.Fatal("expected match")
	}
	got.Reason = "mutated"
	got.CacheBox = CacheBoxPassthrough

	again := e.Evaluate(context.Background(), Request{Point: Egress})
	if again == nil {
		t.Fatal("expected second match")
	}
	if again.Reason != "original" || again.CacheBox != CacheBoxReplay {
		t.Fatalf("decision mutation leaked back into evaluator: %+v", again)
	}
}

func TestStaticEvaluator_SetRulesAtomic(t *testing.T) {
	e := NewStaticEvaluator(StaticRule{
		Name:     "initial",
		Point:    Egress,
		Decision: Decision{Reason: "initial"},
	})

	// Concurrent evaluators + one writer should not race or panic.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = e.Evaluate(context.Background(), Request{Point: Egress})
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		e.SetRules([]StaticRule{{
			Name:     "writer",
			Point:    Egress,
			Decision: Decision{Reason: "writer"},
		}})
	}
	close(stop)
	wg.Wait()

	got := e.Evaluate(context.Background(), Request{Point: Egress})
	if got == nil || got.Reason != "writer" {
		t.Fatalf("expected final decision from writer, got %+v", got)
	}
}

func TestStaticEvaluator_InputRulesSliceIsolated(t *testing.T) {
	// Mutating the input slice after construction must not affect the evaluator.
	input := []StaticRule{{
		Name:     "original",
		Point:    Egress,
		Decision: Decision{Reason: "original"},
	}}
	e := NewStaticEvaluator(input...)
	input[0].Decision.Reason = "mutated"

	d := e.Evaluate(context.Background(), Request{Point: Egress})
	if d == nil || d.Reason != "original" {
		t.Fatalf("input mutation leaked into evaluator: %+v", d)
	}
}
