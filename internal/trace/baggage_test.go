package trace

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/baggage"
)

// The baggage member names are wire names: services downstream (and their
// rules, e.g. a cache-box rule scoped to a workflow) read them by name.
func TestBaggage_WireNames(t *testing.T) {
	for got, want := range map[string]string{
		BaggageWorkflow:     "atropos.workflow",
		BaggageExperimentID: "atropos.experiment_id",
		BaggagePhaseID:      "atropos.phase_id",
	} {
		if got != want {
			t.Fatalf("baggage key %q, want %q", got, want)
		}
	}
}

func TestExperimentFromContext_RoundTrip(t *testing.T) {
	e, p := ExperimentFromContext(WithExperimentBaggage(context.Background(), "exp-2", "ph-9"))
	if e != "exp-2" || p != "ph-9" {
		t.Fatalf("got %q/%q", e, p)
	}
	if e, p := ExperimentFromContext(context.Background()); e != "" || p != "" {
		t.Fatalf("empty context yielded %q/%q", e, p)
	}
}

// Ruling 6: the SDK never drops foreign baggage. WithExperimentBaggage sets
// (or overwrites) its two members and keeps every other member as it was,
// properties included.
func TestWithExperimentBaggage_KeepsForeignMembersAndOverwritesItsOwn(t *testing.T) {
	b, err := baggage.Parse("tenant=acme;region=eu,atropos.workflow=checkout,atropos.experiment_id=old,atropos.phase_id=old")
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithExperimentBaggage(baggage.ContextWithBaggage(context.Background(), b), "exp-1", "ph-1")
	got := baggage.FromContext(ctx)
	if got.Len() != 4 {
		t.Fatalf("baggage = %q, want 4 members", got.String())
	}
	if m := got.Member("tenant"); m.String() != "tenant=acme;region=eu" {
		t.Fatalf("foreign member = %q, want it unchanged", m.String())
	}
	if got.Member(BaggageWorkflow).Value() != "checkout" {
		t.Fatalf("workflow member = %q", got.Member(BaggageWorkflow).Value())
	}
	if e, p := ExperimentFromContext(ctx); e != "exp-1" || p != "ph-1" {
		t.Fatalf("experiment members = %q/%q, want exp-1/ph-1", e, p)
	}
}

// An experiment or phase id that is not a valid baggage value (W3C values
// are percent-encoded on the wire, so any string is) still round-trips.
func TestWithExperimentBaggage_ArbitraryIDsRoundTrip(t *testing.T) {
	ctx := WithExperimentBaggage(context.Background(), "exp 1,=;x", "ph/ü")
	if e, p := ExperimentFromContext(ctx); e != "exp 1,=;x" || p != "ph/ü" {
		t.Fatalf("got %q/%q", e, p)
	}
}

// An empty phase id removes the phase member (one left over from another
// experiment would misattribute the call) rather than writing an empty one;
// an empty experiment id leaves the context as it was.
func TestWithExperimentBaggage_EmptyIDs(t *testing.T) {
	stale, _ := baggage.Parse("atropos.phase_id=old")
	ctx := WithExperimentBaggage(baggage.ContextWithBaggage(context.Background(), stale), "exp-1", "")
	if b := baggage.FromContext(ctx); b.Len() != 1 || b.Member(BaggagePhaseID).Key() != "" || b.Member(BaggageExperimentID).Value() != "exp-1" {
		t.Fatalf("baggage = %q, want only the experiment member", b.String())
	}
	base := context.Background()
	if ctx := WithExperimentBaggage(base, "", "ph-1"); ctx != base {
		t.Fatal("an empty experiment id must leave the context unchanged")
	}
}
