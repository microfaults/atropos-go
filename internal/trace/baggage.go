package trace

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
)

// W3C Baggage members the SDK reads and writes. They are wire names: the
// services downstream read them by name.
const (
	// BaggageWorkflow names the request's workflow (e.g. "browse",
	// "checkout"); the interceptor copies it into the evaluator labels as
	// AttrCacheBoxWorkflow, so a rule can be scoped to one workflow.
	BaggageWorkflow = "atropos.workflow"
	// BaggageExperimentID and BaggagePhaseID name the experiment and phase a
	// request belongs to. The interceptor sets them on an outbound call once
	// a cache-box decision with an experiment applies to it
	// (WithExperimentBaggage), and a decision event without a cache-box
	// context of its own falls back to them (ExperimentFromContext).
	BaggageExperimentID = "atropos.experiment_id"
	BaggagePhaseID      = "atropos.phase_id"
)

// WithExperimentBaggage returns ctx with its baggage's BaggageExperimentID
// set to experimentID and BaggagePhaseID to phaseID, overwriting members
// already there. Every other member is kept as it was: the SDK never drops
// foreign baggage. An empty phaseID removes the phase member (a phase left
// over from another experiment would misattribute the call); an empty
// experimentID returns ctx unchanged. The values are raw strings, so any id
// round-trips (the propagator percent-encodes them on the wire).
func WithExperimentBaggage(ctx context.Context, experimentID, phaseID string) context.Context {
	if experimentID == "" {
		return ctx
	}
	b := baggage.FromContext(ctx)
	// The keys are valid constants, so NewMemberRaw fails only on a value
	// that is not valid UTF-8 (that member is skipped), and SetMember fails
	// only on a member NewMemberRaw rejected: its error is unreachable here.
	if m, err := baggage.NewMemberRaw(BaggageExperimentID, experimentID); err == nil {
		b, _ = b.SetMember(m)
	}
	if phaseID == "" {
		b = b.DeleteMember(BaggagePhaseID)
	} else if m, err := baggage.NewMemberRaw(BaggagePhaseID, phaseID); err == nil {
		b, _ = b.SetMember(m)
	}
	return baggage.ContextWithBaggage(ctx, b)
}

// ExperimentFromContext returns the BaggageExperimentID and BaggagePhaseID
// members of ctx's baggage; "" for a member that is absent.
func ExperimentFromContext(ctx context.Context) (experimentID, phaseID string) {
	b := baggage.FromContext(ctx)
	return b.Member(BaggageExperimentID).Value(), b.Member(BaggagePhaseID).Value()
}
