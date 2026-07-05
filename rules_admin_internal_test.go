package atropos

import (
	"context"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// TestApplyRuleSetStopsRemovedRuleFaults pins the fault-stop seam (X1):
// rule removal is the platform's stop signal, so replacing the rule set
// must cancel running background faults whose rule dropped out -- however
// the replacement arrives (poll Apply, push endpoint, teardown clear).
func TestApplyRuleSetStopsRemovedRuleFaults(t *testing.T) {
	longFault := func(ctx context.Context) (*fault.Handle, error) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		h := fault.NewHandle(cancel)
		go func() {
			<-ctx.Done()
			h.Send(fault.Result{})
		}()
		return h, nil
	}

	h, deduped, err := defaultRegistry.StartOrJoin("r1", evaluator.DeduplicateByRule, longFault)
	if err != nil || deduped {
		t.Fatalf("seed fault: err=%v deduped=%v", err, deduped)
	}
	hSurvivor, _, err := defaultRegistry.StartOrJoin("r2", evaluator.DeduplicateByRule, longFault)
	if err != nil {
		t.Fatalf("seed survivor fault: %v", err)
	}

	eval := NewStaticEvaluator(
		StaticRule{Name: "r1", Point: Egress},
		StaticRule{Name: "r2", Point: Egress},
	)
	// r1 drops out of the set; r2 stays.
	applyRuleSet(eval, []StaticRule{{Name: "r2", Point: Egress}})

	select {
	case <-h.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("removing rule r1 did not stop its running background fault")
	}
	select {
	case <-hSurvivor.Done():
		t.Fatal("rule r2 stayed in the set; its fault must keep running")
	case <-time.After(100 * time.Millisecond):
	}
	hSurvivor.Stop() // clean up
}
