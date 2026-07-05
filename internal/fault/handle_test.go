package fault

import (
	"context"
	"testing"
)

// TestHandleCallbacksCompose pins that SetOnResult accumulates: the
// interceptor's span-recording callback and the registry's bookkeeping
// callback share one handle and must both fire.
func TestHandleCallbacksCompose(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	h := NewHandle(cancel)

	var order []string
	h.SetOnResult(func(Result) { order = append(order, "span") })
	h.SetOnResult(func(Result) { order = append(order, "registry") })
	h.Send(Result{})

	if len(order) != 2 || order[0] != "span" || order[1] != "registry" {
		t.Fatalf("callbacks did not compose in order: %v", order)
	}
	<-h.Done()
}

// TestHandleLateCallbackFiresImmediately pins the no-miss contract: a
// callback registered after Send fires immediately instead of being
// dropped (a dropped registry callback leaks the tracking entry).
func TestHandleLateCallbackFiresImmediately(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	h := NewHandle(cancel)
	h.Send(Result{})

	fired := false
	h.SetOnResult(func(Result) { fired = true })
	if !fired {
		t.Fatal("late-registered callback was dropped")
	}
}

// TestHandleSecondSendIsNoop pins single delivery.
func TestHandleSecondSendIsNoop(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	h := NewHandle(cancel)

	calls := 0
	h.SetOnResult(func(Result) { calls++ })
	h.Send(Result{})
	h.Send(Result{}) // must not fire callbacks again or block on the channel
	if calls != 1 {
		t.Fatalf("callbacks fired %d times, want 1", calls)
	}
	<-h.Done()
}
