package interceptor

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	oteltrace "go.opentelemetry.io/otel/trace"
)

func sampledSC(i byte) oteltrace.SpanContext {
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{i, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, SpanID: oteltrace.SpanID{i, 2, 2, 2, 2, 2, 2, 2}, TraceFlags: oteltrace.FlagsSampled,
	})
}

func TestRequestRing_WrapsAndOrdersNewestFirst(t *testing.T) {
	r := newRequestRing(4)
	for i := byte(1); i <= 6; i++ {
		r.Record(sampledSC(i), map[string]string{"n": string(rune('0' + i))})
	}
	ids := r.TraceIDs()
	if len(ids) != 4 || ids[0] != sampledSC(6).TraceID().String() || ids[3] != sampledSC(3).TraceID().String() {
		t.Fatalf("ids = %v", ids)
	}
}

// Review Focus 2: never link to a trace that was never stored.
func TestRequestRing_IgnoresUnsampledAndInvalid(t *testing.T) {
	r := newRequestRing(4)
	r.Record(oteltrace.SpanContext{}, nil)
	r.Record(sampledSC(1).WithTraceFlags(0), nil)
	if got := r.TraceIDs(); len(got) != 0 {
		t.Fatalf("ids = %v, want none", got)
	}
}

func TestRequestRing_LinksCapAndExclude(t *testing.T) {
	r := newRequestRing(8)
	for i := byte(1); i <= 5; i++ {
		r.Record(sampledSC(i), nil)
	}
	links := r.Links(3, sampledSC(5).SpanID())
	if len(links) != 3 || links[0].SpanContext.SpanID() != sampledSC(4).SpanID() {
		t.Fatalf("links = %+v", links)
	}
	for _, l := range links {
		if len(l.Attributes) != 1 || l.Attributes[0].Key != trace.AttrLinkKind || l.Attributes[0].Value.AsString() != trace.LinkKindRecent {
			t.Fatalf("link attrs = %v", l.Attributes)
		}
	}
}

// Writers and readers run together, so under -race an unlocked read in Links
// or TraceIDs (or an unlocked write in Record) fails.
func TestRequestRing_ConcurrentRecord(t *testing.T) {
	r := newRequestRing(ringSize)
	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if l := r.Links(ringLinks, oteltrace.SpanID{}); len(l) > ringLinks {
					t.Errorf("Links returned %d links, want at most %d", len(l), ringLinks)
				}
				if ids := r.TraceIDs(); len(ids) > ringSize {
					t.Errorf("TraceIDs returned %d ids, want at most %d", len(ids), ringSize)
				}
			}
		}()
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g byte) {
			defer wg.Done()
			for k := 0; k < 100; k++ {
				r.Record(sampledSC(g+1), map[string]string{"k": "v"})
			}
		}(byte(g))
	}
	wg.Wait()
	close(done)
	readers.Wait()
	if len(r.TraceIDs()) != ringSize {
		t.Fatalf("len = %d", len(r.TraceIDs()))
	}
}

// A link names the recorded span exactly -- trace id, span id, sampled -- and
// the walk wraps: after the ring has overwritten its oldest slots, the links
// still run newest first and end at the oldest request it still holds.
func TestRequestRing_LinksNameTheRecordedSpansAcrossTheWrap(t *testing.T) {
	r := newRequestRing(4)
	for i := byte(1); i <= 6; i++ { // 1 and 2 are overwritten
		r.Record(sampledSC(i), nil)
	}
	links := r.Links(10, oteltrace.SpanID{})
	if len(links) != 4 {
		t.Fatalf("links = %d, want the 4 the ring holds", len(links))
	}
	for k, want := range []byte{6, 5, 4, 3} {
		sc := links[k].SpanContext
		if sc.TraceID() != sampledSC(want).TraceID() || sc.SpanID() != sampledSC(want).SpanID() || !sc.IsSampled() {
			t.Fatalf("link %d = %s/%s sampled=%v, want request %d's span, sampled", k, sc.TraceID(), sc.SpanID(), sc.IsSampled(), want)
		}
	}
}

// Nothing recorded, or no room asked for, means no links.
func TestRequestRing_EmptyRingAndZeroLimit(t *testing.T) {
	r := newRequestRing(4)
	if got := r.Links(ringLinks, oteltrace.SpanID{}); len(got) != 0 {
		t.Fatalf("empty ring links = %v", got)
	}
	if got := r.TraceIDs(); len(got) != 0 {
		t.Fatalf("empty ring ids = %v", got)
	}
	r.Record(sampledSC(1), nil)
	if got := r.Links(0, oteltrace.SpanID{}); len(got) != 0 {
		t.Fatalf("Links(0) = %v, want none", got)
	}
}

// When the trigger is the only request the ring holds, excluding it leaves
// no recent link: a fault span never links its trigger as "recent".
func TestRequestRing_ExcludedOnlyEntryLeavesNoLinks(t *testing.T) {
	r := newRequestRing(4)
	r.Record(sampledSC(1), nil)
	if got := r.Links(ringLinks, sampledSC(1).SpanID()); len(got) != 0 {
		t.Fatalf("links = %+v, want none: the only entry is the excluded trigger", got)
	}
}

// The ring keeps its own copy of the labels: the caller's map is the
// request's and may change after Record returns.
func TestRequestRing_RecordClonesLabels(t *testing.T) {
	r := newRequestRing(4)
	labels := map[string]string{"path": "/a"}
	r.Record(sampledSC(1), labels)
	labels["path"] = "/b"
	labels["extra"] = "x"
	r.mu.Lock()
	got := maps.Clone(r.buf[0].Labels)
	r.mu.Unlock()
	if len(got) != 1 || got["path"] != "/a" {
		t.Fatalf("stored labels = %v, want the copy taken at Record (path=/a)", got)
	}
}

// Every evaluation records the request it was made for -- ingress, egress, a
// custom hook -- except the Transient one: it runs for a request whose
// Ingress evaluation already recorded it, and a second entry would spend a
// ring slot and a link on the same span.
func TestEvaluate_RecordsEveryPointButTransient(t *testing.T) {
	i := New(nil, trace.Noop())
	for n, point := range []evaluator.InjectionPoint{evaluator.Ingress, evaluator.Egress, evaluator.Custom, evaluator.Transient} {
		ctx := oteltrace.ContextWithSpanContext(context.Background(), sampledSC(byte(n+1)))
		i.Evaluate(ctx, evaluator.Request{Point: point})
	}
	ids := i.recent.TraceIDs()
	want := []string{sampledSC(3).TraceID().String(), sampledSC(2).TraceID().String(), sampledSC(1).TraceID().String()}
	if len(ids) != len(want) || ids[0] != want[0] || ids[1] != want[1] || ids[2] != want[2] {
		t.Fatalf("recorded = %v, want custom, egress, ingress (newest first) and no transient", ids)
	}
}

// Through the middleware a request is evaluated twice (Ingress, then
// Transient once the response is complete) and recorded once.
func TestIngressMiddleware_RecordsEachRequestOnce(t *testing.T) {
	i := New(nil, trace.Noop())
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	for n := byte(1); n <= 3; n++ {
		ctx := oteltrace.ContextWithSpanContext(context.Background(), sampledSC(n))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil).WithContext(ctx))
	}
	if ids := i.recent.TraceIDs(); len(ids) != 3 {
		t.Fatalf("ring holds %d entries for 3 requests, want 3: %v", len(ids), ids)
	}
}

// The ring is per interceptor: one interceptor's traffic never shows up as
// another's recent requests.
func TestNew_EachInterceptorHasItsOwnRing(t *testing.T) {
	a, b := New(nil, trace.Noop()), New(nil, trace.Noop())
	a.Evaluate(oteltrace.ContextWithSpanContext(context.Background(), sampledSC(1)), evaluator.Request{Point: evaluator.Ingress})
	if got := len(a.recent.TraceIDs()); got != 1 {
		t.Fatalf("a recorded %d requests, want 1", got)
	}
	if got := b.recent.TraceIDs(); len(got) != 0 {
		t.Fatalf("b holds a's request: %v", got)
	}
}
