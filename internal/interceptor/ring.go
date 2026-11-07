package interceptor

import (
	"maps"
	"slices"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	// ringSize is how many recent requests an interceptor remembers.
	ringSize = 256
	// ringLinks caps the recent requests one fault span links to.
	ringLinks = 16
)

// recentRequest is one request the interceptor evaluated: its span, its
// labels (the ring's own copy) and when it was recorded.
type recentRequest struct {
	Trace  oteltrace.TraceID
	Span   oteltrace.SpanID
	Labels map[string]string
	At     time.Time
}

// requestRing remembers the last requests an interceptor evaluated, so a
// fault span can link the requests that led up to it. It holds sampled
// requests only: an unsampled request's trace is never stored, and a link to
// it would point at nothing. Record runs on the request path, so it takes one
// mutex and allocates nothing beyond the label copy; Links, TraceIDs and
// RecentTraceIDs copy the ids out under the mutex and build their results
// after releasing it.
type requestRing struct {
	mu      sync.Mutex
	buf     []recentRequest // fixed size; the oldest entry is overwritten first
	next, n int             // the slot Record writes next; the entries held (at most len(buf))
}

// newRequestRing returns a ring that holds the last size requests (at least
// one).
func newRequestRing(size int) *requestRing {
	return &requestRing{buf: make([]recentRequest, max(size, 1))}
}

// linkable reports whether a span may be the target of a link: its context
// is valid and sampled, so the trace it names is stored.
func linkable(sc oteltrace.SpanContext) bool {
	return sc.IsValid() && sc.IsSampled()
}

// Record remembers the request whose span context is sc, with a copy of its
// labels. It is a no-op unless sc is valid and sampled.
func (r *requestRing) Record(sc oteltrace.SpanContext, labels map[string]string) {
	if !linkable(sc) {
		return
	}
	e := recentRequest{Trace: sc.TraceID(), Span: sc.SpanID(), Labels: maps.Clone(labels), At: time.Now()}
	r.mu.Lock()
	r.buf[r.next] = e
	r.next = (r.next + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
	r.mu.Unlock()
}

// Links returns links to up to limit of the recorded requests, newest
// first, skipping the one whose span id is exclude (the trigger, which is
// linked as such). Each carries atropos.link.kind=recent.
func (r *requestRing) Links(limit int, exclude oteltrace.SpanID) []oteltrace.Link {
	if limit <= 0 {
		return nil
	}
	picked := make([]recentRequest, 0, min(limit, len(r.buf)))
	r.mu.Lock()
	for k := 1; k <= r.n && len(picked) < limit; k++ {
		if e := r.at(k); e.Span != exclude {
			picked = append(picked, recentRequest{Trace: e.Trace, Span: e.Span})
		}
	}
	r.mu.Unlock()

	links := make([]oteltrace.Link, len(picked))
	for k, e := range picked {
		links[k] = oteltrace.Link{
			SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: e.Trace, SpanID: e.Span, TraceFlags: oteltrace.FlagsSampled}),
			Attributes:  []attribute.KeyValue{attribute.String(trace.AttrLinkKind, trace.LinkKindRecent)},
		}
	}
	return links
}

// TraceIDs returns the trace id of every recorded request, newest first, in
// hex. Requests of the same trace each have their own entry.
func (r *requestRing) TraceIDs() []string {
	traces := make([]oteltrace.TraceID, 0, len(r.buf))
	r.mu.Lock()
	for k := 1; k <= r.n; k++ {
		traces = append(traces, r.at(k).Trace)
	}
	r.mu.Unlock()

	ids := make([]string, len(traces))
	for k, t := range traces {
		ids[k] = t.String()
	}
	return ids
}

// RecentTraceIDs returns the trace ids of up to limit distinct recorded
// traces, newest first, in hex. A trace recorded more than once is named at
// its newest entry. The walk dedupes and caps on the raw ids under the mutex,
// so only the ids returned are hex-encoded; nil when limit <= 0.
func (r *requestRing) RecentTraceIDs(limit int) []string {
	if limit <= 0 {
		return nil
	}
	traces := make([]oteltrace.TraceID, 0, min(limit, len(r.buf)))
	r.mu.Lock()
	for k := 1; k <= r.n && len(traces) < limit; k++ {
		if t := r.at(k).Trace; !slices.Contains(traces, t) {
			traces = append(traces, t)
		}
	}
	r.mu.Unlock()

	ids := make([]string, len(traces))
	for k, t := range traces {
		ids[k] = t.String()
	}
	return ids
}

// at returns the k-th newest entry (k = 1 is the newest, k <= r.n). The
// caller holds r.mu.
func (r *requestRing) at(k int) *recentRequest {
	return &r.buf[(r.next-k+len(r.buf))%len(r.buf)]
}
