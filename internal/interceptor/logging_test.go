package interceptor

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// loggedRecord is one record a logRecorder was handed.
type loggedRecord struct {
	ctx   context.Context
	level slog.Level
	msg   string
	attrs map[string]any
}

// logRecorder is a slog.Handler that keeps every record with the context it was
// logged under. The Transient sites log from their own goroutine, so tests read
// the records back with wait.
type logRecorder struct {
	mu   sync.Mutex
	recs []loggedRecord
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *logRecorder) Handle(ctx context.Context, r slog.Record) error {
	rec := loggedRecord{ctx: ctx, level: r.Level, msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, rec)
	return nil
}

func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logRecorder) WithGroup(string) slog.Handler      { return h }

// wait returns the records logged so far once there are n of them, or what
// there is after d.
func (h *logRecorder) wait(n int, d time.Duration) []loggedRecord {
	deadline := time.Now().Add(d)
	for {
		h.mu.Lock()
		recs := append([]loggedRecord(nil), h.recs...)
		h.mu.Unlock()
		if len(recs) >= n || time.Now().After(deadline) {
			return recs
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// checkLogged asserts recs is exactly one Error record with message msg, logged
// under a context that carries want's trace.
func checkLogged(t *testing.T, recs []loggedRecord, msg string, want oteltrace.SpanContext) loggedRecord {
	t.Helper()
	if len(recs) != 1 || recs[0].msg != msg {
		var got []string
		for _, r := range recs {
			got = append(got, r.msg)
		}
		t.Fatalf("records = %q, want exactly %q", got, msg)
	}
	r := recs[0]
	if r.level != slog.LevelError {
		t.Fatalf("level = %v, want Error", r.level)
	}
	if got := oteltrace.SpanContextFromContext(r.ctx); got.TraceID() != want.TraceID() {
		t.Fatalf("log context trace id = %s, want the request's %s", got.TraceID(), want.TraceID())
	}
	return r
}

// The egress check error is logged with the outbound request's context, and
// the call still goes out: a failed check costs the fault, never the request.
func TestEgressTransport_CheckErrorLogsWithRequestContext(t *testing.T) {
	h := &logRecorder{}
	i := New(evaluator.NewStaticEvaluator(invalidFaultRule(evaluator.Egress)), trace.Noop(), WithLogger(slog.New(h)))
	ctx, sc := sampledContext(t)
	var reached int
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		reached++
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})

	req := httptest.NewRequest("GET", "http://downstream.test/x", nil).WithContext(ctx)
	resp, err := i.EgressTransport(base).RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusOK || reached != 1 {
		t.Fatalf("round trip: resp=%v err=%v upstream calls=%d, want the call to go out and succeed", resp, err, reached)
	}
	r := checkLogged(t, h.wait(1, time.Second), "atropos: egress check error", sc)
	if r.attrs["error"] == nil {
		t.Fatalf("attrs = %v, want the check error under \"error\"", r.attrs)
	}
}

// The Transient check error is logged from the start goroutine, under the
// request's context stripped of cancellation: the trace id survives it.
func TestIngressMiddleware_TransientCheckErrorLogsWithRequestContext(t *testing.T) {
	h := &logRecorder{}
	i := New(evaluator.NewStaticEvaluator(invalidFaultRule(evaluator.Transient)), trace.Noop(), WithLogger(slog.New(h)))
	ctx, sc := sampledContext(t)
	req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })).ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("response = %d, want the handler's 202: a failing Transient start never touches the response", rec.Code)
	}

	r := checkLogged(t, h.wait(1, 2*time.Second), "atropos: transient check error", sc)
	if r.attrs["error"] == nil {
		t.Fatalf("attrs = %v, want the check error under \"error\"", r.attrs)
	}
}
