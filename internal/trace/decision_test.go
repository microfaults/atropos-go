package trace

import (
	"context"
	"slices"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/log/logtest"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// recordingLogExporter is an in-memory sdklog.Exporter, the shape of the
// instrument package's test helper.
type recordingLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordingLogExporter) Export(_ context.Context, rs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rs {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (e *recordingLogExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }
func (e *recordingLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

func TestEmitDecision_RecordShape(t *testing.T) {
	rec := logtest.NewRecorder()
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(rec)
	t.Cleanup(func() { global.SetLoggerProvider(prev) })

	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID{9}, SpanID: oteltrace.SpanID{8}, TraceFlags: oteltrace.FlagsSampled})
	ctx := oteltrace.ContextWithSpanContext(context.Background(), sc)
	EmitDecision(ctx, Decision{RuleName: "r1", Reason: "compiled", FaultID: "abc", InjectionPoint: "ingress",
		ExperimentID: "exp-1", PhaseID: "ph-1", Action: "fault.start", RecentTraceIDs: []string{"t1", "t2"}})

	recs := rec.Result()[logtest.Scope{Name: DecisionLoggerName}]
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1 (%v)", len(recs), rec.Result())
	}
	r := recs[0]
	if r.EventName != EventDecision || r.Severity != otellog.SeverityInfo {
		t.Fatalf("event=%q severity=%v", r.EventName, r.Severity)
	}
	if oteltrace.SpanContextFromContext(r.Context).TraceID() != sc.TraceID() {
		t.Fatal("record context must carry the request span context")
	}
	want := map[attribute.Key]string{AttrRuleName: "r1", AttrReason: "compiled", AttrFaultID: "abc", AttrInjectionPoint: "ingress", AttrExperimentID: "exp-1", AttrPhaseID: "ph-1"}
	for _, a := range r.Attributes {
		if w, ok := want[a.Key]; ok && a.Value.AsString() == w {
			delete(want, a.Key)
		}
		if a.Key == AttrRecentTraceIDs && len(a.Value.AsStringSlice()) != 2 {
			t.Fatalf("recent trace ids = %v", a.Value.AsStringSlice())
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing attributes: %v", want)
	}
}

// The SDK path: trace ids land on the exported record.
func TestEmitDecision_TraceIDThroughSDK(t *testing.T) {
	exp := &recordingLogExporter{} // same shape as the instrument test helper; duplicated here (different package)
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(lp)
	t.Cleanup(func() { global.SetLoggerProvider(prev); _ = lp.Shutdown(context.Background()) })

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	ctx, span := tp.Tracer("t").Start(context.Background(), "req")
	EmitDecision(ctx, Decision{RuleName: "r", Action: "fault.start"})
	span.End()

	recs := exp.Records()
	if len(recs) != 1 || recs[0].TraceID() != span.SpanContext().TraceID() || recs[0].EventName() != EventDecision {
		t.Fatalf("records = %+v", recs)
	}
}

// installRecorder makes rec the global LoggerProvider for the test's lifetime.
func installRecorder(t *testing.T, rec *logtest.Recorder) {
	t.Helper()
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(rec)
	t.Cleanup(func() { global.SetLoggerProvider(prev) })
}

// emitOne emits d under ctx into a fresh recorder and returns the one record.
func emitOne(t *testing.T, ctx context.Context, d Decision) logtest.Record {
	t.Helper()
	rec := logtest.NewRecorder()
	installRecorder(t, rec)
	EmitDecision(ctx, d)
	recs := rec.Result()[logtest.Scope{Name: DecisionLoggerName}]
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1 (%v)", len(recs), rec.Result())
	}
	return recs[0]
}

// The rest of the record: the INFO severity text, the body naming the
// action, a timestamp, and the action itself under atropos.action (the wire
// name, spelled out).
func TestEmitDecision_SeverityTextBodyTimestampAndAction(t *testing.T) {
	r := emitOne(t, context.Background(), Decision{Action: "cachebox.replay"})
	if r.SeverityText != "INFO" || r.Body.AsString() != "atropos decision: cachebox.replay" || r.Timestamp.IsZero() {
		t.Fatalf("severity text %q, body %q, timestamp %v", r.SeverityText, r.Body.AsString(), r.Timestamp)
	}
	var action []string
	for _, a := range r.Attributes {
		if a.Key == "atropos.action" {
			action = append(action, a.Value.AsString())
		}
	}
	if !slices.Equal(action, []string{"cachebox.replay"}) {
		t.Fatalf("atropos.action = %q, want exactly [cachebox.replay]", action)
	}
}

// Every attribute but the recent trace ids is on the record even when empty
// (a cache-box action has no fault id; a decision outside an experiment has
// no experiment or phase); the recent trace ids are left out when there are
// none.
func TestEmitDecision_EmptyAttributesStayButNoRecentTraceIDs(t *testing.T) {
	for _, ids := range [][]string{nil, {}} {
		r := emitOne(t, context.Background(), Decision{Action: "fault.start", RecentTraceIDs: ids})
		got := map[attribute.Key]string{}
		for _, a := range r.Attributes {
			if a.Key == AttrRecentTraceIDs {
				t.Fatalf("recent trace ids %v = %v, want the attribute left out", ids, a.Value.AsStringSlice())
			}
			got[a.Key] = a.Value.AsString()
		}
		for _, k := range []attribute.Key{AttrRuleName, AttrReason, AttrFaultID, AttrInjectionPoint, AttrExperimentID, AttrPhaseID} {
			if v, ok := got[k]; !ok || v != "" {
				t.Fatalf("attribute %s = %q (present %v), want present and empty", k, v, ok)
			}
		}
	}
}

// The ring holds one entry per evaluation, so a request with several egress
// calls repeats its trace id: the record names each trace once, newest first
// (the first occurrence wins), and the caller's slice is left as it was.
func TestEmitDecision_RecentTraceIDsAreDistinctNewestFirst(t *testing.T) {
	ids := []string{"c", "b", "c", "a", "b", "c"}
	r := emitOne(t, context.Background(), Decision{Action: "fault.start", RecentTraceIDs: ids})
	var got []string
	for _, a := range r.Attributes {
		if a.Key == AttrRecentTraceIDs {
			got = a.Value.AsStringSlice()
		}
	}
	if !slices.Equal(got, []string{"c", "b", "a"}) {
		t.Fatalf("recent trace ids = %v, want [c b a]", got)
	}
	if !slices.Equal(ids, []string{"c", "b", "c", "a", "b", "c"}) {
		t.Fatalf("the caller's slice was modified: %v", ids)
	}
}

// Each emission goes to the LoggerProvider registered at the time. A logger
// the global API hands out before any provider is registered forwards to the
// first provider only, so one taken once at init would keep writing to the
// first recorder.
func TestEmitDecision_GoesToTheProviderRegisteredNow(t *testing.T) {
	first, second := logtest.NewRecorder(), logtest.NewRecorder()
	installRecorder(t, first)
	EmitDecision(context.Background(), Decision{Action: "fault.start"})
	installRecorder(t, second)
	EmitDecision(context.Background(), Decision{Action: "fault.join"})

	scope := logtest.Scope{Name: DecisionLoggerName}
	if a, b := first.Result()[scope], second.Result()[scope]; len(a) != 1 || len(b) != 1 || b[0].Body.AsString() != "atropos decision: fault.join" {
		t.Fatalf("first recorder %d records, second %d (%v), want one each, the join in the second", len(a), len(b), b)
	}
}

// DecisionEnabled asks the registered provider about an INFO atropos.decision
// event under ctx.
func TestDecisionEnabled_AsksTheRegisteredProvider(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		var got otellog.EnabledParameters
		var gotCtx context.Context
		installRecorder(t, logtest.NewRecorder(logtest.WithEnabledFunc(func(ctx context.Context, p otellog.EnabledParameters) bool {
			got, gotCtx = p, ctx
			return enabled
		})))
		type key struct{}
		ctx := context.WithValue(context.Background(), key{}, "req")
		if DecisionEnabled(ctx) != enabled {
			t.Fatalf("DecisionEnabled = %v, want the provider's %v", !enabled, enabled)
		}
		if got.EventName != EventDecision || got.Severity != otellog.SeverityInfo || gotCtx.Value(key{}) != "req" {
			t.Fatalf("Enabled asked with %+v under %v, want an INFO %s under the caller's context", got, gotCtx, EventDecision)
		}
	}
}

// The decision event's names are wire names (the plan's fixed names): Loki
// queries and dashboards select on them, so they are spelled out here.
func TestDecision_WireNames(t *testing.T) {
	for got, want := range map[string]string{
		DecisionLoggerName: "git.ucsc.edu/microfaults/atropos-go/decision",
		EventDecision:      "atropos.decision",
		AttrRuleName:       "atropos.rule.name",
		AttrReason:         "atropos.reason",
		AttrFaultID:        "atropos.fault.id",
		AttrInjectionPoint: "atropos.injection_point",
		AttrExperimentID:   "atropos.experiment.id",
		AttrPhaseID:        "atropos.phase.id",
		AttrRecentTraceIDs: "atropos.recent.trace_ids",
		AttrAction:         "atropos.action",
	} {
		if got != want {
			t.Errorf("name %q, want %q", got, want)
		}
	}
}
