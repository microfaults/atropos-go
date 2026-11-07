package trace

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
)

// DecisionLoggerName is the instrumentation scope of the decision events.
const DecisionLoggerName = "git.ucsc.edu/microfaults/atropos-go/decision"

// Decision is one applied decision, as its atropos.decision event reports it.
// FaultID is the fault run's id (empty for a cache-box action); ExperimentID
// and PhaseID name the experiment the decision belongs to, when known.
type Decision struct {
	RuleName, Reason, FaultID, InjectionPoint, ExperimentID, PhaseID string

	Action         string   // "fault.start" | "fault.join" | "cachebox.<CacheBoxAction.String()>"
	RecentTraceIDs []string // newest first; omitted from the record when empty
}

// DecisionLogger is the decision events' logger on the LoggerProvider
// registered now. It is looked up per decision rather than once at init: a
// logger the global API hands out before any provider is registered forwards
// to the first provider registered and never to a later one, and a process
// can register more than one (each Serve's InitTelemetry, tests). A caller
// that checks DecisionEnabledOn before EmitDecisionTo resolves it once and
// passes it to both.
func DecisionLogger() otellog.Logger {
	return global.GetLoggerProvider().Logger(DecisionLoggerName)
}

// DecisionEnabled reports whether the registered LoggerProvider would process
// a decision event emitted under ctx, so a caller can skip assembling one
// (the recent trace ids are the costly part) when nothing would take it.
func DecisionEnabled(ctx context.Context) bool {
	return DecisionEnabledOn(ctx, DecisionLogger())
}

// DecisionEnabledOn is DecisionEnabled asked of logger (a DecisionLogger
// result) instead of a fresh lookup.
func DecisionEnabledOn(ctx context.Context, logger otellog.Logger) bool {
	return logger.Enabled(ctx, otellog.EnabledParameters{Severity: otellog.SeverityInfo, EventName: EventDecision})
}

// EmitDecision emits d as one atropos.decision event through the OTel Logs
// API (the global LoggerProvider, scope DecisionLoggerName). ctx is the
// context of the request the decision was applied to: the SDK takes the
// record's trace_id and span_id from its span context, which is how a log
// backend joins the event to the request's trace. Every attribute is on the
// record, empty or not, except atropos.recent.trace_ids: it names each trace
// once, keeping the first (newest) occurrence, and is left out when d has
// none.
func EmitDecision(ctx context.Context, d Decision) {
	EmitDecisionTo(ctx, DecisionLogger(), d)
}

// EmitDecisionTo is EmitDecision through logger (a DecisionLogger result)
// instead of a fresh lookup.
func EmitDecisionTo(ctx context.Context, logger otellog.Logger, d Decision) {
	var rec otellog.Record
	rec.SetTimestamp(time.Now())
	rec.SetEventName(EventDecision)
	rec.SetSeverity(otellog.SeverityInfo)
	rec.SetSeverityText("INFO")
	rec.SetBody(attribute.StringValue("atropos decision: " + d.Action))
	rec.AddAttributes(
		attribute.String(AttrRuleName, d.RuleName),
		attribute.String(AttrReason, d.Reason),
		attribute.String(AttrFaultID, d.FaultID),
		attribute.String(AttrInjectionPoint, d.InjectionPoint),
		attribute.String(AttrExperimentID, d.ExperimentID),
		attribute.String(AttrPhaseID, d.PhaseID),
		attribute.String(AttrAction, d.Action),
	)
	if ids := distinct(d.RecentTraceIDs); len(ids) > 0 {
		rec.AddAttributes(attribute.StringSlice(AttrRecentTraceIDs, ids))
	}
	logger.Emit(ctx, rec)
}

// distinct returns ids in order without repeats, keeping each id's first
// occurrence; ids itself is left as it is. The interceptor's ring holds one
// entry per evaluation, so a request that made several egress calls is in it
// several times.
func distinct(ids []string) []string {
	if len(ids) < 2 {
		return ids
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
