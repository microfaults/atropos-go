package trace

// Attribute keys, span names, and event names in the atropos.* namespace.
const (
	// Fault span attributes.
	AttrFaultType           = "atropos.fault.type"
	AttrFaultInjectionPoint = "atropos.fault.injection_point"
	AttrFaultReason         = "atropos.fault.reason"
	AttrFaultDurationMs     = "atropos.fault.duration_ms"
	AttrFaultActualDuration = "atropos.fault.actual_duration"
	AttrFaultDetail         = "atropos.fault.detail"
	// AttrFaultID is the fault run's id: on the fault span, and on the span
	// of every request that started or joined the run.
	AttrFaultID = "atropos.fault.id"
	// AttrFaultJoined counts the requests that joined a running background
	// fault instead of starting another (on the run's fault span). Every
	// call not marked as an arm (interceptor.ArmedContext) counts, with or
	// without a span context; an eager re-arm (Core.Arm marks its calls that
	// way) that meets the running fault does not.
	AttrFaultJoined = "atropos.fault.joined"

	// Span link attributes. AttrLinkKind says what a link points at; its
	// values are the LinkKind* names.
	AttrLinkKind = "atropos.link.kind"

	// Decision events: the log record EmitDecision emits for every applied
	// decision (a log event, not a span event), and its attributes besides
	// AttrFaultID (the fault run's id; empty for a cache-box action).
	EventDecision      = "atropos.decision"
	AttrRuleName       = "atropos.rule.name"
	AttrReason         = "atropos.reason"
	AttrInjectionPoint = "atropos.injection_point"
	AttrExperimentID   = "atropos.experiment.id"
	AttrPhaseID        = "atropos.phase.id"
	AttrRecentTraceIDs = "atropos.recent.trace_ids"
	AttrAction         = "atropos.action" // what was decided: an Action* value

	// Resource attributes (stamped by internal/instrument's InitTelemetry).
	AttrHost = "atropos.host" // the instrument host: "in-process" or "sidecar"

	// Hook span attributes.
	AttrHookName = "atropos.hook.name"

	// HTTP label attributes (set by middleware).
	AttrHTTPMethod     = "atropos.http.method"
	AttrHTTPPath       = "atropos.http.path"
	AttrHTTPQuery      = "atropos.http.query"
	AttrHTTPHost       = "atropos.http.host"
	AttrHTTPUserAgent  = "atropos.http.user_agent"
	AttrHTTPStatusCode = "atropos.http.status_code" // Transient point only: the completed response's status

	// gRPC label attributes (set by gRPC interceptors).
	AttrGRPCMethod    = "atropos.grpc.method"
	AttrGRPCUserAgent = "atropos.grpc.user_agent"

	// Network fault event attributes.
	AttrNetConnID         = "atropos.net.conn.id"
	AttrNetConnRemoteAddr = "atropos.net.conn.remote_addr"
	AttrNetConnAffected   = "atropos.net.conn.affected"
	AttrNetToxicPhase     = "atropos.net.toxic.phase"
	AttrNetToxicType      = "atropos.net.toxic.type"
	AttrNetUpstreamAddr   = "atropos.net.upstream.addr"
	AttrNetDialDurationMs = "atropos.net.upstream.dial_duration_ms"
	AttrNetConnDurationMs = "atropos.net.conn.duration_ms"
	AttrNetBytesUp        = "atropos.net.bytes_up"
	AttrNetBytesDown      = "atropos.net.bytes_down"

	// Resource fault event attributes.
	AttrResourceTargetLoad = "atropos.resource.target_load"
	AttrResourceTargetRate = "atropos.resource.target_rate"
	AttrResourceRampUpMs   = "atropos.resource.ramp_up_ms"
	AttrResourceRampDownMs = "atropos.resource.ramp_down_ms"

	// Cache-box span attributes.
	AttrCacheBoxMode         = "atropos.cachebox.mode"
	AttrCacheBoxKey          = "atropos.cachebox.key"
	AttrCacheBoxHit          = "atropos.cachebox.hit"
	AttrCacheBoxLatencyUs    = "atropos.cachebox.latency_us"
	AttrCacheBoxResponseSize = "atropos.cachebox.response_size"
	AttrCacheBoxResponseBody = "atropos.cachebox.response_body"
	AttrCacheBoxWorkflow     = "atropos.cachebox.workflow"
	AttrCacheBoxReason       = "atropos.cachebox.reason"
	AttrCacheBoxInjection    = "atropos.cachebox.injection_point"
	AttrCacheBoxMissReason   = "atropos.cachebox.miss_reason"

	// Span names.
	SpanFaultInject   = "atropos.fault.inject"
	SpanCacheBoxCheck = "atropos.cachebox.check"
	SpanHookPrefix    = "atropos.hook."

	// Link kinds (values of AttrLinkKind).
	LinkKindTrigger = "trigger" // fault span -> the request that started the run
	LinkKindRecent  = "recent"  // fault span -> a request the interceptor saw before the run started
	LinkKindFault   = "fault"   // request span -> the fault span of the run it joined

	// Decision actions (values of AttrAction).
	ActionFaultStart     = "fault.start" // a fault run started
	ActionFaultJoin      = "fault.join"  // a request joined a running background fault
	ActionCacheBoxPrefix = "cachebox."   // + the cache-box action: passthrough, replay, replay_with_delay

	// Event names.
	EventFaultInjected = "atropos.fault.injected"
	EventFaultSkipped  = "atropos.fault.skipped"
	EventFaultCheckErr = "atropos.fault.check.error"

	// Network event names.
	EventNetConnAccepted = "atropos.net.conn.accepted"
	EventNetToxicHijack  = "atropos.net.toxic.hijack"
	EventNetUpstreamDial = "atropos.net.upstream.dial"
	EventNetConnError    = "atropos.net.conn.error"
	EventNetConnClosed   = "atropos.net.conn.closed"

	// Resource event names.
	EventResourceRampUpStart      = "atropos.resource.ramp_up.start"
	EventResourceRampUpComplete   = "atropos.resource.ramp_up.complete"
	EventResourceSustainStart     = "atropos.resource.sustain.start"
	EventResourceRampDownStart    = "atropos.resource.ramp_down.start"
	EventResourceRampDownComplete = "atropos.resource.ramp_down.complete"

	// Cache-box event names.
	EventCacheBoxRecord         = "atropos.cachebox.record"
	EventCacheBoxReplay         = "atropos.cachebox.replay"
	EventCacheBoxMiss           = "atropos.cachebox.miss"
	EventCacheBoxOversize       = "atropos.cachebox.oversize"
	EventCacheBoxError          = "atropos.cachebox.error"
	EventCacheBoxMissFailClosed = "atropos.cachebox.miss.fail_closed"
)
