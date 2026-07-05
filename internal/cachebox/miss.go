package cachebox

// Miss reasons for a fail-closed replay miss (INV-1). These names match
// manteion's fidelity snapshot miss_reasons keys (wire spec §W6).
const (
	MissReasonKeyAbsent        = "key_absent"
	MissReasonBodyBufferFailed = "body_buffer_failed"
	// MissReasonNotCommitted is reported when the installed ReplaySet
	// doesn't belong to the matched rule's (experiment_id, phase_id) at
	// all (nothing installed yet, or a different phase's set is live) --
	// distinct from MissReasonKeyAbsent, which means the right phase IS
	// installed but this particular key isn't in it. Defense in depth
	// (ATRO-6): correct preload ordering should prevent this in practice.
	MissReasonNotCommitted = "not_committed"
)
