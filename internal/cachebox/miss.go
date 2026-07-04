package cachebox

import "sync/atomic"

// Miss reasons for a fail-closed replay miss (INV-1). These names match
// manteion's fidelity snapshot miss_reasons keys (wire spec §W6).
const (
	MissReasonKeyAbsent        = "key_absent"
	MissReasonBodyBufferFailed = "body_buffer_failed"
)

// missCounters is a temporary package-level tally of fail-closed replay
// misses by reason. ATRO-7 replaces this with a per-(experiment_id,
// phase_id) fidelity registry.
var missCounters struct {
	keyAbsent        atomic.Int64
	bodyBufferFailed atomic.Int64
}

// RecordMiss increments the counter for reason. Unrecognized reasons are
// ignored -- callers pass one of the MissReason* constants.
func RecordMiss(reason string) {
	switch reason {
	case MissReasonKeyAbsent:
		missCounters.keyAbsent.Add(1)
	case MissReasonBodyBufferFailed:
		missCounters.bodyBufferFailed.Add(1)
	}
}

// MissCounts is a snapshot of the temporary package-level miss counters.
type MissCounts struct {
	KeyAbsent        int64
	BodyBufferFailed int64
}

// MissStats returns a snapshot of the current miss counts.
func MissStats() MissCounts {
	return MissCounts{
		KeyAbsent:        missCounters.keyAbsent.Load(),
		BodyBufferFailed: missCounters.bodyBufferFailed.Load(),
	}
}
