package cachebox

import (
	"net/http"
	"time"
)

// RequestMeta is debug provenance attached to a pushed wire entry: which
// request produced it. It is informational only -- never used for key
// matching or replay lookup.
type RequestMeta struct {
	Method string `json:"method"`
	Host   string `json:"host"`
	Path   string `json:"path"`
}

// WireEntry is the JSON-serializable form of Entry for transfer between
// SDK instances and manteion. HitCount is excluded (local metric, not wire
// data). Body is []byte which encoding/json base64-encodes automatically.
//
// KeyStrategy, StrategyVersion, ResponseBodySHA256, and RequestMeta are
// fidelity-refinement additions (wire spec §W2): the strategy fields record
// which keyer produced Key so manteion can verify record/replay strategy
// agreement at preload (INV-2), and ResponseBodySHA256 feeds the W5 preload
// checksum. All four are additive/optional -- a legacy payload without them
// decodes with zero values.
type WireEntry struct {
	Key               string              `json:"key"`
	StatusCode        int                 `json:"status_code"`
	Header            map[string][]string `json:"header,omitempty"`
	Body              []byte              `json:"body,omitempty"`
	ObservedLatencyUs int64               `json:"observed_latency_us"`
	RecordedAt        time.Time           `json:"recorded_at"`

	KeyStrategy        string       `json:"key_strategy,omitempty"`
	StrategyVersion    int          `json:"strategy_version,omitempty"`
	ResponseBodySHA256 string       `json:"response_body_sha256,omitempty"`
	RequestMeta        *RequestMeta `json:"request_meta,omitempty"`
}

// EntryToWire converts a store Entry to a WireEntry for serialization.
func EntryToWire(e *Entry) WireEntry {
	var header map[string][]string
	if e.Header != nil {
		header = map[string][]string(e.Header.Clone())
	}
	return WireEntry{
		Key:               e.Key,
		StatusCode:        e.StatusCode,
		Header:            header,
		Body:              e.Body,
		ObservedLatencyUs: e.ObservedLatency.Microseconds(),
		RecordedAt:        e.RecordedAt,
	}
}

// WireToEntry converts a WireEntry back to a store Entry. HitCount starts
// at zero. The caller owns the returned Entry.
func WireToEntry(w *WireEntry) *Entry {
	var header http.Header
	if w.Header != nil {
		header = http.Header(w.Header).Clone()
	}
	return &Entry{
		Key:             w.Key,
		StatusCode:      w.StatusCode,
		Header:          header,
		Body:            w.Body,
		ObservedLatency: time.Duration(w.ObservedLatencyUs) * time.Microsecond,
		RecordedAt:      w.RecordedAt,
	}
}
