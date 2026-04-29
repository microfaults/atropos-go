package cachebox

import (
	"net/http"
	"time"
)

// WireEntry is the JSON-serializable form of Entry for transfer between
// SDK instances and manteion. HitCount is excluded (local metric, not wire
// data). Body is []byte which encoding/json base64-encodes automatically.
type WireEntry struct {
	Key               string              `json:"key"`
	StatusCode        int                 `json:"status_code"`
	Header            map[string][]string `json:"header,omitempty"`
	Body              []byte              `json:"body,omitempty"`
	ObservedLatencyUs int64               `json:"observed_latency_us"`
	RecordedAt        time.Time           `json:"recorded_at"`
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
