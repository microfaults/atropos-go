package cachebox

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestWireEntry_Roundtrip(t *testing.T) {
	original := &Entry{
		Key:             "exact:GET|/products",
		StatusCode:      200,
		Header:          http.Header{"Content-Type": {"application/json"}},
		Body:            []byte(`{"items":[]}`),
		ObservedLatency: 12345 * time.Microsecond,
		RecordedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	original.HitCount.Store(42)

	wire := EntryToWire(original)

	b, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded WireEntry
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	restored := WireToEntry(&decoded)

	if restored.Key != original.Key {
		t.Errorf("Key: %q != %q", restored.Key, original.Key)
	}
	if restored.StatusCode != original.StatusCode {
		t.Errorf("StatusCode: %d != %d", restored.StatusCode, original.StatusCode)
	}
	if restored.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Header: %v", restored.Header)
	}
	if string(restored.Body) != string(original.Body) {
		t.Errorf("Body: %q != %q", restored.Body, original.Body)
	}
	if restored.ObservedLatency != original.ObservedLatency {
		t.Errorf("ObservedLatency: %v != %v", restored.ObservedLatency, original.ObservedLatency)
	}
	if !restored.RecordedAt.Equal(original.RecordedAt) {
		t.Errorf("RecordedAt: %v != %v", restored.RecordedAt, original.RecordedAt)
	}
	if restored.HitCount.Load() != 0 {
		t.Errorf("HitCount should be 0 after roundtrip, got %d", restored.HitCount.Load())
	}
}

func TestWireEntry_JSON_BodyBase64(t *testing.T) {
	wire := WireEntry{
		Key:        "k",
		StatusCode: 200,
		Body:       []byte("hello"),
	}
	b, _ := json.Marshal(wire)
	var decoded WireEntry
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(decoded.Body) != "hello" {
		t.Errorf("Body roundtrip failed: %q", decoded.Body)
	}
}
