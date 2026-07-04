package cachebox

import (
	"bytes"
	"net/http"
	"testing"
)

// --- canonical_v2 golden-vector tests (design doc Q3, wire spec §W7) ---

func TestCanonicalV2_DistinctBodiesDistinctKeys(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyCanonicalV2)
	r := mustRequest(t, "POST", "http://svc/search")
	a := fn(r, []byte(`{"q":"socks"}`))
	b := fn(r, []byte(`{"q":"shoes"}`))
	if a == b {
		t.Fatalf("different POST bodies must yield different canonical_v2 keys, both got %q", a)
	}
}

func TestCanonicalV2_QueryOrderInsensitive(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyCanonicalV2)
	a := fn(mustRequest(t, "GET", "http://svc/products?a=1&b=2"), nil)
	b := fn(mustRequest(t, "GET", "http://svc/products?b=2&a=1"), nil)
	if a != b {
		t.Fatalf("query param order should not affect the canonical_v2 key: %q vs %q", a, b)
	}
}

func TestCanonicalV2_IgnoredHeaderVariation(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyCanonicalV2)
	r1 := mustRequest(t, "GET", "http://svc/x")
	r1.Header = http.Header{
		"Date":          {"Mon, 01 Jan 2026 00:00:00 GMT"},
		"Authorization": {"Bearer aaa"},
		"Traceparent":   {"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
		"Cookie":        {"session=aaa"},
		"User-Agent":    {"client/1.0"},
		"X-Request-Id":  {"req-1"},
	}
	r2 := mustRequest(t, "GET", "http://svc/x")
	r2.Header = http.Header{
		"Date":          {"Tue, 02 Jan 2026 00:00:00 GMT"},
		"Authorization": {"Bearer zzz"},
		"Traceparent":   {"00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-yyyyyyyyyyyyyyyy-00"},
		"Cookie":        {"session=zzz"},
		"User-Agent":    {"client/2.0"},
		"X-Request-Id":  {"req-2"},
	}
	a := fn(r1, nil)
	b := fn(r2, nil)
	if a != b {
		t.Fatalf("Date/Authorization/traceparent/Cookie/User-Agent/X-Request-Id variation must not affect the canonical_v2 key: %q vs %q", a, b)
	}
}

func TestCanonicalV2_AllowlistedHeaderVariation(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyCanonicalV2)
	r1 := mustRequest(t, "GET", "http://svc/x")
	r1.Header = http.Header{"Accept": {"application/json"}}
	r2 := mustRequest(t, "GET", "http://svc/x")
	r2.Header = http.Header{"Accept": {"text/xml"}}
	a := fn(r1, nil)
	b := fn(r2, nil)
	if a == b {
		t.Fatal("Accept header variation should change the canonical_v2 key (it's in the default allowlist)")
	}
}

func TestCanonicalV2_FramingUnambiguous(t *testing.T) {
	a := canonicalV2Frame([]byte("ab"), []byte("c"))
	b := canonicalV2Frame([]byte("a"), []byte("bc"))
	if bytes.Equal(a, b) {
		t.Fatalf(`length-prefixed framing must distinguish ("ab","c") from ("a","bc"): both produced %x`, a)
	}
}

// TestCanonicalV2_AllowlistedHeaderPerRuleAddition pins that key_headers
// (per-rule additions to the default allowlist, design doc Q3) actually
// take effect: a header outside the default allowlist changes the key only
// when passed as an extra.
func TestCanonicalV2_AllowlistedHeaderPerRuleAddition(t *testing.T) {
	r1 := mustRequest(t, "GET", "http://svc/x")
	r1.Header = http.Header{"X-Tenant-Id": {"tenant-a"}}
	r2 := mustRequest(t, "GET", "http://svc/x")
	r2.Header = http.Header{"X-Tenant-Id": {"tenant-b"}}

	withoutExtra := KeyFuncFor(KeyStrategyCanonicalV2)
	if withoutExtra(r1, nil) != withoutExtra(r2, nil) {
		t.Fatal("X-Tenant-Id should not affect the key without a per-rule key_headers addition")
	}

	a := Derive(KeyStrategyCanonicalV2, []string{"x-tenant-id"}, r1, nil)
	b := Derive(KeyStrategyCanonicalV2, []string{"x-tenant-id"}, r2, nil)
	if a == b {
		t.Fatal("X-Tenant-Id variation should change the key once added via key_headers")
	}
}

// --- legacy strategy byte-stability (this refactor must not change them) ---

func TestLegacyStrategies_ByteStable(t *testing.T) {
	r := mustRequest(t, "GET", "http://svc.example/api/v1/products?b=2&a=1")

	if got, want := KeyFuncFor(KeyStrategyExact)(r, nil), "exact:GET|/api/v1/products?a=1&b=2"; got != want {
		t.Errorf("exact key changed: got %q, want %q", got, want)
	}
	if got, want := KeyFuncFor(KeyStrategyExactWithHost)(r, nil), "exact_with_host:GET|svc.example|/api/v1/products?a=1&b=2"; got != want {
		t.Errorf("exact_with_host key changed: got %q, want %q", got, want)
	}
	// FNV-1a hash of `{"q":"socks"}`, pinned as a golden literal.
	const wantBodyHash = "779af180437e8c1"
	body := []byte(`{"q":"socks"}`)
	want := "exact_with_body:GET|svc.example|/api/v1/products?a=1&b=2|" + wantBodyHash
	if got := KeyFuncFor(KeyStrategyExactWithBody)(r, body); got != want {
		t.Errorf("exact_with_body key changed: got %q, want %q", got, want)
	}
}

// --- Derive dispatch ---

func TestDerive_UnknownStrategyFallsBackToExact(t *testing.T) {
	r := mustRequest(t, "GET", "http://svc/x")
	got := Derive(KeyStrategy("made_up"), nil, r, nil)
	if got != KeyFuncFor(KeyStrategyExact)(r, nil) {
		t.Fatalf("Derive with an unrecognized strategy should fall back to exact, got %q", got)
	}
}

func TestDerive_CanonicalV2MatchesKeyFuncForWithSameHeaders(t *testing.T) {
	r := mustRequest(t, "GET", "http://svc/x")
	a := Derive(KeyStrategyCanonicalV2, nil, r, nil)
	b := KeyFuncFor(KeyStrategyCanonicalV2)(r, nil)
	if a != b {
		t.Fatalf("Derive and KeyFuncFor should agree when there are no extra key_headers: %q vs %q", a, b)
	}
}
