package cachebox

import "testing"

// TestSetChecksum_GoldenFixture pins the exact wire spec §W5 byte layout
// against an independently hand-computed fixture -- not just internal
// self-consistency, which would pass even under a shared implementation
// bug (e.g. wrong byte order or a missing separator).
func TestSetChecksum_GoldenFixture(t *testing.T) {
	entries := []*Entry{
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
	}
	const want = "b39b7e27ad99477228741443d70b1a721d946dddaf65e3526d25a2b1c9bbe035"
	if got := SetChecksum(entries); got != want {
		t.Fatalf("SetChecksum golden mismatch: got %s, want %s", got, want)
	}
}

func TestSetChecksum_OrderIndependent(t *testing.T) {
	a := []*Entry{
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
	}
	b := []*Entry{
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
	}
	if SetChecksum(a) != SetChecksum(b) {
		t.Fatal("checksum must not depend on entry order")
	}
}

func TestSetChecksum_SensitiveToEveryField(t *testing.T) {
	base := []*Entry{{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")}}
	baseSum := SetChecksum(base)

	diffKey := []*Entry{{Key: "v2:zzz", StatusCode: 200, Body: []byte("hello")}}
	if SetChecksum(diffKey) == baseSum {
		t.Fatal("checksum must change when key changes")
	}
	diffStatus := []*Entry{{Key: "v2:aaa", StatusCode: 500, Body: []byte("hello")}}
	if SetChecksum(diffStatus) == baseSum {
		t.Fatal("checksum must change when status changes")
	}
	diffBody := []*Entry{{Key: "v2:aaa", StatusCode: 200, Body: []byte("goodbye")}}
	if SetChecksum(diffBody) == baseSum {
		t.Fatal("checksum must change when body changes")
	}
}

// TestSetChecksum_CrossRepoVector pins the §W5 checksum to a fixed vector.
// manteion-go/internal/cachestore/checksum_test.go pins the SAME vector:
// the two implementations are intentionally duplicated (internal/ makes
// this one unimportable there), and the preload commit gate 409s every
// isolation phase if they ever diverge by a byte. If this test needs a new
// expected value, the wire spec changed -- update BOTH repos and the spec
// together.
func TestSetChecksum_CrossRepoVector(t *testing.T) {
	entries := []*Entry{
		{Key: "v2:alpha", StatusCode: 200, Body: []byte("hello world")},
		{Key: "v2:beta", StatusCode: 404, Body: nil},
		{Key: "v2:gamma", StatusCode: 503, Body: []byte{0x00, 0x01, 0xFF}},
	}
	const want = "823fb309f1dc167e10405d0f425b06cc48f0e847431b3a57a6e29a3e788d8032"

	if got := SetChecksum(entries); got != want {
		t.Fatalf("W5 vector drifted:\n got %s\nwant %s", got, want)
	}
	// Order independence is part of the contract (chunks stage unordered).
	shuffled := []*Entry{entries[2], entries[0], entries[1]}
	if got := SetChecksum(shuffled); got != want {
		t.Fatalf("W5 checksum is order-dependent: got %s", got)
	}
}
