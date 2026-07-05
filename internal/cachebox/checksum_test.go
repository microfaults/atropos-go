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
