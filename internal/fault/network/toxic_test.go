package network

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestLatency_AddsDelay(t *testing.T) {
	toxic := &Latency{Delay: 100 * time.Millisecond}
	src := strings.NewReader("hello")
	var dst bytes.Buffer

	start := time.Now()
	err := toxic.Pipe(context.Background(), src, &dst)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.String() != "hello" {
		t.Fatalf("got %q, want %q", dst.String(), "hello")
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected >= 80ms delay, got %s", elapsed)
	}
	t.Logf("latency toxic: %s", elapsed)
}

func TestLatency_Jitter(t *testing.T) {
	toxic := &Latency{Delay: 50 * time.Millisecond, Jitter: 100 * time.Millisecond}
	src := strings.NewReader("data")
	var dst bytes.Buffer

	start := time.Now()
	err := toxic.Pipe(context.Background(), src, &dst)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be at least 50ms (base delay), could be up to ~150ms.
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected >= 40ms, got %s", elapsed)
	}
	t.Logf("latency+jitter: %s", elapsed)
}

func TestLatency_ContextCancel(t *testing.T) {
	toxic := &Latency{Delay: 5 * time.Second}
	src := strings.NewReader("data")
	var dst bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := toxic.Pipe(ctx, src, &dst)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestThrottle_LimitsRate(t *testing.T) {
	// 1000 bytes at 5000 bytes/sec should take ~200ms.
	toxic := &Throttle{BytesPerSec: 5000}
	src := strings.NewReader(strings.Repeat("x", 1000))
	var dst bytes.Buffer

	start := time.Now()
	err := toxic.Pipe(context.Background(), src, &dst)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.Len() != 1000 {
		t.Fatalf("got %d bytes, want 1000", dst.Len())
	}
	// Should take at least ~150ms (some variance from read chunking).
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected >= 100ms, got %s (rate not limited)", elapsed)
	}
	t.Logf("throttle: %d bytes in %s", dst.Len(), elapsed)
}

func TestDrip_SlowsOutput(t *testing.T) {
	// 10 bytes, 2 bytes per chunk, 50ms interval → ~200ms.
	toxic := &Drip{ChunkSize: 2, Interval: 50 * time.Millisecond}
	src := strings.NewReader("0123456789")
	var dst bytes.Buffer

	start := time.Now()
	err := toxic.Pipe(context.Background(), src, &dst)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.String() != "0123456789" {
		t.Fatalf("got %q, want %q", dst.String(), "0123456789")
	}
	// 5 chunks, 4 intervals of 50ms = ~200ms.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected >= 150ms, got %s", elapsed)
	}
	t.Logf("drip: %s", elapsed)
}

func TestBlackhole_PipeDropsAll(t *testing.T) {
	toxic := &Blackhole{}
	src := strings.NewReader("data")
	var dst bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := toxic.Pipe(ctx, src, &dst)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if dst.Len() != 0 {
		t.Fatalf("expected no output, got %d bytes", dst.Len())
	}
}

func TestRST_AfterBytes(t *testing.T) {
	toxic := &RST{AfterBytes: 5}
	src := strings.NewReader("0123456789")
	var dst bytes.Buffer

	err := toxic.Pipe(context.Background(), src, &dst)

	rstErr, ok := err.(*RSTError)
	if !ok {
		t.Fatalf("expected *RSTError, got %T: %v", err, err)
	}
	if rstErr.Reason != "bytes" {
		t.Fatalf("expected reason 'bytes', got %q", rstErr.Reason)
	}
	if rstErr.BytesForwarded < 5 {
		t.Fatalf("expected >= 5 bytes forwarded, got %d", rstErr.BytesForwarded)
	}
	t.Logf("RST after %d bytes: %v", rstErr.BytesForwarded, rstErr)
}

func TestRST_AfterDuration(t *testing.T) {
	toxic := &RST{AfterDuration: 100 * time.Millisecond}
	// Slow reader that never finishes.
	src := &slowReader{}
	var dst bytes.Buffer

	start := time.Now()
	err := toxic.Pipe(context.Background(), src, &dst)
	elapsed := time.Since(start)

	rstErr, ok := err.(*RSTError)
	if !ok {
		t.Fatalf("expected *RSTError, got %T: %v", err, err)
	}
	if rstErr.Reason != "duration" {
		t.Fatalf("expected reason 'duration', got %q", rstErr.Reason)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected >= 80ms, got %s", elapsed)
	}
	t.Logf("RST after %s: %v", elapsed, rstErr)
}

func TestRetransmitDelay_DelaysLostChunks(t *testing.T) {
	// 100% rate → every chunk gets retransmit delay.
	toxic := &RetransmitDelay{Rate: 1.0, Delay: 50 * time.Millisecond}
	src := strings.NewReader("hello")
	var dst bytes.Buffer

	start := time.Now()
	err := toxic.Pipe(context.Background(), src, &dst)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.String() != "hello" {
		t.Fatalf("got %q, want %q", dst.String(), "hello")
	}
	// Should be delayed.
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected >= 40ms delay, got %s", elapsed)
	}
	t.Logf("retransmit_delay (100%%): %s", elapsed)
}

func TestRetransmitDelay_ResetThreshold(t *testing.T) {
	toxic := &RetransmitDelay{Rate: 1.0, Delay: 10 * time.Millisecond, ResetThreshold: 2}
	// Need enough data for 2 reads.
	src := &multiReader{chunks: [][]byte{[]byte("aa"), []byte("bb")}}
	var dst bytes.Buffer

	err := toxic.Pipe(context.Background(), src, &dst)

	rstErr, ok := err.(*RSTError)
	if !ok {
		t.Fatalf("expected *RSTError, got %T: %v", err, err)
	}
	if rstErr.Reason != "consecutive_retransmit_threshold" {
		t.Fatalf("expected reason 'consecutive_retransmit_threshold', got %q", rstErr.Reason)
	}
}

// Compile-time interface checks.
var (
	_ Toxic     = (*Latency)(nil)
	_ Toxic     = (*Blackhole)(nil)
	_ ConnToxic = (*Blackhole)(nil)
	_ Toxic     = (*RST)(nil)
	_ ConnToxic = (*RST)(nil)
	_ Toxic     = (*Throttle)(nil)
	_ Toxic     = (*RetransmitDelay)(nil)
	_ Toxic     = (*Drip)(nil)
)

// slowReader blocks on each read until context expires.
type slowReader struct{}

func (r *slowReader) Read(p []byte) (int, error) {
	time.Sleep(50 * time.Millisecond)
	p[0] = 'x'
	return 1, nil
}

// multiReader returns one chunk per Read call, then EOF.
type multiReader struct {
	chunks [][]byte
	idx    int
}

func (r *multiReader) Read(p []byte) (int, error) {
	if r.idx >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.idx])
	r.idx++
	return n, nil
}
