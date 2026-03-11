// faults_test.go
package atropos

import (
	"context"
	"testing"
	"time"
)

func TestNewLatencyFault(t *testing.T) {
	f := NewLatencyFault(100*time.Millisecond, 0)
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}

	handle, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := <-handle.Done()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if r.ActualDuration < 80*time.Millisecond {
		t.Fatalf("expected ~100ms, got %s", r.ActualDuration)
	}
}

func TestNewLatencyFault_WithJitter(t *testing.T) {
	f := NewLatencyFault(50*time.Millisecond, 50*time.Millisecond)
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestNewHangFault(t *testing.T) {
	f := NewHangFault(100 * time.Millisecond)
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}

	handle, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := <-handle.Done()
	if r.ActualDuration < 80*time.Millisecond {
		t.Fatalf("expected ~100ms, got %s", r.ActualDuration)
	}
}

func TestNewErrorFault(t *testing.T) {
	f := NewErrorFault(503, "service unavailable")
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}

	handle, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := <-handle.Done()
	if r.Detail == nil {
		t.Fatal("expected error detail")
	}
}

func TestNewErrorFault_InvalidStatus(t *testing.T) {
	f := NewErrorFault(0, "bad")
	if err := f.Validate(); err == nil {
		t.Fatal("expected validation error for status 0")
	}
}
