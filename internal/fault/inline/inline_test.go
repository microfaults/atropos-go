package inline

import (
	"context"
	"testing"
	"time"

	fault "github.com/microfaults/atropos-go/internal/fault"
)

func TestLatency_Basic(t *testing.T) {
	l := &Latency{Delay: 100 * time.Millisecond}

	start := time.Now()
	handle, err := l.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	result := <-handle.Done()
	elapsed := time.Since(start)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected >= 80ms, got %s", elapsed)
	}
	t.Logf("latency: %s", elapsed)
}

func TestLatency_Cancel(t *testing.T) {
	l := &Latency{Delay: 5 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	handle, err := l.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	result := <-handle.Done()
	if result.Err == nil {
		t.Fatal("expected error from context cancellation")
	}
	if result.ActualDuration > 200*time.Millisecond {
		t.Fatalf("expected quick cancel, got %s", result.ActualDuration)
	}
}

func TestLatency_Invalid(t *testing.T) {
	l := &Latency{Delay: 0, Jitter: 0}
	_, err := l.Start(context.Background())
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestError_Basic(t *testing.T) {
	e := &Error{StatusCode: 503, Message: "service unavailable"}

	handle, err := e.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	result := <-handle.Done()
	// Result.Err should be nil — the error is the intended effect, not a failure.
	if result.Err != nil {
		t.Fatalf("unexpected result.Err: %v", result.Err)
	}

	detail := IsErrorResult(result)
	if detail == nil {
		t.Fatal("expected ErrorDetail in result")
	}
	if detail.StatusCode != 503 {
		t.Fatalf("expected 503, got %d", detail.StatusCode)
	}
	t.Logf("error fault: %s", detail.Err())
}

func TestError_Invalid(t *testing.T) {
	e := &Error{StatusCode: 0}
	_, err := e.Start(context.Background())
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestHang_Basic(t *testing.T) {
	h := &Hang{FaultConfig: fault.FaultConfig{Duration: 200 * time.Millisecond}}

	start := time.Now()
	handle, err := h.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	result := <-handle.Done()
	elapsed := time.Since(start)

	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected ~200ms hang, got %s", elapsed)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("expected ~200ms hang, got %s", elapsed)
	}
	t.Logf("hang: %s (err=%v)", result.ActualDuration, result.Err)
}

func TestHang_Stop(t *testing.T) {
	h := &Hang{FaultConfig: fault.FaultConfig{Duration: 5 * time.Second}}

	handle, err := h.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	handle.Stop()

	result := <-handle.Done()
	if result.ActualDuration > 500*time.Millisecond {
		t.Fatalf("expected early stop, got %s", result.ActualDuration)
	}
	t.Logf("hang stopped: %s", result.ActualDuration)
}

func TestOnResult_Callback(t *testing.T) {
	// Verify the OnResult callback fires before Done() receives the result.
	l := &Latency{Delay: 50 * time.Millisecond}

	handle, err := l.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var callbackResult fault.Result
	callbackCalled := false
	handle.SetOnResult(func(r fault.Result) {
		callbackCalled = true
		callbackResult = r
	})

	result := <-handle.Done()

	if !callbackCalled {
		t.Fatal("OnResult callback was not called")
	}
	if callbackResult.ActualDuration != result.ActualDuration {
		t.Fatalf("callback got different duration: %s vs %s", callbackResult.ActualDuration, result.ActualDuration)
	}
	t.Logf("OnResult callback fired correctly: %s", result.ActualDuration)
}

// Compile-time checks.
var (
	_ fault.Fault = (*Latency)(nil)
	_ fault.Fault = (*Error)(nil)
	_ fault.Fault = (*Hang)(nil)
)
