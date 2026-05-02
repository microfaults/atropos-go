package atroposgrpc

import (
	"context"
	"testing"
	"time"

	"github.com/microfaults/atropos-go/internal/evaluator"
	"github.com/microfaults/atropos-go/internal/fault/inline"
	"github.com/microfaults/atropos-go/internal/interceptor"
	"github.com/microfaults/atropos-go/internal/trace"

	"google.golang.org/grpc"
)

type testEvaluator struct {
	decision *evaluator.Decision
}

func (e *testEvaluator) Evaluate(_ context.Context, _ evaluator.Request) *evaluator.Decision {
	return e.decision
}

func TestInterceptors_NoFault(t *testing.T) {
	i := interceptor.New(nil, trace.Noop())
	ctx := context.Background()

	t.Run("UnaryServer", func(t *testing.T) {
		fn := UnaryServerInterceptor(i)
		resp, err := fn(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Do"}, func(ctx context.Context, req any) (any, error) { return "ok", nil })
		if err != nil {
			t.Fatal(err)
		}
		if resp != "ok" {
			t.Fatalf("expected 'ok', got %v", resp)
		}
	})

	t.Run("StreamServer", func(t *testing.T) {
		fn := StreamServerInterceptor(i)
		err := fn(nil, &fakeServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(srv any, ss grpc.ServerStream) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("UnaryClient", func(t *testing.T) {
		fn := UnaryClientInterceptor(i)
		err := fn(ctx, "/test.Service/Call", "req", "reply", nil, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("StreamClient", func(t *testing.T) {
		fn := StreamClientInterceptor(i)
		cs, err := fn(ctx, &grpc.StreamDesc{StreamName: "Stream"}, nil, "/test.Service/Stream", func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) { return &fakeClientStream{}, nil })
		if err != nil {
			t.Fatal(err)
		}
		if cs == nil {
			t.Fatal("expected non-nil client stream")
		}
	})
}

func TestUnaryServerInterceptor_WithFault(t *testing.T) {
	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 30 * time.Millisecond},
			Reason: "test: grpc fault",
			Mode:   evaluator.Inline,
		},
	}
	i := interceptor.New(eval, trace.Noop())
	interceptorFn := UnaryServerInterceptor(i)

	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/DoStuff"}

	start := time.Now()
	resp, err := interceptorFn(context.Background(), "test-request", info, handler)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %v", resp)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected >= 20ms from fault, got %s", elapsed)
	}
	t.Logf("grpc unary server interceptor with fault: %s", elapsed)
}

func TestExtractGRPCLabels(t *testing.T) {
	labels := extractGRPCLabels(context.Background(), "/test.Service/DoStuff")
	if labels[trace.AttrGRPCMethod] != "/test.Service/DoStuff" {
		t.Fatalf("expected method label, got %v", labels)
	}
}


func TestStreamServerInterceptor_WithFault(t *testing.T) {
	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 30 * time.Millisecond},
			Reason: "test: stream fault",
			Mode:   evaluator.Inline,
		},
	}
	i := interceptor.New(eval, trace.Noop())
	interceptorFn := StreamServerInterceptor(i)

	handler := func(srv any, ss grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamStuff"}
	start := time.Now()
	err := interceptorFn(nil, &fakeServerStream{ctx: context.Background()}, info, handler)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected >= 20ms from fault, got %s", elapsed)
	}
}


func TestUnaryClientInterceptor_WithFault(t *testing.T) {
	eval := &testEvaluator{
		decision: &evaluator.Decision{
			Fault:  &inline.Latency{Delay: 30 * time.Millisecond},
			Reason: "test: client fault",
			Mode:   evaluator.Inline,
		},
	}
	i := interceptor.New(eval, trace.Noop())
	interceptorFn := UnaryClientInterceptor(i)

	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	start := time.Now()
	err := interceptorFn(context.Background(), "/test.Service/Call", "req", "reply", nil, invoker)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected >= 20ms from fault, got %s", elapsed)
	}
}


type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

type fakeClientStream struct {
	grpc.ClientStream
}
