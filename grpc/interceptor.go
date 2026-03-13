// Package atroposgrpc provides gRPC interceptors for fault injection.
// Separate subpackage to avoid pulling grpc deps into HTTP-only services.
package atroposgrpc

import (
	"context"
	"log"

	"github.com/microfaults/atropos-go/internal/evaluator"
	"github.com/microfaults/atropos-go/internal/interceptor"
	"github.com/microfaults/atropos-go/internal/trace"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// UnaryServerInterceptor checks for faults at Ingress on each RPC.
func UnaryServerInterceptor(i *interceptor.Interceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		labels := extractGRPCLabels(ctx, info.FullMethod)

		cr, err := i.Check(ctx, evaluator.Request{
			Point:  evaluator.Ingress,
			Labels: labels,
		})
		if err != nil {
			log.Printf("atropos: grpc ingress check error: %v", err)
		}

		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			<-cr.Handle.Done()
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor checks for faults at Ingress when the stream is established.
func StreamServerInterceptor(i *interceptor.Interceptor) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		labels := extractGRPCLabels(ctx, info.FullMethod)

		cr, err := i.Check(ctx, evaluator.Request{
			Point:  evaluator.Ingress,
			Labels: labels,
		})
		if err != nil {
			log.Printf("atropos: grpc stream ingress check error: %v", err)
		}

		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			<-cr.Handle.Done()
		}

		return handler(srv, ss)
	}
}

// UnaryClientInterceptor checks for faults at Egress on each outbound RPC.
func UnaryClientInterceptor(i *interceptor.Interceptor) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		labels := map[string]string{
			trace.AttrGRPCMethod: method,
		}

		cr, err := i.Check(ctx, evaluator.Request{
			Point:  evaluator.Egress,
			Labels: labels,
		})
		if err != nil {
			log.Printf("atropos: grpc egress check error: %v", err)
		}

		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			<-cr.Handle.Done()
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor checks for faults at Egress when the stream is opened.
func StreamClientInterceptor(i *interceptor.Interceptor) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		labels := map[string]string{
			trace.AttrGRPCMethod: method,
		}

		cr, err := i.Check(ctx, evaluator.Request{
			Point:  evaluator.Egress,
			Labels: labels,
		})
		if err != nil {
			log.Printf("atropos: grpc stream egress check error: %v", err)
		}

		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			<-cr.Handle.Done()
		}

		return streamer(ctx, desc, cc, method, opts...)
	}
}

// extractGRPCLabels builds a label map from gRPC metadata.
func extractGRPCLabels(ctx context.Context, fullMethod string) map[string]string {
	labels := map[string]string{
		trace.AttrGRPCMethod: fullMethod,
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ua := md.Get("user-agent"); len(ua) > 0 {
			labels[trace.AttrGRPCUserAgent] = ua[0]
		}
	}
	return labels
}
