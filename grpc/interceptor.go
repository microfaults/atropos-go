// Package atroposgrpc provides gRPC interceptors for fault injection.
// Separate subpackage to avoid pulling grpc deps into HTTP-only services.
package atroposgrpc

import (
	"context"
	"log"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// checkAndWait runs fault evaluation and blocks on inline faults.
func checkAndWait(i *interceptor.Interceptor, ctx context.Context, point evaluator.InjectionPoint, labels map[string]string) {
	cr, err := i.Check(ctx, evaluator.Request{Point: point, Labels: labels})
	if err != nil {
		log.Printf("atropos: grpc check error: %v", err)
	}
	if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
		<-cr.Handle.Done()
	}
}

// UnaryServerInterceptor checks for faults at Ingress on each RPC.
func UnaryServerInterceptor(i *interceptor.Interceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		checkAndWait(i, ctx, evaluator.Ingress, extractGRPCLabels(ctx, info.FullMethod))
		return handler(ctx, req)
	}
}

// StreamServerInterceptor checks for faults at Ingress when the stream is established.
func StreamServerInterceptor(i *interceptor.Interceptor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		checkAndWait(i, ss.Context(), evaluator.Ingress, extractGRPCLabels(ss.Context(), info.FullMethod))
		return handler(srv, ss)
	}
}

// UnaryClientInterceptor checks for faults at Egress on each outbound RPC.
func UnaryClientInterceptor(i *interceptor.Interceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		checkAndWait(i, ctx, evaluator.Egress, map[string]string{trace.AttrGRPCMethod: method})
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor checks for faults at Egress when the stream is opened.
func StreamClientInterceptor(i *interceptor.Interceptor) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		checkAndWait(i, ctx, evaluator.Egress, map[string]string{trace.AttrGRPCMethod: method})
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
