package interceptor

import (
	"context"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

func TestWithService_NamesTheInterceptorsService(t *testing.T) {
	if got := New(nil, nil).Service(); got != "" {
		t.Fatalf("default service = %q, want empty", got)
	}
	if got := New(nil, nil, WithService("checkout")).Service(); got != "checkout" {
		t.Fatalf("service = %q, want checkout", got)
	}
}

type hookCtxKey struct{}

// The injection hook gets the context the fault was started under: the
// request's, so a metrics hook can attach the request span as an exemplar.
func TestInjectionHook_GetsTheRequestContext(t *testing.T) {
	type call struct {
		ctx          context.Context
		ftype, point string
	}
	calls := make(chan call, 2)
	i := New(evaluator.NewStaticEvaluator(evaluator.StaticRule{Name: "lat", Point: evaluator.Ingress,
		Decision: *inlineLatencyDecision("lat", time.Millisecond)}), trace.Noop(),
		WithInjectionHook(func(ctx context.Context, faultType, point string) { calls <- call{ctx, faultType, point} }))

	ctx := context.WithValue(context.Background(), hookCtxKey{}, "request")
	cr, err := i.Check(ctx, evaluator.Request{Point: evaluator.Ingress})
	if err != nil || cr.Handle == nil {
		t.Fatalf("check: %+v, %v", cr, err)
	}
	<-cr.Handle.Done()
	select {
	case c := <-calls:
		if c.ctx.Value(hookCtxKey{}) != "request" || c.ftype != "*inline.Latency" || c.point != "ingress" {
			t.Fatalf("hook call = %+v, want the request context, *inline.Latency, ingress", c)
		}
	default:
		t.Fatal("the hook was not called")
	}
}
