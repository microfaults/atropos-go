package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ctxHandler records the context of each record it handles, so a test can
// tell a *Context logging call from a context-free one.
type ctxHandler struct{ ctxs *[]context.Context }

func (h ctxHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h ctxHandler) Handle(ctx context.Context, _ slog.Record) error {
	*h.ctxs = append(*h.ctxs, ctx)
	return nil
}
func (h ctxHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h ctxHandler) WithGroup(string) slog.Handler      { return h }

type ctxKey struct{}

// The upstream warning logs with the request context, so the logging bridge
// stamps the request's trace and span ids on it.
func TestErrorHandler_UpstreamWarningCarriesRequestContext(t *testing.T) {
	var ctxs []context.Context
	h := newErrorHandler(slog.New(ctxHandler{&ctxs}))
	r := httptest.NewRequest("GET", "http://upstream.test/x", nil)
	r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, "req"))
	rec := httptest.NewRecorder()
	h(rec, r, errors.New("dial refused"))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code = %d, want 502", rec.Code)
	}
	if len(ctxs) != 1 || ctxs[0].Value(ctxKey{}) != "req" {
		t.Fatalf("logged contexts = %v, want the request context once", ctxs)
	}
}
