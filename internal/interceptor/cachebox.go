package interceptor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/microfaults/atropos-go/internal/cachebox"
	"github.com/microfaults/atropos-go/internal/evaluator"
	"github.com/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Response headers added by the cache-box dispatch path. These are visible
// both to the caller (which can use them for correlation in its own logs)
// and to any downstream OTel instrumentation.
const (
	headerCacheKey       = "X-Atropos-Cache-Key"
	headerCacheMode      = "X-Atropos-Cache-Mode"
	headerCacheLatencyUs = "X-Atropos-Cache-Latency-Us"
)

// handleCacheBox dispatches a cache-box decision for an egress request.
// Must only be called when i.cacheBox != nil and decision.CacheBox != CacheBoxNone.
// The function always ends its own span; the caller should not End() it.
func (i *Interceptor) handleCacheBox(r *http.Request, base http.RoundTripper, req evaluator.Request, decision *evaluator.Decision) (*http.Response, error) {
	cb := i.cacheBox
	ctx := r.Context()

	ctx, span := i.tracer.Start(ctx, trace.SpanCacheBoxCheck,
		attribute.String(trace.AttrCacheBoxMode, decision.CacheBox.String()),
		attribute.String(trace.AttrCacheBoxInjection, req.Point.String()),
		attribute.String(trace.AttrCacheBoxReason, decision.Reason),
	)
	defer span.End()

	// Thread evaluator labels onto the cache-box span for trace correlation.
	for k, v := range req.Labels {
		span.SetAttributes(attribute.String(k, v))
	}
	// Make sure the context we propagate to base.RoundTrip carries the
	// cache-box span as parent.
	r = r.WithContext(ctx)

	// Capture the request body if the key strategy needs it. For strategies
	// that don't need the body (the default), this is a no-op.
	var reqBody []byte
	if cb.NeedsRequestBody() {
		captured, err := cachebox.BufferRequestBody(r, cb.MaxBodyBytes())
		if err != nil {
			span.AddEvent(trace.EventCacheBoxError,
				attribute.String("error", err.Error()))
			// Fall through to passthrough -- failing to buffer shouldn't
			// break the request.
		}
		reqBody = captured
	}

	key := cb.DeriveKey(r, reqBody)
	span.SetAttributes(attribute.String(trace.AttrCacheBoxKey, key))

	switch decision.CacheBox {
	case evaluator.CacheBoxPassthrough:
		return i.cacheBoxPassthrough(ctx, r, base, cb, key, reqBody, span)

	case evaluator.CacheBoxReplay:
		if entry, ok := cb.Lookup(key); ok {
			return cacheBoxServe(entry, key, decision.CacheBox, 0, span), nil
		}
		span.AddEvent(trace.EventCacheBoxMiss,
			attribute.String(trace.AttrCacheBoxKey, key))
		// Miss fallback: forward and record so the next request hits.
		return i.cacheBoxPassthrough(ctx, r, base, cb, key, reqBody, span)

	case evaluator.CacheBoxReplayDelay:
		if entry, ok := cb.Lookup(key); ok {
			delay := cb.SampleDelay(entry)
			if delay > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
			}
			return cacheBoxServe(entry, key, decision.CacheBox, delay, span), nil
		}
		span.AddEvent(trace.EventCacheBoxMiss,
			attribute.String(trace.AttrCacheBoxKey, key))
		return i.cacheBoxPassthrough(ctx, r, base, cb, key, reqBody, span)
	}

	// Unknown action -- fall through to the real downstream call.
	return base.RoundTrip(r)
}

// cacheBoxPassthrough forwards the request to the real downstream service,
// buffers the response body for caching, and enqueues an async record.
//
// Body handling is split into two cases based on total size:
//   - Within cap: fully buffer, cache, and return a replayable NopCloser.
//   - Over cap: do NOT cache; rebuild resp.Body as a MultiReader over what
//     we already peeked plus the remainder of the original body, so the
//     caller still streams the full response without truncation.
func (i *Interceptor) cacheBoxPassthrough(ctx context.Context, r *http.Request, base http.RoundTripper, cb *cachebox.CacheBox, key string, reqBody []byte, span trace.Span) (*http.Response, error) {
	_ = ctx
	start := time.Now()
	resp, err := base.RoundTrip(r)
	if err != nil || resp == nil {
		return resp, err
	}
	observed := time.Since(start)

	maxBytes := cb.MaxBodyBytes()

	// Peek up to maxBytes+1 bytes so we can distinguish "fits" from "oversize"
	// without reading the whole body for oversized responses.
	buf := bytes.NewBuffer(make([]byte, 0, maxBytes+1))
	peeked, copyErr := io.CopyN(buf, resp.Body, int64(maxBytes)+1)
	if copyErr != nil && copyErr != io.EOF {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("cachebox: read response body: %w", copyErr)
	}

	if peeked > int64(maxBytes) {
		// Oversized: stream the rest of the body through alongside what we
		// already peeked. The original Body is the Closer; MultiReader is
		// the Reader. Do NOT close the original body here.
		span.AddEvent(trace.EventCacheBoxOversize,
			attribute.Int64(trace.AttrCacheBoxResponseSize, peeked))
		orig := resp.Body
		resp.Body = struct {
			io.Reader
			io.Closer
		}{
			Reader: io.MultiReader(bytes.NewReader(buf.Bytes()), orig),
			Closer: orig,
		}
		return resp, nil
	}

	// Body fits in cap -- safe to close the original and replace with a
	// replayable reader over the captured bytes.
	_ = resp.Body.Close()
	body := buf.Bytes()
	resp.Body = io.NopCloser(bytes.NewReader(body))

	// Enqueue the record. We clone the header on the hot path so the drain
	// goroutine has its own snapshot and the caller can freely mutate
	// resp.Header (e.g. to add the X-Atropos-Cache-* headers below) without
	// racing the recorder.
	cb.Record(cachebox.CacheRecord{
		Request:         r,
		RequestBody:     reqBody,
		StatusCode:      resp.StatusCode,
		ResponseHeader:  resp.Header.Clone(),
		ResponseBody:    body,
		ObservedLatency: observed,
		Timestamp:       time.Now(),
	})

	span.AddEvent(trace.EventCacheBoxRecord,
		attribute.Int64(trace.AttrCacheBoxLatencyUs, observed.Microseconds()),
		attribute.Int(trace.AttrCacheBoxResponseSize, len(body)),
	)

	// Tag the response so callers can see which key was assigned.
	resp.Header.Set(headerCacheKey, key)
	resp.Header.Set(headerCacheLatencyUs, strconv.FormatInt(observed.Microseconds(), 10))

	// Optional: attach the response body to the span (research/debug mode).
	if limit := cb.OTelCaptureLimit(); limit > 0 && len(body) <= limit {
		span.SetAttributes(attribute.String(trace.AttrCacheBoxResponseBody, string(body)))
	}

	return resp, nil
}

// cacheBoxServe builds an HTTP response from a cached entry and tags it with
// cache-box headers. delay is the synthetic sleep duration used by
// replay_with_delay (0 for plain replay).
func cacheBoxServe(entry *cachebox.Entry, key string, action evaluator.CacheBoxAction, delay time.Duration, span trace.Span) *http.Response {
	header := http.Header{}
	if entry.Header != nil {
		header = entry.Header.Clone()
	}
	header.Set(headerCacheKey, key)
	header.Set(headerCacheMode, action.String())
	if delay > 0 {
		header.Set(headerCacheLatencyUs, strconv.FormatInt(delay.Microseconds(), 10))
	} else {
		header.Set(headerCacheLatencyUs, strconv.FormatInt(entry.ObservedLatency.Microseconds(), 10))
	}

	span.AddEvent(trace.EventCacheBoxReplay,
		attribute.Bool(trace.AttrCacheBoxHit, true),
		attribute.Int(trace.AttrCacheBoxResponseSize, len(entry.Body)),
		attribute.Int64(trace.AttrCacheBoxLatencyUs, entry.ObservedLatency.Microseconds()),
	)

	return &http.Response{
		Status:        http.StatusText(entry.StatusCode),
		StatusCode:    entry.StatusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(entry.Body)),
		ContentLength: int64(len(entry.Body)),
	}
}
