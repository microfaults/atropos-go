package interceptor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Response headers added by the cache-box dispatch path. These are visible
// both to the caller (which can use them for correlation in its own logs)
// and to any downstream OTel instrumentation.
const (
	headerCacheKey       = "X-Atropos-Cache-Key"
	headerCacheMode      = "X-Atropos-Cache-Mode"
	headerCacheLatencyUs = "X-Atropos-Cache-Latency-Us"
	headerCacheMiss      = "X-Atropos-Cache-Miss"
)

// defaultMissStatus is the synthetic status code returned for a fail-closed
// replay miss (INV-1, design doc Q1). The control plane will be able to
// override this per freeze context once CacheBoxContext.MissStatus is
// plumbed through the matched rule; until then every fail-closed miss uses
// this default.
const defaultMissStatus = http.StatusServiceUnavailable

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

	// The matched rule's CacheBoxContext, when present, is authoritative
	// end-to-end (design doc Q3): it overrides both the key strategy and
	// the miss status code the CacheBox was constructed with. Absence
	// (legacy rules, or the freeze/admin paths that don't carry one yet)
	// falls back to the CacheBox's construction-time behavior.
	cbCtx := decision.CacheBoxContext

	needsBody := cb.NeedsRequestBody()
	if cbCtx != nil {
		needsBody = cachebox.KeyStrategy(cbCtx.KeyStrategy).NeedsBody()
	}

	// Capture the request body if the key strategy needs it. For strategies
	// that don't need the body (the default), this is a no-op.
	var reqBody []byte
	var bodyBufferErr error
	if needsBody {
		captured, err := cachebox.BufferRequestBody(r, cb.MaxBodyBytes())
		if err != nil {
			span.AddEvent(trace.EventCacheBoxError,
				attribute.String("error", err.Error()))
			bodyBufferErr = err
		}
		reqBody = captured
	}

	var key string
	if cbCtx != nil {
		key = cachebox.Derive(cachebox.KeyStrategy(cbCtx.KeyStrategy), cbCtx.KeyHeaders, r, reqBody)
	} else {
		key = cb.DeriveKey(r, reqBody)
	}
	span.SetAttributes(attribute.String(trace.AttrCacheBoxKey, key))

	// A failed body capture under a replay-family action means we cannot
	// trust the derived key (it's missing a component the record-time key
	// had). Falling back to a live call would violate INV-1, so fail closed
	// immediately rather than entering the switch below.
	if bodyBufferErr != nil && isReplayAction(decision.CacheBox) {
		return cacheBoxMissResponse(key, decision.CacheBox, cachebox.MissReasonBodyBufferFailed, cbCtx, span), nil
	}

	switch decision.CacheBox {
	case evaluator.CacheBoxPassthrough:
		return i.cacheBoxPassthrough(ctx, r, base, cb, key, reqBody, cbCtx, span)

	case evaluator.CacheBoxReplay:
		if entry, ok := cb.Lookup(key); ok {
			return cacheBoxServe(entry, key, decision.CacheBox, 0, span), nil
		}
		return cacheBoxMissResponse(key, decision.CacheBox, replayMissReason(cb, cbCtx), cbCtx, span), nil

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
		return cacheBoxMissResponse(key, decision.CacheBox, replayMissReason(cb, cbCtx), cbCtx, span), nil
	}

	// Unknown/unrecognized action -- INV-1 requires every non-passthrough
	// path to fail closed rather than silently reaching the live downstream.
	return cacheBoxMissResponse(key, decision.CacheBox, cachebox.MissReasonKeyAbsent, cbCtx, span), nil
}

// isReplayAction reports whether action is one of the replay-family actions
// that must never reach the live downstream on a miss (INV-1).
func isReplayAction(a evaluator.CacheBoxAction) bool {
	return a == evaluator.CacheBoxReplay || a == evaluator.CacheBoxReplayDelay
}

// replayMissReason distinguishes a lookup miss within the correctly
// installed phase (key_absent) from a miss because the installed
// ReplaySet doesn't even belong to the matched rule's (experiment_id,
// phase_id) -- nothing preloaded yet, or a different phase's set is still
// live (not_committed, ATRO-6 defense in depth: correct preload ordering
// on the control-plane side should prevent this in practice).
func replayMissReason(cb *cachebox.CacheBox, cbCtx *cachebox.CacheBoxContext) string {
	if cbCtx != nil {
		expected := cachebox.PhaseKey(cbCtx.ExperimentID, cbCtx.PhaseID)
		if cb.ReplaySetPhaseKey() != expected {
			return cachebox.MissReasonNotCommitted
		}
	}
	return cachebox.MissReasonKeyAbsent
}

// cacheBoxMissResponse builds the synthetic response for a fail-closed
// replay miss (INV-1, design doc Q1): the request never reaches the live
// downstream (no base.RoundTrip) and is never written to the store (no
// cb.Record). reason is one of the cachebox.MissReason* constants; it is
// surfaced as a span event and, temporarily (pending ATRO-7's fidelity
// registry), as a package-local counter. cbCtx is the matched rule's
// CacheBoxContext, if any: its MissStatus overrides the default 503, and
// its ExperimentID/PhaseID are echoed in the response body. A nil cbCtx
// (legacy rule, or the freeze/admin paths) uses the plain default.
func cacheBoxMissResponse(key string, action evaluator.CacheBoxAction, reason string, cbCtx *cachebox.CacheBoxContext, span trace.Span) *http.Response {
	cachebox.RecordMiss(reason)
	span.AddEvent(trace.EventCacheBoxMissFailClosed,
		attribute.String(trace.AttrCacheBoxKey, key),
		attribute.String(trace.AttrCacheBoxMissReason, reason),
	)

	status := defaultMissStatus
	miss := cacheBoxMissBody{Error: "cachebox_miss", Key: key}
	if cbCtx != nil {
		if cbCtx.MissStatus != 0 {
			status = cbCtx.MissStatus
		}
		miss.ExperimentID = cbCtx.ExperimentID
		miss.PhaseID = cbCtx.PhaseID
	}
	body, _ := json.Marshal(miss)

	header := http.Header{}
	header.Set("Content-Type", "application/problem+json")
	header.Set(headerCacheMiss, "1")
	header.Set(headerCacheKey, key)
	header.Set(headerCacheMode, action.String())

	return &http.Response{
		Status:        http.StatusText(status),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// cacheBoxMissBody is the application/problem+json body for a fail-closed
// miss. ExperimentID/PhaseID are populated when the matched rule carries a
// CacheBoxContext; omitted otherwise.
type cacheBoxMissBody struct {
	Error        string `json:"error"`
	Key          string `json:"key"`
	ExperimentID string `json:"experiment_id,omitempty"`
	PhaseID      string `json:"phase_id,omitempty"`
}

// cacheBoxPassthrough forwards the request to the real downstream service,
// buffers the response body for caching, and enqueues an async record --
// but only when cbCtx carries a non-empty (experiment_id, phase_id): a
// passthrough rule with no context (legacy rule, or none at all) forwards
// the request without ever calling cb.Record (design doc Q5/INV-5, ATRO-5).
// There is no ambient/registration-time fallback for a missing pair.
//
// Body handling is split into two cases based on total size:
//   - Within cap: fully buffer, cache, and return a replayable NopCloser.
//   - Over cap: do NOT cache; rebuild resp.Body as a MultiReader over what
//     we already peeked plus the remainder of the original body, so the
//     caller still streams the full response without truncation.
func (i *Interceptor) cacheBoxPassthrough(ctx context.Context, r *http.Request, base http.RoundTripper, cb *cachebox.CacheBox, key string, reqBody []byte, cbCtx *cachebox.CacheBoxContext, span trace.Span) (*http.Response, error) {
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

	// Record only when the matched rule authorizes it with a phase pair --
	// no ctx, or an empty experiment/phase id, means "don't record" rather
	// than falling back to any ambient state (design doc Q5/INV-5).
	if cbCtx != nil && cbCtx.ExperimentID != "" && cbCtx.PhaseID != "" {
		// Enqueue the record. We clone the header on the hot path so the
		// drain goroutine has its own snapshot and the caller can freely
		// mutate resp.Header (e.g. to add the X-Atropos-Cache-* headers
		// below) without racing the recorder.
		cb.Record(cachebox.CacheRecord{
			Request:         r,
			RequestBody:     reqBody,
			StatusCode:      resp.StatusCode,
			ResponseHeader:  resp.Header.Clone(),
			ResponseBody:    body,
			ObservedLatency: observed,
			Timestamp:       time.Now(),
			ExperimentID:    cbCtx.ExperimentID,
			PhaseID:         cbCtx.PhaseID,
		})

		span.AddEvent(trace.EventCacheBoxRecord,
			attribute.Int64(trace.AttrCacheBoxLatencyUs, observed.Microseconds()),
			attribute.Int(trace.AttrCacheBoxResponseSize, len(body)),
		)
	}

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
