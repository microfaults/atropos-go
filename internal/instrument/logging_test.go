package instrument

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/slogtest"
	"time"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// recordingLogExporter is the in-memory sdklog.Exporter the log tests assert on.
type recordingLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordingLogExporter) Export(_ context.Context, rs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rs {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (e *recordingLogExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }
func (e *recordingLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

func TestNewLogger_TeesToWriterAndBridgeWithTraceID(t *testing.T) {
	exp := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	var buf bytes.Buffer
	logger := NewLogger(&buf, lp)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	ctx, span := tp.Tracer("t").Start(context.Background(), "req")
	logger.InfoContext(ctx, "hello", "k", "v")
	span.End()

	if !strings.Contains(buf.String(), `"msg":"hello"`) || !strings.Contains(buf.String(), `"k":"v"`) {
		t.Fatalf("stdout JSON = %q", buf.String())
	}
	recs := exp.Records()
	if len(recs) != 1 {
		t.Fatalf("bridge records = %d, want 1", len(recs))
	}
	if recs[0].TraceID() != span.SpanContext().TraceID() || recs[0].SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("record trace/span = %s/%s, want %s", recs[0].TraceID(), recs[0].SpanID(), span.SpanContext().TraceID())
	}
	if recs[0].Body().AsString() != "hello" {
		t.Fatalf("body = %v", recs[0].Body())
	}
}

func TestTeeHandler_EnabledIfAnyEnabled(t *testing.T) {
	info := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
	errOnly := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})
	h := newTeeHandler(errOnly, info)
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("warn must be enabled when any handler accepts it")
	}
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("debug must be disabled when no handler accepts it")
	}
}

func TestTeeHandler_WithAttrsReachesEveryHandler(t *testing.T) {
	var a, b bytes.Buffer
	h := newTeeHandler(slog.NewJSONHandler(&a, nil), slog.NewJSONHandler(&b, nil)).WithAttrs([]slog.Attr{slog.String("svc", "x")})
	slog.New(h).Info("m")
	for _, buf := range []*bytes.Buffer{&a, &b} {
		if !strings.Contains(buf.String(), `"svc":"x"`) {
			t.Fatalf("missing attr in %q", buf.String())
		}
	}
}

// The default logger's stdout half: JSON, Info and above, the standard keys.
func TestNewLogger_StdoutIsJSONAtInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, sdklog.NewLoggerProvider())
	logger.Debug("dropped")
	logger.Info("kept", "k", "v")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("stdout lines = %q, want only the Info record", lines)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("stdout line %q is not JSON: %v", lines[0], err)
	}
	if m["level"] != "INFO" || m["msg"] != "kept" || m["k"] != "v" || m["time"] == nil {
		t.Fatalf("stdout record = %v", m)
	}
}

// The bridge names its records' scope after the module.
func TestNewLogger_BridgeScope(t *testing.T) {
	exp := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	NewLogger(io.Discard, lp).Info("scoped")
	recs := exp.Records()
	if len(recs) != 1 {
		t.Fatalf("bridge records = %d, want 1", len(recs))
	}
	if got := recs[0].InstrumentationScope().Name; got != "git.ucsc.edu/microfaults/atropos-go" {
		t.Fatalf("scope = %q", got)
	}
}

// probeHandler is a slog.Handler for the tee tests. Enabled from level up, it
// adds the attribute named add to each record it handles (when add is set),
// as an enriching handler may, keeps the record's attribute keys in seen, and
// returns err.
type probeHandler struct {
	level slog.Level
	add   string
	err   error
	seen  *[]string // one entry per record handled: its keys, comma-joined
}

func (h probeHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h probeHandler) Handle(_ context.Context, r slog.Record) error {
	if h.add != "" {
		r.AddAttrs(slog.Bool(h.add, true))
	}
	var keys []string
	r.Attrs(func(a slog.Attr) bool {
		keys = append(keys, a.Key)
		return true
	})
	*h.seen = append(*h.seen, strings.Join(keys, ","))
	return h.err
}

func (h probeHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h probeHandler) WithGroup(string) slog.Handler      { return h }

// Handle reaches every handler enabled at the record's level, skips the rest,
// keeps going past a handler that fails, and joins the failures.
func TestTeeHandler_HandleSkipsDisabledAndJoinsErrors(t *testing.T) {
	errA, errB := errors.New("first handler failed"), errors.New("third handler failed")
	var a, quiet, b []string
	h := newTeeHandler(
		probeHandler{level: slog.LevelInfo, err: errA, seen: &a},
		probeHandler{level: slog.LevelError, seen: &quiet},
		probeHandler{level: slog.LevelInfo, err: errB, seen: &b},
	)
	err := h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelWarn, "m", 0))
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("err = %v, want both failures joined", err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("enabled handlers handled %d and %d records, want 1 each", len(a), len(b))
	}
	if len(quiet) != 0 {
		t.Fatalf("a handler disabled at Warn handled %d records, want 0", len(quiet))
	}
}

// Each handler gets its own copy of the record: one that adds attributes does
// not leak them into, or corrupt, the record the next handler sees.
func TestTeeHandler_EachHandlerGetsItsOwnRecord(t *testing.T) {
	var first, second []string
	h := newTeeHandler(
		probeHandler{add: "first", seen: &first},
		probeHandler{add: "second", seen: &second},
	)
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "m", 0)
	// One at a time: past the five inline attributes slog grows a slice with
	// spare capacity, which copies of a Record share unless cloned.
	for _, k := range []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"} {
		r.AddAttrs(slog.Int(k, 1))
	}
	if err := h.Handle(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	const base = "a1,a2,a3,a4,a5,a6,a7,a8"
	if len(first) != 1 || first[0] != base+",first" {
		t.Fatalf("first handler saw %q", first)
	}
	if len(second) != 1 || second[0] != base+",second" {
		t.Fatalf("second handler saw %q, want %q", second, base+",second")
	}
}

// The tee passes the standard library's handler conformance suite, and every
// handler behind it writes the same line.
func TestTeeHandler_SlogConformance(t *testing.T) {
	var a, b bytes.Buffer
	slogtest.Run(t, func(*testing.T) slog.Handler {
		a.Reset()
		b.Reset()
		return newTeeHandler(slog.NewJSONHandler(&a, nil), slog.NewJSONHandler(&b, nil))
	}, func(t *testing.T) map[string]any {
		if a.String() != b.String() {
			t.Fatalf("the handlers wrote different lines:\n%s%s", a.String(), b.String())
		}
		m := map[string]any{}
		if err := json.Unmarshal(a.Bytes(), &m); err != nil {
			t.Fatalf("line %q: %v", a.String(), err)
		}
		return m
	})
}
