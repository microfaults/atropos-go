package instrument

import (
	"bytes"
	"context"
	"log"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
)

// logEntry is one record a logCapture was handed.
type logEntry struct {
	ctx   context.Context
	level slog.Level
	msg   string
	attrs map[string]any
}

// logCapture keeps every record the loggers it hands out receive, for the tests
// that check where the SDK's log lines go. (The root package's tests carry a
// twin of it: a test helper cannot be imported across packages.)
type logCapture struct {
	mu      sync.Mutex
	entries []logEntry
}

func (c *logCapture) logger() *slog.Logger { return slog.New(&captureHandler{c: c}) }

// messages returns the message of every record captured so far, in order.
func (c *logCapture) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	msgs := make([]string, len(c.entries))
	for i, e := range c.entries {
		msgs[i] = e.msg
	}
	return msgs
}

// find returns the first record whose message starts with prefix.
func (c *logCapture) find(prefix string) (logEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if strings.HasPrefix(e.msg, prefix) {
			return e, true
		}
	}
	return logEntry{}, false
}

// captureHandler is the slog.Handler behind logCapture.logger. Handlers derived
// with WithAttrs share the capture, and groups are ignored.
type captureHandler struct {
	c     *logCapture
	attrs []slog.Attr
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	e := logEntry{ctx: ctx, level: r.Level, msg: r.Message, attrs: map[string]any{}}
	for _, a := range h.attrs {
		e.attrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		e.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.c.mu.Lock()
	h.c.entries = append(h.c.entries, e)
	h.c.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &captureHandler{c: h.c, attrs: append(slices.Clone(h.attrs), attrs...)}
}

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

// lockedBuffer is a goroutine-safe sink: goroutines other tests left running may
// log through the standard logger while one test has it redirected.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureDefaultSlog redirects, for the rest of the test, what slog.Default()
// writes into the returned buffer: the default handler prints through the
// standard log package. The tests that use it must not run in parallel.
func captureDefaultSlog(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return buf
}
