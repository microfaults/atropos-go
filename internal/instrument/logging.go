package instrument

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	otellog "go.opentelemetry.io/otel/log"
)

// loggerScopeName is the instrumentation scope of the records the SDK's
// default logger sends through the OpenTelemetry slog bridge.
const loggerScopeName = "git.ucsc.edu/microfaults/atropos-go"

// NewLogger returns the SDK's default logger: JSON lines on w at Info and
// above, teed into the OpenTelemetry slog bridge on lp, whose records carry the
// trace and span of the context passed to the *Context methods. A nil lp means
// the global logger provider.
func NewLogger(w io.Writer, lp otellog.LoggerProvider) *slog.Logger {
	return slog.New(newTeeHandler(
		slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}),
		otelslog.NewHandler(loggerScopeName, otelslog.WithLoggerProvider(lp)),
	))
}

// teeHandler sends each record to every handler behind it that is enabled at
// the record's level. It stands in for slog.NewMultiHandler, which arrives in
// Go 1.26, above this module's Go 1.25 floor.
type teeHandler struct{ handlers []slog.Handler }

func newTeeHandler(handlers ...slog.Handler) slog.Handler {
	return teeHandler{handlers: slices.Clone(handlers)}
}

// Enabled reports whether any handler accepts records at level l.
func (h teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

// Handle gives every handler enabled at r's level its own clone of r, so a
// handler that adds to its record cannot disturb the next one, and joins the
// handlers' errors.
func (h teeHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, handler := range h.handlers {
		if !handler.Enabled(ctx, r.Level) {
			continue
		}
		if err := handler.Handle(ctx, r.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// WithAttrs returns a tee of every handler with attrs added.
func (h teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithAttrs(attrs)
	}
	return teeHandler{handlers: handlers}
}

// WithGroup returns a tee of every handler with the group opened; an empty name
// returns the receiver, as slog.Handler requires.
func (h teeHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithGroup(name)
	}
	return teeHandler{handlers: handlers}
}
