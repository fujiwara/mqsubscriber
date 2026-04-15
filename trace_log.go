package subscriber

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type contextKey struct{ name string }

var messageIDKey = &contextKey{"messageID"}

// contextWithMessageID returns a new context with the given message ID.
func contextWithMessageID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, messageIDKey, id)
}

// messageIDFromContext extracts the message ID from the context.
func messageIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(messageIDKey).(string); ok {
		return v
	}
	return ""
}

// traceHandler wraps a slog.Handler to add trace_id, span_id, and message_id from context.
type traceHandler struct {
	slog.Handler
}

func newTraceHandler(h slog.Handler) *traceHandler {
	return &traceHandler{Handler: h}
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
	}
	if sc.HasSpanID() {
		r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
	}
	if msgID := messageIDFromContext(ctx); msgID != "" {
		r.AddAttrs(slog.String("message_id", msgID))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
}
