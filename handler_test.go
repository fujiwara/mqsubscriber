package subscriber

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fujiwara/mqbridge"
)

func newTestMetrics(t *testing.T) *Metrics {
	t.Helper()
	m, err := newMetrics()
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}
	return m
}

func TestHandlerMatch(t *testing.T) {
	m := newTestMetrics(t)
	h := NewHandler(HandlerConfig{
		Name: "test",
		Match: map[string]string{
			"rabbitmq.routing_key":  "deploy",
			"rabbitmq.header.x-env": "production",
		},
		Command: []string{"echo"},
	}, slog.Default(), m)

	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name: "full match",
			headers: map[string]string{
				"rabbitmq.routing_key":  "deploy",
				"rabbitmq.header.x-env": "production",
				"extra":                 "ignored",
			},
			want: true,
		},
		{
			name: "partial match",
			headers: map[string]string{
				"rabbitmq.routing_key": "deploy",
			},
			want: false,
		},
		{
			name: "wrong value",
			headers: map[string]string{
				"rabbitmq.routing_key":  "deploy",
				"rabbitmq.header.x-env": "staging",
			},
			want: false,
		},
		{
			name:    "nil headers",
			headers: nil,
			want:    false,
		},
		{
			name:    "empty headers",
			headers: map[string]string{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &mqbridge.Message{Body: []byte("test"), Headers: tt.headers}
			if got := h.Match(msg); got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandlerExecute(t *testing.T) {
	m := newTestMetrics(t)
	h := NewHandler(HandlerConfig{
		Name:    "echo",
		Match:   map[string]string{"k": "v"},
		Command: []string{"cat"},
		Timeout: "5s",
	}, slog.Default(), m)

	msg := &mqbridge.Message{
		Body: []byte("hello world"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "test.key",
			"rabbitmq.exchange":    "test-exchange",
		},
	}

	result, err := h.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(result.Body) != "hello world" {
		t.Errorf("body: expected %q, got %q", "hello world", string(result.Body))
	}
	// Headers should be preserved (no reply_to, so no rewriting)
	if result.Headers["rabbitmq.routing_key"] != "test.key" {
		t.Errorf("routing_key header: expected %q, got %q", "test.key", result.Headers["rabbitmq.routing_key"])
	}
	if result.Headers["rabbitmq.exchange"] != "test-exchange" {
		t.Errorf("exchange header: expected %q, got %q", "test-exchange", result.Headers["rabbitmq.exchange"])
	}
	if result.Headers["x-status"] != "success" {
		t.Errorf("x-status: expected %q, got %q", "success", result.Headers["x-status"])
	}
}

func TestHandlerExecuteRPCResponse(t *testing.T) {
	m := newTestMetrics(t)
	h := NewHandler(HandlerConfig{
		Name:    "rpc",
		Match:   map[string]string{"k": "v"},
		Command: []string{"cat"},
		Timeout: "5s",
	}, slog.Default(), m)

	t.Run("with reply_to routes to reply queue", func(t *testing.T) {
		msg := &mqbridge.Message{
			Body: []byte("request"),
			Headers: map[string]string{
				"k":                        "v",
				"rabbitmq.exchange":        "commands",
				"rabbitmq.routing_key":     "deploy",
				"rabbitmq.reply_to":        "amq.gen-reply-queue",
				"rabbitmq.correlation_id":  "req-123",
				"rabbitmq.header.x-custom": "preserved",
			},
		}

		result, err := h.Execute(t.Context(), msg)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		if got := result.Headers["rabbitmq.exchange"]; got != "" {
			t.Errorf("exchange: expected empty, got %q", got)
		}
		if got := result.Headers["rabbitmq.routing_key"]; got != "amq.gen-reply-queue" {
			t.Errorf("routing_key: expected %q, got %q", "amq.gen-reply-queue", got)
		}
		if got := result.Headers["rabbitmq.correlation_id"]; got != "req-123" {
			t.Errorf("correlation_id: expected %q, got %q", "req-123", got)
		}
		if got := result.Headers["rabbitmq.header.x-custom"]; got != "preserved" {
			t.Errorf("custom header: expected %q, got %q", "preserved", got)
		}
		if _, ok := result.Headers["rabbitmq.reply_to"]; ok {
			t.Error("reply_to should be removed from response")
		}
		// RabbitMQ context: status header has rabbitmq.header. prefix
		if got := result.Headers["rabbitmq.header.x-status"]; got != "success" {
			t.Errorf("x-status: expected %q, got %q", "success", got)
		}
	})

	t.Run("without reply_to preserves headers", func(t *testing.T) {
		msg := &mqbridge.Message{
			Body: []byte("request"),
			Headers: map[string]string{
				"k":                    "v",
				"rabbitmq.exchange":    "commands",
				"rabbitmq.routing_key": "deploy",
			},
		}

		result, err := h.Execute(t.Context(), msg)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		if got := result.Headers["rabbitmq.exchange"]; got != "commands" {
			t.Errorf("exchange: expected %q, got %q", "commands", got)
		}
		if got := result.Headers["rabbitmq.routing_key"]; got != "deploy" {
			t.Errorf("routing_key: expected %q, got %q", "deploy", got)
		}
		// SimpleMQ context: status header has no prefix
		if got := result.Headers["x-status"]; got != "success" {
			t.Errorf("x-status: expected %q, got %q", "success", got)
		}
	})
}

func TestHandlerExecuteTransform(t *testing.T) {
	m := newTestMetrics(t)
	h := NewHandler(HandlerConfig{
		Name:    "upper",
		Match:   map[string]string{"k": "v"},
		Command: []string{"tr", "a-z", "A-Z"},
		Timeout: "5s",
	}, slog.Default(), m)

	msg := &mqbridge.Message{
		Body:    []byte("hello"),
		Headers: map[string]string{"k": "v"},
	}

	result, err := h.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(result.Body) != "HELLO" {
		t.Errorf("body: expected %q, got %q", "HELLO", string(result.Body))
	}
}

func TestHandlerExecuteFailure(t *testing.T) {
	m := newTestMetrics(t)

	t.Run("response false returns error", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "fail",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"false"},
			Timeout:  "5s",
			Response: false,
		}, slog.Default(), m)

		msg := &mqbridge.Message{Body: []byte("test"), Headers: map[string]string{"k": "v"}}
		_, err := h.Execute(t.Context(), msg)
		if err == nil {
			t.Error("expected error from failing command")
		}
	})

	t.Run("response true returns error response with x-status", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "fail-rpc",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"sh", "-c", "echo 'something went wrong' >&2; exit 1"},
			Timeout:  "5s",
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body: []byte("test"),
			Headers: map[string]string{
				"k":                    "v",
				"rabbitmq.exchange":    "commands",
				"rabbitmq.routing_key": "deploy",
			},
		}
		result, err := h.Execute(t.Context(), msg)
		if err != nil {
			t.Fatalf("expected no error for response handler, got %v", err)
		}
		if result.Headers["x-status"] != "error" {
			t.Errorf("x-status: expected %q, got %q", "error", result.Headers["x-status"])
		}
		if result.Headers["x-exit-code"] != "1" {
			t.Errorf("x-exit-code: expected %q, got %q", "1", result.Headers["x-exit-code"])
		}
		if string(result.Body) != "something went wrong\n" {
			t.Errorf("body: expected stderr content, got %q", string(result.Body))
		}
	})

	t.Run("response true truncates large stderr to last 4KB", func(t *testing.T) {
		// Generate 8KB of stderr output, keep only last 4KB
		h := NewHandler(HandlerConfig{
			Name:     "fail-large-stderr",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"sh", "-c", "dd if=/dev/zero bs=1 count=8192 | tr '\\0' 'X' >&2; exit 1"},
			Timeout:  "5s",
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body:    []byte("test"),
			Headers: map[string]string{"k": "v"},
		}
		result, err := h.Execute(t.Context(), msg)
		if err != nil {
			t.Fatalf("expected no error for response handler, got %v", err)
		}
		if len(result.Body) != maxErrorBodySize {
			t.Errorf("body length: expected %d, got %d", maxErrorBodySize, len(result.Body))
		}
		if result.Headers["x-status"] != "error" {
			t.Errorf("x-status: expected %q, got %q", "error", result.Headers["x-status"])
		}
	})

	t.Run("response true with reply_to uses rabbitmq.header prefix", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "fail-rpc-rabbitmq",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"false"},
			Timeout:  "5s",
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body: []byte("test"),
			Headers: map[string]string{
				"k":                       "v",
				"rabbitmq.exchange":       "commands",
				"rabbitmq.routing_key":    "deploy",
				"rabbitmq.reply_to":       "amq.gen-reply-queue",
				"rabbitmq.correlation_id": "req-456",
			},
		}
		result, err := h.Execute(t.Context(), msg)
		if err != nil {
			t.Fatalf("expected no error for response handler, got %v", err)
		}
		if got := result.Headers["rabbitmq.header.x-status"]; got != "error" {
			t.Errorf("x-status: expected %q, got %q", "error", got)
		}
		if got := result.Headers["rabbitmq.header.x-exit-code"]; got != "1" {
			t.Errorf("x-exit-code: expected %q, got %q", "1", got)
		}
		// RPC routing should still apply
		if got := result.Headers["rabbitmq.exchange"]; got != "" {
			t.Errorf("exchange: expected empty, got %q", got)
		}
		if got := result.Headers["rabbitmq.routing_key"]; got != "amq.gen-reply-queue" {
			t.Errorf("routing_key: expected %q, got %q", "amq.gen-reply-queue", got)
		}
		if got := result.Headers["rabbitmq.correlation_id"]; got != "req-456" {
			t.Errorf("correlation_id: expected %q, got %q", "req-456", got)
		}
	})
}

func TestHandlerExecuteTimeout(t *testing.T) {
	m := newTestMetrics(t)
	h := NewHandler(HandlerConfig{
		Name:     "slow",
		Match:    map[string]string{"k": "v"},
		Command:  []string{"sleep", "10"},
		Timeout:  "100ms",
		Response: false,
	}, slog.Default(), m)

	msg := &mqbridge.Message{Body: []byte("test"), Headers: map[string]string{"k": "v"}}
	_, err := h.Execute(context.Background(), msg)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestTailBytes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than limit", "abc", 10, "abc"},
		{"exact limit", "abcd", 4, "abcd"},
		{"exceeds limit", "abcdefgh", 4, "efgh"},
		{"empty", "", 4, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tailBytes([]byte(tt.in), tt.n))
			if got != tt.want {
				t.Errorf("tailBytes(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
			}
		})
	}
}

func TestHeadersToEnv(t *testing.T) {
	env := headersToEnv(map[string]string{
		"rabbitmq.routing_key":     "test.key",
		"rabbitmq.header.x-custom": "value",
	})
	expected := map[string]string{
		"SIMPLEMQ_HEADER_RABBITMQ_ROUTING_KEY":     "test.key",
		"SIMPLEMQ_HEADER_RABBITMQ_HEADER_X_CUSTOM": "value",
	}
	envMap := make(map[string]string)
	for _, e := range env {
		for i, c := range e {
			if c == '=' {
				envMap[e[:i]] = e[i+1:]
				break
			}
		}
	}
	for k, v := range expected {
		if envMap[k] != v {
			t.Errorf("env %s: expected %q, got %q", k, v, envMap[k])
		}
	}
}
