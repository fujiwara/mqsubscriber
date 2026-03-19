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

	result := h.Execute(context.Background(), msg)
	if result.Err != nil {
		t.Fatalf("Execute failed: %v", result.Err)
	}
	if result.ExitCode != 0 {
		t.Errorf("exit code: expected 0, got %d", result.ExitCode)
	}
	if string(result.Stdout) != "hello world" {
		t.Errorf("stdout: expected %q, got %q", "hello world", string(result.Stdout))
	}
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

	result := h.Execute(context.Background(), msg)
	if result.Err != nil {
		t.Fatalf("Execute failed: %v", result.Err)
	}
	if string(result.Stdout) != "HELLO" {
		t.Errorf("stdout: expected %q, got %q", "HELLO", string(result.Stdout))
	}
}

func TestHandlerExecuteFailure(t *testing.T) {
	m := newTestMetrics(t)

	t.Run("returns error and exit code", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:    "fail",
			Match:   map[string]string{"k": "v"},
			Command: []string{"sh", "-c", "echo 'something went wrong' >&2; exit 1"},
			Timeout: "5s",
		}, slog.Default(), m)

		msg := &mqbridge.Message{Body: []byte("test"), Headers: map[string]string{"k": "v"}}
		result := h.Execute(t.Context(), msg)
		if result.Err == nil {
			t.Error("expected error from failing command")
		}
		if result.ExitCode != 1 {
			t.Errorf("exit code: expected 1, got %d", result.ExitCode)
		}
		if string(result.Stderr) != "something went wrong\n" {
			t.Errorf("stderr: expected %q, got %q", "something went wrong\n", string(result.Stderr))
		}
	})

	t.Run("captures large stderr", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:    "fail-large-stderr",
			Match:   map[string]string{"k": "v"},
			Command: []string{"sh", "-c", "head -c 8192 /dev/zero | tr '\\0' 'X' >&2; exit 1"},
			Timeout: "5s",
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body:    []byte("test"),
			Headers: map[string]string{"k": "v"},
		}
		result := h.Execute(t.Context(), msg)
		if result.Err == nil {
			t.Error("expected error from failing command")
		}
		if len(result.Stderr) != 8192 {
			t.Errorf("stderr length: expected 8192, got %d", len(result.Stderr))
		}
	})
}

func TestHandlerExecuteTimeout(t *testing.T) {
	m := newTestMetrics(t)
	h := NewHandler(HandlerConfig{
		Name:    "slow",
		Match:   map[string]string{"k": "v"},
		Command: []string{"sleep", "10"},
		Timeout: "100ms",
	}, slog.Default(), m)

	msg := &mqbridge.Message{Body: []byte("test"), Headers: map[string]string{"k": "v"}}
	result := h.Execute(context.Background(), msg)
	if result.Err == nil {
		t.Error("expected timeout error")
	}
}

func TestBuildResponse(t *testing.T) {
	m := newTestMetrics(t)

	t.Run("without reply_to preserves headers", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "test",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"cat"},
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body: []byte("request"),
			Headers: map[string]string{
				"rabbitmq.exchange":    "commands",
				"rabbitmq.routing_key": "deploy",
			},
		}

		resp := h.buildResponse(msg, []byte("output"), "success", 0)
		if got := resp.Headers["rabbitmq.exchange"]; got != "commands" {
			t.Errorf("exchange: expected %q, got %q", "commands", got)
		}
		if got := resp.Headers["rabbitmq.routing_key"]; got != "deploy" {
			t.Errorf("routing_key: expected %q, got %q", "deploy", got)
		}
		if got := resp.Headers["x-status"]; got != "success" {
			t.Errorf("x-status: expected %q, got %q", "success", got)
		}
	})

	t.Run("with reply_to routes to reply queue", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "rpc",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"cat"},
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body: []byte("request"),
			Headers: map[string]string{
				"rabbitmq.exchange":        "commands",
				"rabbitmq.routing_key":     "deploy",
				"rabbitmq.reply_to":        "amq.gen-reply-queue",
				"rabbitmq.correlation_id":  "req-123",
				"rabbitmq.header.x-custom": "preserved",
			},
		}

		resp := h.buildResponse(msg, []byte("output"), "success", 0)
		if got := resp.Headers["rabbitmq.exchange"]; got != "" {
			t.Errorf("exchange: expected empty, got %q", got)
		}
		if got := resp.Headers["rabbitmq.routing_key"]; got != "amq.gen-reply-queue" {
			t.Errorf("routing_key: expected %q, got %q", "amq.gen-reply-queue", got)
		}
		if got := resp.Headers["rabbitmq.correlation_id"]; got != "req-123" {
			t.Errorf("correlation_id: expected %q, got %q", "req-123", got)
		}
		if got := resp.Headers["rabbitmq.header.x-custom"]; got != "preserved" {
			t.Errorf("custom header: expected %q, got %q", "preserved", got)
		}
		if _, ok := resp.Headers["rabbitmq.reply_to"]; ok {
			t.Error("reply_to should be removed from response")
		}
		if got := resp.Headers["rabbitmq.header.x-status"]; got != "success" {
			t.Errorf("x-status: expected %q, got %q", "success", got)
		}
	})

	t.Run("error response with exit code", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "fail",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"false"},
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body: []byte("test"),
			Headers: map[string]string{
				"rabbitmq.exchange":    "commands",
				"rabbitmq.routing_key": "deploy",
			},
		}

		resp := h.buildResponse(msg, []byte("error output"), "error", 1)
		if got := resp.Headers["x-status"]; got != "error" {
			t.Errorf("x-status: expected %q, got %q", "error", got)
		}
		if got := resp.Headers["x-exit-code"]; got != "1" {
			t.Errorf("x-exit-code: expected %q, got %q", "1", got)
		}
		if string(resp.Body) != "error output" {
			t.Errorf("body: expected %q, got %q", "error output", string(resp.Body))
		}
	})

	t.Run("error response with reply_to uses rabbitmq.header prefix", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:     "fail-rpc",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"false"},
			Response: true,
		}, slog.Default(), m)

		msg := &mqbridge.Message{
			Body: []byte("test"),
			Headers: map[string]string{
				"rabbitmq.exchange":       "commands",
				"rabbitmq.routing_key":    "deploy",
				"rabbitmq.reply_to":       "amq.gen-reply-queue",
				"rabbitmq.correlation_id": "req-456",
			},
		}

		resp := h.buildResponse(msg, []byte("error"), "error", 1)
		if got := resp.Headers["rabbitmq.header.x-status"]; got != "error" {
			t.Errorf("x-status: expected %q, got %q", "error", got)
		}
		if got := resp.Headers["rabbitmq.header.x-exit-code"]; got != "1" {
			t.Errorf("x-exit-code: expected %q, got %q", "1", got)
		}
		if got := resp.Headers["rabbitmq.exchange"]; got != "" {
			t.Errorf("exchange: expected empty, got %q", got)
		}
		if got := resp.Headers["rabbitmq.routing_key"]; got != "amq.gen-reply-queue" {
			t.Errorf("routing_key: expected %q, got %q", "amq.gen-reply-queue", got)
		}
	})
}

func TestShouldIgnoreResponse(t *testing.T) {
	m := newTestMetrics(t)

	t.Run("nil config", func(t *testing.T) {
		h := NewHandler(HandlerConfig{
			Name:    "test",
			Match:   map[string]string{"k": "v"},
			Command: []string{"echo"},
		}, slog.Default(), m)
		if h.shouldIgnoreResponse(&CommandResult{ExitCode: 99}) {
			t.Error("expected false when responseIgnore is nil")
		}
	})

	t.Run("matching exit code", func(t *testing.T) {
		exitCode := 99
		h := NewHandler(HandlerConfig{
			Name:           "test",
			Match:          map[string]string{"k": "v"},
			Command:        []string{"echo"},
			Response:       true,
			ResponseIgnore: &ResponseIgnoreConfig{ExitCode: &exitCode},
		}, slog.Default(), m)
		if !h.shouldIgnoreResponse(&CommandResult{ExitCode: 99}) {
			t.Error("expected true for matching exit code")
		}
	})

	t.Run("non-matching exit code", func(t *testing.T) {
		exitCode := 99
		h := NewHandler(HandlerConfig{
			Name:           "test",
			Match:          map[string]string{"k": "v"},
			Command:        []string{"echo"},
			Response:       true,
			ResponseIgnore: &ResponseIgnoreConfig{ExitCode: &exitCode},
		}, slog.Default(), m)
		if h.shouldIgnoreResponse(&CommandResult{ExitCode: 1}) {
			t.Error("expected false for non-matching exit code")
		}
	})
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
