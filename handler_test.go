package subscriber

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fujiwara/mqbridge"
	"github.com/fujiwara/trabbits/pattern"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func newTestMetrics(t *testing.T) *Metrics {
	t.Helper()
	m, err := newMetrics()
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}
	return m
}

func newTestHandler(t *testing.T, cfg HandlerConfig, m *Metrics) *Handler {
	t.Helper()
	h, err := NewHandler(cfg, slog.Default(), m)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	return h
}

func TestHandlerMatch(t *testing.T) {
	m := newTestMetrics(t)
	h := newTestHandler(t, HandlerConfig{
		Name: "test",
		Match: map[string]string{
			"rabbitmq.routing_key":  "deploy",
			"rabbitmq.header.x-env": "production",
		},
		Command: []string{"echo"},
	}, m)

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

func TestHandlerMatchPattern(t *testing.T) {
	m := newTestMetrics(t)
	h := newTestHandler(t, HandlerConfig{
		Name: "test-pattern",
		Match: map[string]string{
			"rabbitmq.routing_key": "order.*",
			"x-env":                "production",
		},
		MatchPattern: true,
		Command:      []string{"echo"},
	}, m)

	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name: "star matches one word",
			headers: map[string]string{
				"rabbitmq.routing_key": "order.created",
				"x-env":                "production",
			},
			want: true,
		},
		{
			name: "star does not match two words",
			headers: map[string]string{
				"rabbitmq.routing_key": "order.created.v2",
				"x-env":                "production",
			},
			want: false,
		},
		{
			name: "exact value still works in pattern mode",
			headers: map[string]string{
				"rabbitmq.routing_key": "order.created",
				"x-env":                "staging",
			},
			want: false,
		},
		{
			name: "star matches empty trailing word",
			headers: map[string]string{
				"rabbitmq.routing_key": "order.",
				"x-env":                "production",
			},
			want: true,
		},
		{
			name:    "nil headers",
			headers: nil,
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

func TestHandlerMatchPatternHash(t *testing.T) {
	m := newTestMetrics(t)

	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{"hash matches zero words", "order.#", "order", false},
		{"hash matches one word", "order.#", "order.created", true},
		{"hash matches multiple words", "order.#", "order.created.v2", true},
		{"hash alone matches anything", "#", "anything.at.all", true},
		{"hash alone matches single word", "#", "single", true},
		{"hash at start", "#.error", "a.b.error", true},
		{"hash at start single word prefix", "#.error", "error", false},
		{"hash in middle", "order.#.done", "order.created.done", true},
		{"hash in middle multiple", "order.#.done", "order.a.b.done", true},
		{"hash in middle zero", "order.#.done", "order.done", true},
		{"no match", "order.#", "user.created", false},
		{"star and hash combined", "order.*.#", "order.created", true},
		{"star and hash combined multi", "order.*.#", "order.created.v2.final", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t, HandlerConfig{
				Name: "test",
				Match: map[string]string{
					"key": tt.pattern,
				},
				MatchPattern: true,
				Command:      []string{"echo"},
			}, m)
			msg := &mqbridge.Message{
				Body:    []byte("test"),
				Headers: map[string]string{"key": tt.value},
			}
			if got := h.Match(msg); got != tt.want {
				t.Errorf("Match(%q, %q) = %v, want %v", tt.pattern, tt.value, got, tt.want)
			}
		})
	}
}

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		pattern string
		match   []string
		noMatch []string
	}{
		{
			pattern: "order.created",
			match:   []string{"order.created"},
			noMatch: []string{"order.updated", "order.created.v2", "order"},
		},
		{
			pattern: "order.*",
			match:   []string{"order.created", "order.updated"},
			noMatch: []string{"order", "order.created.v2", "user.created"},
		},
		{
			pattern: "*.created",
			match:   []string{"order.created", "user.created"},
			noMatch: []string{"created", "order.user.created"},
		},
		{
			pattern: "#",
			match:   []string{"anything", "a.b.c", ""},
			noMatch: []string{},
		},
		{
			pattern: "order.#",
			match:   []string{"order.created", "order.a.b.c"},
			noMatch: []string{"order", "user.created", "orders"},
		},
		{
			pattern: "#.error",
			match:   []string{"a.error", "a.b.error"},
			noMatch: []string{"error", "error.log", "a.error.b"},
		},
		{
			pattern: "a.#.z",
			match:   []string{"a.z", "a.b.z", "a.b.c.z"},
			noMatch: []string{"a", "z", "a.b.c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			for _, s := range tt.match {
				if !pattern.Match(s, tt.pattern) {
					t.Errorf("pattern %q should match %q", tt.pattern, s)
				}
			}
			for _, s := range tt.noMatch {
				if pattern.Match(s, tt.pattern) {
					t.Errorf("pattern %q should NOT match %q", tt.pattern, s)
				}
			}
		})
	}
}

func TestHandlerExecute(t *testing.T) {
	m := newTestMetrics(t)
	h := newTestHandler(t, HandlerConfig{
		Name:    "echo",
		Match:   map[string]string{"k": "v"},
		Command: []string{"cat"},
		Timeout: "5s",
	}, m)

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
	h := newTestHandler(t, HandlerConfig{
		Name:    "upper",
		Match:   map[string]string{"k": "v"},
		Command: []string{"tr", "a-z", "A-Z"},
		Timeout: "5s",
	}, m)

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
		h := newTestHandler(t, HandlerConfig{
			Name:    "fail",
			Match:   map[string]string{"k": "v"},
			Command: []string{"sh", "-c", "echo 'something went wrong' >&2; exit 1"},
			Timeout: "5s",
		}, m)

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
		h := newTestHandler(t, HandlerConfig{
			Name:    "fail-large-stderr",
			Match:   map[string]string{"k": "v"},
			Command: []string{"sh", "-c", "head -c 8192 /dev/zero | tr '\\0' 'X' >&2; exit 1"},
			Timeout: "5s",
		}, m)

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
	h := newTestHandler(t, HandlerConfig{
		Name:    "slow",
		Match:   map[string]string{"k": "v"},
		Command: []string{"sleep", "10"},
		Timeout: "100ms",
	}, m)

	msg := &mqbridge.Message{Body: []byte("test"), Headers: map[string]string{"k": "v"}}
	result := h.Execute(context.Background(), msg)
	if result.Err == nil {
		t.Error("expected timeout error")
	}
}

func TestBuildResponse(t *testing.T) {
	m := newTestMetrics(t)

	t.Run("without reply_to preserves headers", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:     "test",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"cat"},
			Response: true,
		}, m)

		msg := &mqbridge.Message{
			Body: []byte("request"),
			Headers: map[string]string{
				"rabbitmq.exchange":    "commands",
				"rabbitmq.routing_key": "deploy",
			},
		}

		resp := h.buildResponse(msg, []byte("output"), "success", 0)
		// Non-RPC: routing headers should be removed
		if _, ok := resp.Headers["rabbitmq.exchange"]; ok {
			t.Error("exchange should be removed from non-RPC response")
		}
		if _, ok := resp.Headers["rabbitmq.routing_key"]; ok {
			t.Error("routing_key should be removed from non-RPC response")
		}
		// RabbitMQ origin: x-status uses rabbitmq.header prefix
		if got := resp.Headers["rabbitmq.header.x-status"]; got != "success" {
			t.Errorf("x-status: expected %q, got %q", "success", got)
		}
	})

	t.Run("with reply_to routes to reply queue", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:     "rpc",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"cat"},
			Response: true,
		}, m)

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
		h := newTestHandler(t, HandlerConfig{
			Name:     "fail",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"false"},
			Response: true,
		}, m)

		msg := &mqbridge.Message{
			Body: []byte("test"),
			Headers: map[string]string{
				"rabbitmq.exchange":    "commands",
				"rabbitmq.routing_key": "deploy",
			},
		}

		resp := h.buildResponse(msg, []byte("error output"), "error", 1)
		// RabbitMQ origin: x-status uses rabbitmq.header prefix
		if got := resp.Headers["rabbitmq.header.x-status"]; got != "error" {
			t.Errorf("x-status: expected %q, got %q", "error", got)
		}
		if got := resp.Headers["rabbitmq.header.x-exit-code"]; got != "1" {
			t.Errorf("x-exit-code: expected %q, got %q", "1", got)
		}
		if string(resp.Body) != "error output" {
			t.Errorf("body: expected %q, got %q", "error output", string(resp.Body))
		}
	})

	t.Run("error response with reply_to uses rabbitmq.header prefix", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:     "fail-rpc",
			Match:    map[string]string{"k": "v"},
			Command:  []string{"false"},
			Response: true,
		}, m)

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
		h := newTestHandler(t, HandlerConfig{
			Name:    "test",
			Match:   map[string]string{"k": "v"},
			Command: []string{"echo"},
		}, m)
		if h.shouldIgnoreResponse(&CommandResult{ExitCode: 99}) {
			t.Error("expected false when responseIgnore is nil")
		}
	})

	t.Run("matching exit code", func(t *testing.T) {
		exitCode := 99
		h := newTestHandler(t, HandlerConfig{
			Name:           "test",
			Match:          map[string]string{"k": "v"},
			Command:        []string{"echo"},
			Response:       true,
			ResponseIgnore: &ResponseIgnoreConfig{ExitCode: &exitCode},
		}, m)
		if !h.shouldIgnoreResponse(&CommandResult{ExitCode: 99}) {
			t.Error("expected true for matching exit code")
		}
	})

	t.Run("non-matching exit code", func(t *testing.T) {
		exitCode := 99
		h := newTestHandler(t, HandlerConfig{
			Name:           "test",
			Match:          map[string]string{"k": "v"},
			Command:        []string{"echo"},
			Response:       true,
			ResponseIgnore: &ResponseIgnoreConfig{ExitCode: &exitCode},
		}, m)
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

func TestLogHandlerMessage(t *testing.T) {
	m := newTestMetrics(t)

	t.Run("with log_message and log_body_fields", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:          "test",
			Match:         map[string]string{"x-type": "order"},
			Command:       []string{"echo"},
			LogMessage:    "processing order",
			LogBodyFields: []string{"order_id", "status"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{"x-type": "order"},
			Body:    []byte(`{"order_id":"ORD-001","status":"pending","secret":"s3cret"}`),
		}
		// Should not panic; log output is visual confirmation
		h.logHandlerMessage(t.Context(), msg, "msg-1")
	})

	t.Run("with log_header_fields", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:            "test",
			Match:           map[string]string{"x-type": "order"},
			Command:         []string{"echo"},
			LogMessage:      "processing order",
			LogHeaderFields: []string{"rabbitmq.routing_key", "x-type"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{
				"x-type":               "order",
				"rabbitmq.routing_key": "order.created",
				"rabbitmq.exchange":    "events",
			},
			Body: []byte(`{}`),
		}
		// Should log header.rabbitmq.routing_key and header.x-type but not header.rabbitmq.exchange
		h.logHandlerMessage(t.Context(), msg, "msg-h1")
	})

	t.Run("with log_header_fields and log_body_fields", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:            "test",
			Match:           map[string]string{"x-type": "order"},
			Command:         []string{"echo"},
			LogMessage:      "processing order",
			LogHeaderFields: []string{"rabbitmq.routing_key"},
			LogBodyFields:   []string{"order_id"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{
				"x-type":               "order",
				"rabbitmq.routing_key": "order.created",
			},
			Body: []byte(`{"order_id":"ORD-001"}`),
		}
		// Should log both header and body fields
		h.logHandlerMessage(t.Context(), msg, "msg-h2")
	})

	t.Run("with log_header_fields missing header", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:            "test",
			Match:           map[string]string{"x-type": "order"},
			Command:         []string{"echo"},
			LogMessage:      "processing order",
			LogHeaderFields: []string{"nonexistent-header"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{"x-type": "order"},
			Body:    []byte(`{}`),
		}
		// Missing header should be silently skipped
		h.logHandlerMessage(t.Context(), msg, "msg-h3")
	})

	t.Run("with log_message only (no body fields)", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:       "test",
			Match:      map[string]string{"x-type": "order"},
			Command:    []string{"echo"},
			LogMessage: "got a message",
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{"x-type": "order"},
			Body:    []byte(`not json`),
		}
		// No body parsing, should not warn
		h.logHandlerMessage(t.Context(), msg, "msg-2")
	})

	t.Run("no log_message configured", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:    "test",
			Match:   map[string]string{"x-type": "order"},
			Command: []string{"echo"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{"x-type": "order"},
			Body:    []byte(`{"order_id":"ORD-001"}`),
		}
		// Should be a no-op
		h.logHandlerMessage(t.Context(), msg, "msg-3")
	})

	t.Run("invalid JSON body with log_body_fields", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:          "test",
			Match:         map[string]string{"x-type": "order"},
			Command:       []string{"echo"},
			LogMessage:    "process",
			LogBodyFields: []string{"order_id"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{"x-type": "order"},
			Body:    []byte(`not json`),
		}
		// Should warn but not panic
		h.logHandlerMessage(t.Context(), msg, "msg-4")
	})

	t.Run("missing field in body", func(t *testing.T) {
		h := newTestHandler(t, HandlerConfig{
			Name:          "test",
			Match:         map[string]string{"x-type": "order"},
			Command:       []string{"echo"},
			LogMessage:    "process",
			LogBodyFields: []string{"nonexistent"},
		}, m)
		msg := &mqbridge.Message{
			Headers: map[string]string{"x-type": "order"},
			Body:    []byte(`{"order_id":"ORD-001"}`),
		}
		// Should log without the missing field
		h.logHandlerMessage(t.Context(), msg, "msg-5")
	})
}

func TestBuildEnv(t *testing.T) {
	handlerEnv := map[string]string{
		"MY_VAR": "hello",
	}
	headers := map[string]string{
		"rabbitmq.routing_key":     "test.key",
		"rabbitmq.header.x-custom": "value",
	}
	env := buildEnv(t.Context(), handlerEnv, headers)
	envMap := make(map[string]string)
	for _, e := range env {
		for i, c := range e {
			if c == '=' {
				envMap[e[:i]] = e[i+1:]
				break
			}
		}
	}

	// Should include parent process env (PATH should exist)
	if _, ok := envMap["PATH"]; !ok {
		t.Error("expected parent process PATH to be inherited")
	}
	// Should include handler env
	if envMap["MY_VAR"] != "hello" {
		t.Errorf("handler env MY_VAR: expected %q, got %q", "hello", envMap["MY_VAR"])
	}
	// Should include header env
	expected := map[string]string{
		"MQ_HEADER_RABBITMQ_ROUTING_KEY":     "test.key",
		"MQ_HEADER_RABBITMQ_HEADER_X_CUSTOM": "value",
	}
	for k, v := range expected {
		if envMap[k] != v {
			t.Errorf("env %s: expected %q, got %q", k, v, envMap[k])
		}
	}
}

func TestBuildEnvTraceparent(t *testing.T) {
	// Set up a real tracer provider so spans have valid trace IDs
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(t.Context())
	otel.SetTracerProvider(tp)

	ctx, span := otel.Tracer(tracerName).Start(t.Context(), "test-span")
	defer span.End()

	env := buildEnv(ctx, nil, nil)
	envMap := make(map[string]string)
	for _, e := range env {
		for i, c := range e {
			if c == '=' {
				envMap[e[:i]] = e[i+1:]
				break
			}
		}
	}

	// TRACEPARENT should be set from the active span
	tp2 := envMap["TRACEPARENT"]
	if tp2 == "" {
		t.Fatal("expected TRACEPARENT to be set")
	}
	// Verify it contains the correct trace ID
	sc := span.SpanContext()
	if !sc.TraceID().IsValid() {
		t.Fatal("expected valid trace ID")
	}
	traceID := sc.TraceID().String()
	if got := tp2; len(got) < 36 || got[3:35] != traceID {
		t.Errorf("TRACEPARENT trace ID mismatch: expected %s in %s", traceID, got)
	}
}

func TestExtractTraceContextFromEnv(t *testing.T) {
	traceparent := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	t.Setenv("TRACEPARENT", traceparent)

	ctx := extractTraceContextFromEnv(t.Context())
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("expected valid span context from TRACEPARENT env")
	}
	if got := sc.TraceID().String(); got != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace ID: expected %s, got %s", "0af7651916cd43dd8448eb211c80319c", got)
	}
	if got := sc.SpanID().String(); got != "b7ad6b7169203331" {
		t.Errorf("span ID: expected %s, got %s", "b7ad6b7169203331", got)
	}
}
