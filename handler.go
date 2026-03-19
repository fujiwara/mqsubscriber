package subscriber

import (
	"bytes"
	"context"
	"log/slog"
	"maps"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/fujiwara/mqbridge"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// CommandResult holds the output of a command execution.
type CommandResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
	Err      error // non-nil if command failed (non-zero exit or execution error)
}

// Handler matches messages by headers and executes a command.
type Handler struct {
	name           string
	match          map[string]string
	command        []string
	timeout        time.Duration
	blocking       bool
	response       bool
	responseIgnore *ResponseIgnoreConfig
	maxConcurrency int
	sem            chan struct{} // semaphore for non-blocking concurrency control
	logger         *slog.Logger
	metrics        *Metrics
	attrs          attribute.Set
}

// NewHandler creates a Handler from config.
func NewHandler(cfg HandlerConfig, logger *slog.Logger, m *Metrics) *Handler {
	h := &Handler{
		name:           cfg.Name,
		match:          cfg.Match,
		command:        cfg.Command,
		timeout:        cfg.GetTimeout(),
		blocking:       cfg.Blocking,
		response:       cfg.Response,
		responseIgnore: cfg.ResponseIgnore,
		maxConcurrency: cfg.GetMaxConcurrency(),
		logger:         logger.With("handler", cfg.Name),
		metrics:        m,
		attrs: attribute.NewSet(
			attribute.String("handler", cfg.Name),
		),
	}
	if !h.blocking {
		h.sem = make(chan struct{}, h.maxConcurrency)
	}
	return h
}

// Match returns true if all match conditions are satisfied by the message headers (exact match).
func (h *Handler) Match(msg *mqbridge.Message) bool {
	if msg.Headers == nil {
		return false
	}
	for key, want := range h.match {
		got, ok := msg.Headers[key]
		if !ok || got != want {
			return false
		}
	}
	return true
}

// Execute runs the command with the message body as stdin and returns the result.
func (h *Handler) Execute(ctx context.Context, msg *mqbridge.Message) *CommandResult {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "simplemq_subscriber.execute",
		trace.WithAttributes(
			attribute.String("handler", h.name),
			attribute.String("command", h.command[0]),
		),
	)
	defer span.End()

	cmdCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	start := time.Now()

	cmd := exec.CommandContext(cmdCtx, h.command[0], h.command[1:]...)
	cmd.Stdin = bytes.NewReader(msg.Body)

	// Set headers as environment variables
	cmd.Env = headersToEnv(msg.Headers)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	duration := time.Since(start).Seconds()
	h.metrics.commandDuration.Record(ctx, duration, metric.WithAttributeSet(h.attrs))

	if stderr.Len() > 0 {
		h.logger.InfoContext(ctx, "command stderr", "stderr", stderr.String())
	}

	result := &CommandResult{
		Stdout: stdout.Bytes(),
		Stderr: stderr.Bytes(),
		Err:    err,
	}
	if err != nil {
		if cmd.ProcessState != nil {
			result.ExitCode = cmd.ProcessState.ExitCode()
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "command failed")
	}
	return result
}

// Acquire acquires a semaphore slot for non-blocking handlers.
// Blocks if max_concurrency is reached.
func (h *Handler) Acquire(ctx context.Context) error {
	if h.blocking {
		return nil
	}
	select {
	case h.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release releases a semaphore slot for non-blocking handlers.
func (h *Handler) Release() {
	if h.blocking {
		return
	}
	<-h.sem
}

// shouldIgnoreResponse returns true if the command result matches the response_ignore condition.
func (h *Handler) shouldIgnoreResponse(result *CommandResult) bool {
	if h.responseIgnore == nil || h.responseIgnore.ExitCode == nil {
		return false
	}
	return result.ExitCode == *h.responseIgnore.ExitCode
}

// buildResponse constructs a response message from the original request.
// It copies headers, applies RPC routing when reply_to is present,
// and sets x-status / x-exit-code headers.
func (h *Handler) buildResponse(req *mqbridge.Message, body []byte, status string, exitCode int) *mqbridge.Message {
	respHeaders := copyHeaders(req.Headers)

	// Determine status header key prefix based on context:
	// RabbitMQ (reply_to present) -> "rabbitmq.header." prefix for AMQP header mapping
	// SimpleMQ-only              -> no prefix
	statusKey := "x-status"
	exitCodeKey := "x-exit-code"
	if replyTo := req.Headers["rabbitmq.reply_to"]; replyTo != "" {
		// RPC response routing: route via default exchange to the reply queue
		respHeaders["rabbitmq.exchange"] = ""
		respHeaders["rabbitmq.routing_key"] = replyTo
		delete(respHeaders, "rabbitmq.reply_to")
		statusKey = "rabbitmq.header." + statusKey
		exitCodeKey = "rabbitmq.header." + exitCodeKey
	}

	respHeaders[statusKey] = status
	if exitCode != 0 {
		respHeaders[exitCodeKey] = strconv.Itoa(exitCode)
	}

	return &mqbridge.Message{
		Body:    body,
		Headers: respHeaders,
	}
}

// maxErrorBodySize is the maximum size of stderr included in error responses.
// Only the last 4KB is kept, as the most relevant error information
// (final error messages, stack traces) typically appears at the end.
const maxErrorBodySize = 4096

// tailBytes returns the last n bytes of b. If b is shorter than n, returns b as-is.
func tailBytes(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[len(b)-n:]
}

// headersToEnv converts message headers to environment variables.
// e.g. "rabbitmq.routing_key" -> "SIMPLEMQ_HEADER_RABBITMQ_ROUTING_KEY=value"
func headersToEnv(headers map[string]string) []string {
	env := make([]string, 0, len(headers))
	for k, v := range headers {
		envKey := "SIMPLEMQ_HEADER_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(k))
		env = append(env, envKey+"="+v)
	}
	return env
}

func copyHeaders(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}
