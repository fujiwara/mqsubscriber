package subscriber

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fujiwara/mqbridge"
	simplemq "github.com/sacloud/simplemq-api-go"
	"github.com/sacloud/simplemq-api-go/apis/v1/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// publishRetryCount is the number of retry attempts for response publishing.
	publishRetryCount = 3
	// publishRetryBaseInterval is the base interval for exponential backoff.
	publishRetryBaseInterval = time.Second
)

// apiKeySource implements message.SecuritySource.
type apiKeySource struct {
	apiKey string
}

func (s *apiKeySource) ApiKeyAuth(_ context.Context, _ message.OperationName) (message.ApiKeyAuth, error) {
	return message.ApiKeyAuth{Token: s.apiKey}, nil
}

func newSimpleMQClient(apiURL, apiKey string) (*message.Client, error) {
	if apiURL == "" {
		apiURL = simplemq.DefaultMessageAPIRootURL
	}
	return message.NewClient(apiURL, &apiKeySource{apiKey: apiKey})
}

// App holds the application state.
type App struct {
	config    *Config
	handlers  []*Handler
	reqClient *message.Client
	resClient *message.Client
	metrics   *Metrics
	tracer    trace.Tracer
	wg        sync.WaitGroup
}

// New creates a new App from a config.
func New(cfg *Config) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	m, err := newMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics: %w", err)
	}

	reqClient, err := newSimpleMQClient(cfg.Request.APIURL, cfg.Request.APIKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create request queue client: %w", err)
	}
	var resClient *message.Client
	if cfg.hasResponseQueue() {
		resClient, err = newSimpleMQClient(cfg.Response.APIURL, cfg.Response.APIKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create response queue client: %w", err)
		}
	}

	var handlers []*Handler
	for _, hc := range cfg.Handlers {
		logger := slog.Default()
		handlers = append(handlers, NewHandler(hc, logger, m))
	}

	return &App{
		config:    cfg,
		handlers:  handlers,
		reqClient: reqClient,
		resClient: resClient,
		metrics:   m,
		tracer:    newTracer(),
	}, nil
}

// Run starts the subscriber loop.
func (a *App) Run(ctx context.Context) error {
	logAttrs := []any{
		"request_queue", a.config.Request.Queue,
		"handlers", len(a.handlers),
	}
	if a.config.Response.Queue != "" {
		logAttrs = append(logAttrs, "response_queue", a.config.Response.Queue)
	}
	slog.Info("starting simplemq-subscriber", logAttrs...)

	interval := a.config.Request.GetPollingInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping subscriber, waiting for in-flight handlers")
			a.wg.Wait()
			slog.Info("subscriber stopped")
			return nil
		case <-ticker.C:
			a.drainQueue(ctx)
		}
	}
}

// drainQueue polls repeatedly until the queue is empty or an error occurs.
func (a *App) drainQueue(ctx context.Context) {
	for {
		n, err := a.poll(ctx)
		if err != nil {
			slog.Error("poll error", "error", err)
			return
		}
		if n == 0 {
			return
		}
	}
}

func (a *App) poll(ctx context.Context) (int, error) {
	res, err := a.reqClient.ReceiveMessage(ctx, message.ReceiveMessageParams{
		QueueName: message.QueueName(a.config.Request.Queue),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to receive message: %w", err)
	}
	recvOK, ok := res.(*message.ReceiveMessageOK)
	if !ok {
		return 0, fmt.Errorf("unexpected response type: %T", res)
	}

	// Use non-cancellable context for all message processing so that
	// in-flight work (command execution, response publish, request delete)
	// completes even during shutdown.
	msgCtx := context.WithoutCancel(ctx)

	for _, raw := range recvOK.Messages {
		decoded, err := base64.StdEncoding.DecodeString(string(raw.Content))
		if err != nil {
			slog.Error("failed to decode message content, deleting invalid message",
				"messageId", raw.ID,
				"error", err,
			)
			a.deleteMessage(msgCtx, raw.ID)
			continue
		}

		msg := mqbridge.UnmarshalMessage(decoded)
		a.metrics.messagesReceived.Add(msgCtx, 1)

		handler := a.findHandler(msg)
		if handler == nil {
			slog.Warn("no matching handler, dropping message",
				"messageId", raw.ID,
				"headers", msg.Headers,
			)
			a.metrics.messagesDropped.Add(msgCtx, 1)
			a.deleteMessage(msgCtx, raw.ID)
			continue
		}

		if handler.blocking {
			a.handleBlocking(msgCtx, handler, msg, raw.ID)
		} else {
			// Acquire semaphore before spawning goroutine (blocks if at max_concurrency)
			if err := handler.Acquire(ctx); err != nil {
				return 0, err // context cancelled
			}
			a.wg.Go(func() {
				defer handler.Release()
				a.handleBlocking(msgCtx, handler, msg, raw.ID)
			})
		}
	}
	return len(recvOK.Messages), nil
}

func (a *App) findHandler(msg *mqbridge.Message) *Handler {
	for _, h := range a.handlers {
		if h.Match(msg) {
			return h
		}
	}
	return nil
}

func (a *App) handleBlocking(ctx context.Context, handler *Handler, msg *mqbridge.Message, msgID message.MessageId) {
	// Extract trace context from message headers (traceparent or rabbitmq.header.traceparent)
	ctx = extractTraceContext(ctx, msg.Headers)

	ctx, span := a.tracer.Start(ctx, "simplemq_subscriber.handle_message",
		trace.WithAttributes(
			attribute.String("handler", handler.name),
			attribute.String("message_id", string(msgID)),
			attribute.Bool("blocking", handler.blocking),
		),
		trace.WithAttributes(headerAttributes("request.header.", msg.Headers)...),
	)
	defer span.End()

	handler.logger.InfoContext(ctx, "handling message", "messageId", msgID)
	handler.logHandlerMessage(ctx, msg, string(msgID))

	result := handler.Execute(ctx, msg)

	switch {
	case result.Err != nil && handler.shouldIgnoreResponse(result):
		// response_ignore matched: suppress response, delete message
		handler.logger.InfoContext(ctx, "response ignored by exit code",
			"messageId", msgID, "exit_code", result.ExitCode)

	case result.Err != nil && !handler.response:
		// fire-and-forget failure: don't delete, will be redelivered
		span.RecordError(result.Err)
		span.SetStatus(codes.Error, "command execution failed")
		handler.logger.ErrorContext(ctx, "command execution failed. no response will be sent since response mode is disabled",
			"messageId", msgID, "error", result.Err, "exit_code", result.ExitCode)
		a.metrics.messageErrors.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
		return

	case result.Err != nil && handler.response:
		// response mode failure: send error response, then delete
		handler.logger.InfoContext(ctx, "command execution failed, sending error response",
			"messageId", msgID, "error", result.Err, "exit_code", result.ExitCode)
		resp := handler.buildResponse(msg, tailBytes(result.Stderr, maxErrorBodySize), "error", result.ExitCode)
		a.publishResponse(ctx, span, handler, resp, msgID)

	case handler.response:
		// response mode success: send success response, then delete
		handler.logger.InfoContext(ctx, "command execution succeeded, sending success response",
			"messageId", msgID)
		resp := handler.buildResponse(msg, result.Stdout, "success", 0)
		a.publishResponse(ctx, span, handler, resp, msgID)

	default:
		// fire-and-forget success: just delete
		handler.logger.InfoContext(ctx, "command execution succeeded", "messageId", msgID)
	}

	a.deleteMessage(ctx, msgID)
	a.metrics.messagesProcessed.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
	handler.logger.InfoContext(ctx, "message processed", "messageId", msgID)
}

// publishResponse injects trace context and publishes a response message with
// retry. After exhausting all retries, it logs the error and returns nil so
// the caller proceeds to delete the request message (preventing command
// re-execution on redelivery).
func (a *App) publishResponse(ctx context.Context, span trace.Span, handler *Handler, resp *mqbridge.Message, msgID message.MessageId) {
	injectTraceContext(ctx, resp.Headers)
	var lastErr error
	for attempt := range publishRetryCount {
		if attempt > 0 {
			backoff := publishRetryBaseInterval * (1 << (attempt - 1)) // 1s, 2s, 4s
			handler.logger.InfoContext(ctx, "retrying response publish",
				"messageId", msgID, "attempt", attempt+1, "backoff", backoff)
			time.Sleep(backoff)
		}
		if err := a.publishResult(ctx, resp); err != nil {
			lastErr = err
			continue
		}
		return
	}
	// All retries exhausted: record error but proceed to delete
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, "failed to publish result after retries")
	handler.logger.ErrorContext(ctx, "failed to publish result after retries, deleting message",
		"messageId", msgID, "error", lastErr, "retries", publishRetryCount)
	a.metrics.messageErrors.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
}

func (a *App) publishResult(ctx context.Context, msg *mqbridge.Message) error {
	ctx, span := a.tracer.Start(ctx, "simplemq_subscriber.publish",
		trace.WithAttributes(
			attribute.String("queue", a.config.Response.Queue),
		),
		trace.WithAttributes(headerAttributes("response.header.", msg.Headers)...),
	)
	defer span.End()

	data, err := mqbridge.MarshalMessage(msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to marshal message")
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	res, err := a.resClient.SendMessage(ctx,
		&message.SendRequest{Content: message.MessageContent(encoded)},
		message.SendMessageParams{QueueName: message.QueueName(a.config.Response.Queue)},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to send message")
		return fmt.Errorf("failed to send message to response queue %q: %w", a.config.Response.Queue, err)
	}
	if _, ok := res.(*message.SendMessageOK); !ok {
		return fmt.Errorf("unexpected response type from SimpleMQ: %T", res)
	}
	return nil
}

func (a *App) deleteMessage(ctx context.Context, msgID message.MessageId) {
	if _, err := a.reqClient.DeleteMessage(ctx, message.DeleteMessageParams{
		QueueName: message.QueueName(a.config.Request.Queue),
		MessageId: msgID,
	}); err != nil {
		slog.Error("failed to delete message", "messageId", msgID, "error", err)
	}
}
