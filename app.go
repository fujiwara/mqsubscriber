package subscriber

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fujiwara/mqbridge"
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

// App holds the application state.
type App struct {
	config   *Config
	handlers []*Handler
	reqQueue QueueClient
	resQueue QueueClient
	metrics  *Metrics
	tracer   trace.Tracer
	wg       sync.WaitGroup
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

	reqQueue, resQueue, err := newQueueClients(cfg)
	if err != nil {
		return nil, err
	}

	var handlers []*Handler
	for _, hc := range cfg.Handlers {
		logger := slog.Default()
		h, err := NewHandler(hc, logger, m)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, h)
	}

	return &App{
		config:   cfg,
		handlers: handlers,
		reqQueue: reqQueue,
		resQueue: resQueue,
		metrics:  m,
		tracer:   newTracer(),
	}, nil
}

func newQueueClients(cfg *Config) (reqQueue QueueClient, resQueue QueueClient, err error) {
	switch cfg.BackendType() {
	case BackendRabbitMQ:
		prefetch := totalMaxConcurrency(cfg)
		reqQueue = NewRabbitMQReceiver(cfg, prefetch)
		if cfg.hasResponseQueue() {
			resQueue = NewRabbitMQPublisher(cfg)
		}
	default:
		reqQueue, err = NewSimpleMQReceiver(cfg.SMQRequest.APIURL, cfg.SMQRequest.APIKey, cfg.SMQRequest.Queue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create request queue client: %w", err)
		}
		if cfg.hasResponseQueue() {
			resQueue, err = NewSimpleMQPublisher(cfg.SMQResponse.APIURL, cfg.SMQResponse.APIKey, cfg.SMQResponse.Queue)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create response queue client: %w", err)
			}
		}
	}
	return reqQueue, resQueue, nil
}

// totalMaxConcurrency returns the sum of max_concurrency across all handlers,
// used as the RabbitMQ prefetch count.
func totalMaxConcurrency(cfg *Config) int {
	total := 0
	for _, h := range cfg.Handlers {
		total += h.GetMaxConcurrency()
	}
	return total
}

// Run starts the subscriber loop.
func (a *App) Run(ctx context.Context) error {
	logAttrs := []any{
		"backend", a.config.BackendType(),
		"request_queue", a.config.RequestQueue,
		"handlers", len(a.handlers),
	}
	if a.config.ResponseQueue != "" {
		logAttrs = append(logAttrs, "response_queue", a.config.ResponseQueue)
	}
	slog.Info("starting subscriber", logAttrs...)

	switch a.config.BackendType() {
	case BackendRabbitMQ:
		return a.runPushLoop(ctx)
	default:
		return a.runPollLoop(ctx)
	}
}

// runPollLoop runs the SimpleMQ polling loop with ticker-based drain.
func (a *App) runPollLoop(ctx context.Context) error {
	interval := a.config.SMQRequest.GetPollingInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return a.shutdown()
		case <-ticker.C:
			a.drainQueue(ctx)
		}
	}
}

// runPushLoop runs the RabbitMQ push-based receive loop.
// Receive blocks until a message arrives or context is cancelled.
func (a *App) runPushLoop(ctx context.Context) error {
	for {
		_, err := a.poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return a.shutdown()
			}
			slog.Error("receive error", "error", err)
			// Continue; RabbitMQReceiver will reconnect on next Receive call.
		}
	}
}

func (a *App) shutdown() error {
	slog.Info("stopping subscriber, waiting for in-flight handlers")
	a.wg.Wait()
	slog.Info("subscriber stopped")
	return nil
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
	qmsg, err := a.reqQueue.Receive(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to receive message: %w", err)
	}
	if qmsg == nil {
		return 0, nil
	}

	// Use non-cancellable context for all message processing so that
	// in-flight work (command execution, response publish, request delete)
	// completes even during shutdown.
	msgCtx := context.WithoutCancel(ctx)

	// Invalid message (decode failed): ack and skip
	if qmsg.Message == nil {
		a.ackMessage(msgCtx, qmsg)
		return 1, nil
	}

	a.metrics.messagesReceived.Add(msgCtx, 1)

	handler := a.findHandler(qmsg.Message)
	if handler == nil {
		slog.Warn("no matching handler, dropping message",
			"messageId", qmsg.ID,
			"headers", qmsg.Message.Headers,
		)
		a.metrics.messagesDropped.Add(msgCtx, 1)
		a.ackMessage(msgCtx, qmsg)
		return 1, nil
	}

	if handler.blocking {
		a.handleMessage(msgCtx, handler, qmsg)
	} else {
		// Acquire semaphore before spawning goroutine (blocks if at max_concurrency)
		if err := handler.Acquire(ctx); err != nil {
			return 0, err // context cancelled
		}
		a.wg.Go(func() {
			defer handler.Release()
			a.handleMessage(msgCtx, handler, qmsg)
		})
	}
	return 1, nil
}

func (a *App) findHandler(msg *mqbridge.Message) *Handler {
	for _, h := range a.handlers {
		if h.Match(msg) {
			return h
		}
	}
	return nil
}

func (a *App) handleMessage(ctx context.Context, handler *Handler, qmsg *QueueMessage) {
	msg := qmsg.Message

	// Extract trace context from message headers (traceparent or rabbitmq.header.traceparent)
	ctx = extractTraceContext(ctx, msg.Headers)

	ctx, span := a.tracer.Start(ctx, "mqsubscriber.handle_message",
		trace.WithAttributes(
			attribute.String("handler", handler.name),
			attribute.String("message_id", qmsg.ID),
			attribute.Bool("blocking", handler.blocking),
		),
		trace.WithAttributes(headerAttributes("request.header.", msg.Headers)...),
	)
	defer span.End()

	handler.logger.InfoContext(ctx, "handling message", "messageId", qmsg.ID)
	handler.logHandlerMessage(ctx, msg, qmsg.ID)

	result := handler.Execute(ctx, msg)

	switch {
	case result.Err != nil && handler.shouldIgnoreResponse(result):
		// response_ignore matched: suppress response, delete message
		handler.logger.InfoContext(ctx, "response ignored by exit code",
			"messageId", qmsg.ID, "exit_code", result.ExitCode)

	case result.Err != nil && !handler.response:
		// fire-and-forget failure: nack for redelivery
		span.RecordError(result.Err)
		span.SetStatus(codes.Error, "command execution failed")
		handler.logger.ErrorContext(ctx, "command execution failed. no response will be sent since response mode is disabled",
			"messageId", qmsg.ID, "error", result.Err, "exit_code", result.ExitCode)
		a.metrics.messageErrors.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
		a.nackMessage(ctx, qmsg)
		return

	case result.Err != nil && handler.response:
		// response mode failure: send error response, then delete
		handler.logger.InfoContext(ctx, "command execution failed, sending error response",
			"messageId", qmsg.ID, "error", result.Err, "exit_code", result.ExitCode)
		resp := handler.buildResponse(msg, tailBytes(result.Stderr, maxErrorBodySize), "error", result.ExitCode)
		a.publishResponse(ctx, span, handler, resp, qmsg.ID)

	case handler.response:
		// response mode success: send success response, then delete
		handler.logger.InfoContext(ctx, "command execution succeeded, sending success response",
			"messageId", qmsg.ID)
		resp := handler.buildResponse(msg, result.Stdout, "success", 0)
		a.publishResponse(ctx, span, handler, resp, qmsg.ID)

	default:
		// fire-and-forget success: just delete
		handler.logger.InfoContext(ctx, "command execution succeeded", "messageId", qmsg.ID)
	}

	a.ackMessage(ctx, qmsg)
	a.metrics.messagesProcessed.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
	handler.logger.InfoContext(ctx, "message processed", "messageId", qmsg.ID)
}

// publishResponse injects trace context and publishes a response message with
// retry. After exhausting all retries, it logs the error so the caller
// proceeds to ack the request message (preventing command re-execution on
// redelivery).
func (a *App) publishResponse(ctx context.Context, span trace.Span, handler *Handler, resp *mqbridge.Message, msgID string) {
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
	// All retries exhausted: record error but proceed to ack
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, "failed to publish result after retries")
	handler.logger.ErrorContext(ctx, "failed to publish result after retries, deleting message",
		"messageId", msgID, "error", lastErr, "retries", publishRetryCount)
	a.metrics.messageErrors.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
}

func (a *App) publishResult(ctx context.Context, msg *mqbridge.Message) error {
	ctx, span := a.tracer.Start(ctx, "mqsubscriber.publish",
		trace.WithAttributes(
			attribute.String("queue", a.config.ResponseQueue),
		),
		trace.WithAttributes(headerAttributes("response.header.", msg.Headers)...),
	)
	defer span.End()

	if err := a.resQueue.Publish(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")
		return err
	}
	return nil
}

func (a *App) ackMessage(ctx context.Context, qmsg *QueueMessage) {
	if err := a.reqQueue.Ack(ctx, qmsg); err != nil {
		slog.Error("failed to ack message", "messageId", qmsg.ID, "error", err)
	}
}

func (a *App) nackMessage(ctx context.Context, qmsg *QueueMessage) {
	if err := a.reqQueue.Nack(ctx, qmsg); err != nil {
		slog.Error("failed to nack message", "messageId", qmsg.ID, "error", err)
	}
}
