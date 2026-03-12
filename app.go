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
	"go.opentelemetry.io/otel/metric"
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
	resClient, err := newSimpleMQClient(cfg.Response.APIURL, cfg.Response.APIKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create response queue client: %w", err)
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
	}, nil
}

// Run starts the subscriber loop.
func (a *App) Run(ctx context.Context) error {
	slog.Info("starting simplemq-subscriber",
		"request_queue", a.config.Request.Queue,
		"response_queue", a.config.Response.Queue,
		"handlers", len(a.handlers),
	)

	interval := a.config.Request.GetPollingInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping subscriber, waiting for in-flight handlers")
			a.wg.Wait()
			slog.Info("subscriber stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := a.poll(ctx); err != nil {
				slog.Error("poll error", "error", err)
			}
		}
	}
}

func (a *App) poll(ctx context.Context) error {
	res, err := a.reqClient.ReceiveMessage(ctx, message.ReceiveMessageParams{
		QueueName: message.QueueName(a.config.Request.Queue),
	})
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}
	recvOK, ok := res.(*message.ReceiveMessageOK)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", res)
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
				return err // context cancelled
			}
			a.wg.Go(func() {
				defer handler.Release()
				a.handleBlocking(msgCtx, handler, msg, raw.ID)
			})
		}
	}
	return nil
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
	handler.logger.Debug("handling message", "messageId", msgID)

	result, err := handler.Execute(ctx, msg)
	if err != nil {
		handler.logger.Error("command execution failed",
			"messageId", msgID,
			"error", err,
		)
		a.metrics.messageErrors.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
		return // don't delete message, will be redelivered
	}

	if handler.response {
		if err := a.publishResult(ctx, result); err != nil {
			handler.logger.Error("failed to publish result",
				"messageId", msgID,
				"error", err,
			)
			a.metrics.messageErrors.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
			return // don't delete message, will be redelivered
		}
	}

	a.deleteMessage(ctx, msgID)
	a.metrics.messagesProcessed.Add(ctx, 1, metric.WithAttributeSet(handler.attrs))
	handler.logger.Debug("message processed", "messageId", msgID)
}

func (a *App) publishResult(ctx context.Context, msg *mqbridge.Message) error {
	data, err := mqbridge.MarshalMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	res, err := a.resClient.SendMessage(ctx,
		&message.SendRequest{Content: message.MessageContent(encoded)},
		message.SendMessageParams{QueueName: message.QueueName(a.config.Response.Queue)},
	)
	if err != nil {
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
