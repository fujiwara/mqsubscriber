package subscriber

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/fujiwara/mqbridge"
	"go.opentelemetry.io/otel/attribute"
)

// PublishCmd is the "publish" subcommand.
type PublishCmd struct {
	Header   map[string]string `short:"H" help:"Message header as key=value (repeatable)"`
	Body     string            `help:"Message body string"`
	BodyFile string            `help:"Read message body from file" type:"existingfile"`
	Request  bool              `help:"Publish to the request queue (default)" xor:"queue"`
	Response bool              `help:"Publish to the response queue" xor:"queue"`
}

func (c *PublishCmd) Run(ctx context.Context, globals *CLI) error {
	cfg, err := LoadConfig(ctx, globals.Config)
	if err != nil {
		return err
	}

	body, err := c.readBody()
	if err != nil {
		return err
	}

	msg := &mqbridge.Message{
		Body:    body,
		Headers: c.Header,
	}
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}

	// Restore parent trace context from TRACEPARENT environment variable (W3C standard)
	ctx = extractTraceContextFromEnv(ctx)

	tracer := newTracer()
	ctx, span := tracer.Start(ctx, "publish")
	defer span.End()
	injectTraceContext(ctx, msg.Headers)

	var pub QueueClient
	queue := cfg.RequestQueue
	if c.Response {
		pub, err = newResponsePublisher(cfg)
		queue = cfg.ResponseQueue
	} else {
		pub, err = newRequestPublisher(cfg)
	}
	if err != nil {
		return fmt.Errorf("failed to create publisher: %w", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, msg); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	span.SetAttributes(
		attribute.String("messaging.destination.name", queue),
		attribute.Int("messaging.message.body.size", len(body)),
	)
	slog.Info("message published", "queue", queue, "headers", c.Header, "body_size", len(body))
	return nil
}

func (c *PublishCmd) readBody() ([]byte, error) {
	switch {
	case c.Body != "" && c.BodyFile != "":
		return nil, fmt.Errorf("--body and --body-file are mutually exclusive")
	case c.Body != "":
		return []byte(c.Body), nil
	case c.BodyFile != "":
		return os.ReadFile(c.BodyFile)
	default:
		return io.ReadAll(os.Stdin)
	}
}

// newRequestPublisher creates a QueueClient publisher targeting the request queue.
func newRequestPublisher(cfg *Config) (QueueClient, error) {
	switch cfg.BackendType() {
	case BackendRabbitMQ:
		// RabbitMQPublisher uses config.RMQResponse for publish destination.
		// Create a config copy with RMQResponse pointing to the request queue,
		// so headers (rabbitmq.exchange, rabbitmq.routing_key) in the message
		// can override the destination as usual.
		pubCfg := *cfg
		pubCfg.RMQResponse = &RMQResponseConfig{
			Queue: cfg.RMQRequest.Queue,
		}
		return NewRabbitMQPublisher(&pubCfg), nil
	default:
		return NewSimpleMQPublisher(
			cfg.SMQRequest.APIURL,
			cfg.SMQRequest.APIKey,
			cfg.SMQRequest.Queue,
			cfg.SimpleMQ.GetTimeout(),
		)
	}
}

// newResponsePublisher creates a QueueClient publisher targeting the response queue.
func newResponsePublisher(cfg *Config) (QueueClient, error) {
	switch cfg.BackendType() {
	case BackendRabbitMQ:
		if cfg.RMQResponse == nil {
			return nil, fmt.Errorf("response queue is not configured")
		}
		return NewRabbitMQPublisher(cfg), nil
	default:
		if cfg.SMQResponse == nil {
			return nil, fmt.Errorf("response queue is not configured")
		}
		return NewSimpleMQPublisher(
			cfg.SMQResponse.APIURL,
			cfg.SMQResponse.APIKey,
			cfg.SMQResponse.Queue,
			cfg.SimpleMQ.GetTimeout(),
		)
	}
}
