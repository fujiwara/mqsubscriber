package subscriber

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/fujiwara/mqbridge"
)

// PublishCmd is the "publish" subcommand.
type PublishCmd struct {
	Header   map[string]string `short:"H" help:"Message header as key=value (repeatable)"`
	Body     string            `help:"Message body string"`
	BodyFile string            `help:"Read message body from file" type:"existingfile"`
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

	pub, err := newRequestPublisher(cfg)
	if err != nil {
		return fmt.Errorf("failed to create publisher: %w", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	slog.Info("message published", "queue", cfg.RequestQueue, "headers", c.Header, "body_size", len(body))
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
