package subscriber

import (
	"context"

	"github.com/fujiwara/mqbridge"
)

// QueueMessage represents a message received from a queue.
type QueueMessage struct {
	ID       string
	Message  *mqbridge.Message
	internal any // backend-specific data (e.g., message.MessageId, *amqp.Delivery)
}

// QueueClient abstracts queue operations for different MQ backends.
type QueueClient interface {
	// Receive returns a single message from the queue.
	// Returns (nil, nil) when no messages are available.
	Receive(ctx context.Context) (*QueueMessage, error)

	// Publish sends a message to the response queue.
	Publish(ctx context.Context, msg *mqbridge.Message) error

	// Ack acknowledges a message (SimpleMQ: DELETE, RabbitMQ: Ack).
	Ack(ctx context.Context, qmsg *QueueMessage) error

	// Nack negatively acknowledges a message.
	// SimpleMQ: no-op (visibility timeout handles redelivery).
	// RabbitMQ: Nack without requeue (message routed to dead-letter exchange if configured).
	Nack(ctx context.Context, qmsg *QueueMessage) error

	// Close releases resources held by the client.
	Close() error
}
