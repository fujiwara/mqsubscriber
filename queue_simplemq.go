package subscriber

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"github.com/fujiwara/mqbridge"
	simplemq "github.com/sacloud/simplemq-api-go"
	"github.com/sacloud/simplemq-api-go/apis/v1/message"
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

// SimpleMQReceiver implements QueueClient for receiving from SimpleMQ.
type SimpleMQReceiver struct {
	client  *message.Client
	queue   string
	timeout time.Duration
	buf     []message.Message // buffered messages from a single poll
}

// NewSimpleMQReceiver creates a new SimpleMQReceiver.
func NewSimpleMQReceiver(apiURL, apiKey, queue string, timeout time.Duration) (*SimpleMQReceiver, error) {
	client, err := newSimpleMQClient(apiURL, apiKey)
	if err != nil {
		return nil, err
	}
	return &SimpleMQReceiver{client: client, queue: queue, timeout: timeout}, nil
}

// Receive returns a single message from the queue.
// Internally buffers multiple messages from a single poll and returns them one at a time.
func (r *SimpleMQReceiver) Receive(ctx context.Context) (*QueueMessage, error) {
	if len(r.buf) == 0 {
		ctx, cancel := context.WithTimeout(ctx, r.timeout)
		defer cancel()
		res, err := r.client.ReceiveMessage(ctx, message.ReceiveMessageParams{
			QueueName: message.QueueName(r.queue),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to receive message: %w", err)
		}
		recvOK, ok := res.(*message.ReceiveMessageOK)
		if !ok {
			return nil, fmt.Errorf("unexpected response type: %T", res)
		}
		if len(recvOK.Messages) == 0 {
			return nil, nil
		}
		r.buf = recvOK.Messages
	}

	raw := r.buf[0]
	r.buf = r.buf[1:]

	decoded, err := base64.StdEncoding.DecodeString(string(raw.Content))
	if err != nil {
		// Invalid message: ack (delete) it and return a QueueMessage with nil Message
		// so the caller can identify and skip it.
		slog.Error("failed to decode message content, deleting invalid message",
			"message_id", raw.ID, "error", err)
		return &QueueMessage{
			ID:       string(raw.ID),
			Message:  nil,
			internal: raw.ID,
		}, nil
	}

	msg := mqbridge.UnmarshalMessage(decoded)
	return &QueueMessage{
		ID:       string(raw.ID),
		Message:  msg,
		internal: raw.ID,
	}, nil
}

// Publish is not supported on SimpleMQReceiver. Use SimpleMQPublisher instead.
func (r *SimpleMQReceiver) Publish(_ context.Context, _ *mqbridge.Message) error {
	return fmt.Errorf("SimpleMQReceiver does not support Publish")
}

// Ack deletes the message from the SimpleMQ queue.
func (r *SimpleMQReceiver) Ack(ctx context.Context, qmsg *QueueMessage) error {
	msgID, ok := qmsg.internal.(message.MessageId)
	if !ok {
		return fmt.Errorf("invalid internal type for SimpleMQ Ack: %T", qmsg.internal)
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	_, err := r.client.DeleteMessage(ctx, message.DeleteMessageParams{
		QueueName: message.QueueName(r.queue),
		MessageId: msgID,
	})
	return err
}

// Nack is a no-op for SimpleMQ (messages are redelivered after visibility timeout).
func (r *SimpleMQReceiver) Nack(_ context.Context, _ *QueueMessage) error {
	return nil
}

// Close is a no-op for SimpleMQ (HTTP client, no persistent connection).
func (r *SimpleMQReceiver) Close() error {
	return nil
}

// SimpleMQPublisher implements QueueClient for publishing to SimpleMQ.
type SimpleMQPublisher struct {
	client  *message.Client
	queue   string
	timeout time.Duration
}

// NewSimpleMQPublisher creates a new SimpleMQPublisher.
func NewSimpleMQPublisher(apiURL, apiKey, queue string, timeout time.Duration) (*SimpleMQPublisher, error) {
	client, err := newSimpleMQClient(apiURL, apiKey)
	if err != nil {
		return nil, err
	}
	return &SimpleMQPublisher{client: client, queue: queue, timeout: timeout}, nil
}

// Receive is not supported on SimpleMQPublisher.
func (p *SimpleMQPublisher) Receive(_ context.Context) (*QueueMessage, error) {
	return nil, fmt.Errorf("SimpleMQPublisher does not support Receive")
}

// Publish sends a message to the SimpleMQ response queue.
func (p *SimpleMQPublisher) Publish(ctx context.Context, msg *mqbridge.Message) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	data, err := mqbridge.MarshalMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	res, err := p.client.SendMessage(ctx,
		&message.SendRequest{Content: message.MessageContent(encoded)},
		message.SendMessageParams{QueueName: message.QueueName(p.queue)},
	)
	if err != nil {
		return fmt.Errorf("failed to send message to response queue %q: %w", p.queue, err)
	}
	if _, ok := res.(*message.SendMessageOK); !ok {
		return fmt.Errorf("unexpected response type from SimpleMQ: %T", res)
	}
	return nil
}

// Ack is not supported on SimpleMQPublisher.
func (p *SimpleMQPublisher) Ack(_ context.Context, _ *QueueMessage) error {
	return fmt.Errorf("SimpleMQPublisher does not support Ack")
}

// Nack is not supported on SimpleMQPublisher.
func (p *SimpleMQPublisher) Nack(_ context.Context, _ *QueueMessage) error {
	return fmt.Errorf("SimpleMQPublisher does not support Nack")
}

// Close is a no-op for SimpleMQ (HTTP client, no persistent connection).
func (p *SimpleMQPublisher) Close() error {
	return nil
}
