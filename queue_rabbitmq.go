package subscriber

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/fujiwara/mqbridge"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	rabbitMQInitialBackoff = 1 * time.Second
	rabbitMQMaxBackoff     = 30 * time.Second
)

// RabbitMQReceiver implements QueueClient for receiving from RabbitMQ.
type RabbitMQReceiver struct {
	config   *Config
	conn     *amqp.Connection
	netConn  net.Conn
	ch       *amqp.Channel
	msgs     <-chan amqp.Delivery
	mu       sync.Mutex
	prefetch int
	timeout  time.Duration
}

// NewRabbitMQReceiver creates a new RabbitMQReceiver.
func NewRabbitMQReceiver(cfg *Config, prefetch int) *RabbitMQReceiver {
	return &RabbitMQReceiver{
		config:   cfg,
		prefetch: prefetch,
		timeout:  cfg.RabbitMQ.GetTimeout(),
	}
}

// connect establishes connection, declares exchange/queue, and starts consuming.
func (r *RabbitMQReceiver) connect() error {
	var netConn net.Conn
	conn, err := amqp.DialConfig(r.config.RabbitMQ.URL, amqp.Config{
		Dial: dialWithCapture(r.timeout, &netConn),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}
	if r.prefetch > 0 {
		if err := ch.Qos(r.prefetch, 0, false); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("failed to set QoS: %w", err)
		}
	}

	req := r.config.RMQRequest
	exchangeType := req.ExchangeType
	if exchangeType == "" {
		exchangeType = "direct"
	}
	routingKeys := req.RoutingKey
	if len(routingKeys) == 0 {
		routingKeys = []string{"#"}
	}

	if req.Exchange != "" {
		if req.ExchangePassive {
			err = ch.ExchangeDeclarePassive(req.Exchange, exchangeType, true, false, false, false, nil)
		} else {
			err = ch.ExchangeDeclare(req.Exchange, exchangeType, true, false, false, false, nil)
		}
		if err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("failed to declare exchange %q: %w", req.Exchange, err)
		}
	}

	if _, err := ch.QueueDeclare(req.Queue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare queue %q: %w", req.Queue, err)
	}

	if req.Exchange != "" {
		for _, rk := range routingKeys {
			if err := ch.QueueBind(req.Queue, rk, req.Exchange, false, nil); err != nil {
				ch.Close()
				conn.Close()
				return fmt.Errorf("failed to bind queue %q to exchange %q with routing key %q: %w",
					req.Queue, req.Exchange, rk, err)
			}
		}
	}

	msgs, err := ch.Consume(req.Queue, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to consume from queue %q: %w", req.Queue, err)
	}

	r.conn = conn
	r.netConn = netConn
	r.ch = ch
	r.msgs = msgs
	return nil
}

// ensureConnected connects with exponential backoff if not already connected.
func (r *RabbitMQReceiver) ensureConnected(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn != nil && !r.conn.IsClosed() {
		return nil
	}
	r.closeConn()

	backoff := rabbitMQInitialBackoff
	for {
		if err := r.connect(); err != nil {
			slog.Error("RabbitMQ connection failed, retrying...", "error", err, "backoff", backoff)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			backoff = min(backoff*2, rabbitMQMaxBackoff)
			continue
		}
		slog.Info("RabbitMQ receiver connected", "queue", r.config.RequestQueue)
		return nil
	}
}

// Receive returns a single message from the RabbitMQ queue.
// Blocks until a message is available or context is cancelled.
func (r *RabbitMQReceiver) Receive(ctx context.Context) (*QueueMessage, error) {
	if err := r.ensureConnected(ctx); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, ok := <-r.msgs:
		if !ok {
			// Channel closed, reset connection for next call
			r.mu.Lock()
			r.closeConn()
			r.mu.Unlock()
			return nil, fmt.Errorf("RabbitMQ channel closed for queue %q", r.config.RequestQueue)
		}
		msg := messageFromDelivery(delivery)
		return &QueueMessage{
			ID:       fmt.Sprintf("%d", delivery.DeliveryTag),
			Message:  msg,
			internal: &delivery,
		}, nil
	}
}

// Publish is not supported on RabbitMQReceiver. Use RabbitMQPublisher instead.
func (r *RabbitMQReceiver) Publish(_ context.Context, _ *mqbridge.Message) error {
	return fmt.Errorf("RabbitMQReceiver does not support Publish")
}

// Ack acknowledges the message.
func (r *RabbitMQReceiver) Ack(_ context.Context, qmsg *QueueMessage) error {
	delivery, ok := qmsg.internal.(*amqp.Delivery)
	if !ok {
		return fmt.Errorf("invalid internal type for RabbitMQ Ack: %T", qmsg.internal)
	}
	return r.withDeadline(func() error {
		return delivery.Ack(false)
	})
}

// Nack negatively acknowledges the message with requeue.
func (r *RabbitMQReceiver) Nack(_ context.Context, qmsg *QueueMessage) error {
	delivery, ok := qmsg.internal.(*amqp.Delivery)
	if !ok {
		return fmt.Errorf("invalid internal type for RabbitMQ Nack: %T", qmsg.internal)
	}
	return r.withDeadline(func() error {
		return delivery.Nack(false, true)
	})
}

// withDeadline sets a write deadline on the underlying TCP connection before fn
// and clears it after. This provides timeout for operations like Ack/Nack
// that don't support context.
func (r *RabbitMQReceiver) withDeadline(fn func() error) error {
	if r.netConn != nil {
		if err := r.netConn.SetWriteDeadline(time.Now().Add(r.timeout)); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer r.netConn.SetWriteDeadline(time.Time{}) //nolint:errcheck
	}
	return fn()
}

// Close closes the RabbitMQ connection.
func (r *RabbitMQReceiver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeConn()
	return nil
}

func (r *RabbitMQReceiver) closeConn() {
	if r.ch != nil {
		r.ch.Close()
		r.ch = nil
	}
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
	r.netConn = nil
	r.msgs = nil
}

// RabbitMQPublisher implements QueueClient for publishing to RabbitMQ.
type RabbitMQPublisher struct {
	config  *Config
	conn    *amqp.Connection
	netConn net.Conn
	ch      *amqp.Channel
	mu      sync.Mutex
	timeout time.Duration
}

// NewRabbitMQPublisher creates a new RabbitMQPublisher.
func NewRabbitMQPublisher(cfg *Config) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		config:  cfg,
		timeout: cfg.RabbitMQ.GetTimeout(),
	}
}

func (p *RabbitMQPublisher) ensureConnected() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	var netConn net.Conn
	conn, err := amqp.DialConfig(p.config.RabbitMQ.URL, amqp.Config{
		Dial: dialWithCapture(p.timeout, &netConn),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}
	// Declare response queue if configured
	if q := p.config.RMQResponse.Queue; q != "" {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("failed to declare response queue %q: %w", q, err)
		}
	}
	p.conn = conn
	p.netConn = netConn
	p.ch = ch
	slog.Info("RabbitMQ publisher connected", "response_queue", p.config.RMQResponse.Queue)
	return nil
}

// Receive is not supported on RabbitMQPublisher.
func (p *RabbitMQPublisher) Receive(_ context.Context) (*QueueMessage, error) {
	return nil, fmt.Errorf("RabbitMQPublisher does not support Receive")
}

// Publish sends a message to RabbitMQ.
// Uses rabbitmq.exchange and rabbitmq.routing_key from message headers,
// or falls back to config-level exchange/routing_key if set.
func (p *RabbitMQPublisher) Publish(ctx context.Context, msg *mqbridge.Message) error {
	if err := p.ensureConnected(); err != nil {
		return err
	}

	exchange := p.config.RMQResponse.Exchange
	routingKey := p.config.RMQResponse.RoutingKey

	// Message headers override config if present
	if v, ok := msg.Headers[mqbridge.HeaderRabbitMQExchange]; ok {
		exchange = v
	}
	if v, ok := msg.Headers[mqbridge.HeaderRabbitMQRoutingKey]; ok {
		routingKey = v
	}

	// Fall back to default exchange + queue name as routing key
	if routingKey == "" && p.config.RMQResponse.Queue != "" {
		routingKey = p.config.RMQResponse.Queue
	}

	if routingKey == "" {
		return fmt.Errorf("cannot determine response destination: no routing_key in message headers and no response.queue configured")
	}

	// Build AMQP headers from rabbitmq.header.* prefix
	headers := make(amqp.Table)
	for k, v := range msg.Headers {
		if strings.HasPrefix(k, mqbridge.HeaderRabbitMQHeaderPrefix) {
			headers[k[len(mqbridge.HeaderRabbitMQHeaderPrefix):]] = v
		}
	}

	pub := amqp.Publishing{
		Headers:      headers,
		Body:         msg.Body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}
	if v := msg.Headers[mqbridge.HeaderRabbitMQReplyTo]; v != "" {
		pub.ReplyTo = v
	}
	if v := msg.Headers[mqbridge.HeaderRabbitMQCorrelationID]; v != "" {
		pub.CorrelationId = v
	}
	if v := msg.Headers[mqbridge.HeaderRabbitMQContentType]; v != "" {
		pub.ContentType = v
	}
	if v := msg.Headers[mqbridge.HeaderRabbitMQMessageID]; v != "" {
		pub.MessageId = v
	}

	if err := p.withDeadline(func() error {
		return p.ch.PublishWithContext(ctx, exchange, routingKey, false, false, pub)
	}); err != nil {
		return fmt.Errorf("failed to publish to RabbitMQ exchange %q: %w", exchange, err)
	}
	return nil
}

// Ack is not supported on RabbitMQPublisher.
func (p *RabbitMQPublisher) Ack(_ context.Context, _ *QueueMessage) error {
	return fmt.Errorf("RabbitMQPublisher does not support Ack")
}

// Nack is not supported on RabbitMQPublisher.
func (p *RabbitMQPublisher) Nack(_ context.Context, _ *QueueMessage) error {
	return fmt.Errorf("RabbitMQPublisher does not support Nack")
}

// withDeadline sets a write deadline on the underlying TCP connection before fn
// and clears it after. This provides timeout for operations like Publish
// that don't support context.
func (p *RabbitMQPublisher) withDeadline(fn func() error) error {
	if p.netConn != nil {
		if err := p.netConn.SetWriteDeadline(time.Now().Add(p.timeout)); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer p.netConn.SetWriteDeadline(time.Time{}) //nolint:errcheck
	}
	return fn()
}

// Close closes the RabbitMQ publisher connection.
func (p *RabbitMQPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			errs = append(errs, err)
		}
		p.ch = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	p.netConn = nil
	if len(errs) > 0 {
		return fmt.Errorf("errors closing RabbitMQ publisher: %v", errs)
	}
	return nil
}

// dialWithCapture returns a dial function that captures the net.Conn for later
// deadline control. The captured conn can be used to set write deadlines on
// operations that don't natively support context timeouts.
func dialWithCapture(timeout time.Duration, dst *net.Conn) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		// Set deadline for AMQP handshake (cleared by the library after handshake)
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			conn.Close()
			return nil, err
		}
		*dst = conn
		return conn, nil
	}
}

// messageFromDelivery constructs a mqbridge.Message from an AMQP delivery.
func messageFromDelivery(d amqp.Delivery) *mqbridge.Message {
	headers := map[string]string{
		mqbridge.HeaderRabbitMQExchange:   d.Exchange,
		mqbridge.HeaderRabbitMQRoutingKey: d.RoutingKey,
	}
	if d.ReplyTo != "" {
		headers[mqbridge.HeaderRabbitMQReplyTo] = d.ReplyTo
	}
	if d.CorrelationId != "" {
		headers[mqbridge.HeaderRabbitMQCorrelationID] = d.CorrelationId
	}
	if d.ContentType != "" {
		headers[mqbridge.HeaderRabbitMQContentType] = d.ContentType
	}
	if d.MessageId != "" {
		headers[mqbridge.HeaderRabbitMQMessageID] = d.MessageId
	}
	for k, v := range d.Headers {
		headers[mqbridge.HeaderRabbitMQHeaderPrefix+k] = fmt.Sprintf("%v", v)
	}
	return &mqbridge.Message{
		Body:    d.Body,
		Headers: headers,
	}
}
