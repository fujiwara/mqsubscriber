//go:build integration

package subscriber

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fujiwara/mqbridge"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func setupRabbitMQ(t *testing.T, ctx context.Context) string {
	t.Helper()
	container, err := rabbitmq.Run(ctx, "rabbitmq:3-management")
	if err != nil {
		t.Fatalf("failed to start RabbitMQ container: %v", err)
	}
	t.Cleanup(func() {
		container.Terminate(ctx)
	})
	url, err := container.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to get AMQP URL: %v", err)
	}
	return url
}

func publishToRabbitMQ(t *testing.T, url, exchange, routingKey string, body []byte, headers map[string]string) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	amqpHeaders := make(amqp.Table)
	for k, v := range headers {
		amqpHeaders[k] = v
	}
	err = ch.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
		Headers:      amqpHeaders,
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
}

func consumeFromRabbitMQ(t *testing.T, url, queue string, timeout time.Duration) *mqbridge.Message {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	// Declare queue in case it doesn't exist yet
	ch.QueueDeclare(queue, true, false, false, false, nil)

	msgs, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	select {
	case d := <-msgs:
		return messageFromDelivery(d)
	case <-time.After(timeout):
		return nil
	}
}

func TestRabbitMQBlockingHandler(t *testing.T) {
	ctx := t.Context()
	url := setupRabbitMQ(t, ctx)

	reqExchange := uniqueName("req-exchange")
	reqQueue := uniqueName("req-rmq")
	resQueue := uniqueName("res-rmq")

	cfg := &Config{
		RabbitMQ: RabbitMQConfig{URL: url},
		Request: RequestConfig{
			Queue:        reqQueue,
			Exchange:     reqExchange,
			ExchangeType: "direct",
			RoutingKeys:  []string{"echo"},
		},
		Response: ResponseConfig{
			Queue: resQueue,
		},
		Handlers: []HandlerConfig{
			{
				Name:     "echo",
				Match:    map[string]string{"rabbitmq.routing_key": "echo"},
				Command:  []string{"cat"},
				Timeout:  "5s",
				Blocking: true,
				Response: true,
			},
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	appCtx, appCancel := context.WithCancel(ctx)
	defer appCancel()

	go app.Run(appCtx)
	time.Sleep(500 * time.Millisecond) // wait for connection

	testBody := fmt.Sprintf("rabbitmq-test-%d", time.Now().UnixNano())
	publishToRabbitMQ(t, url, reqExchange, "echo", []byte(testBody), nil)

	received := consumeFromRabbitMQ(t, url, resQueue, 10*time.Second)
	appCancel()

	if received == nil {
		t.Fatal("expected response message, got nil")
	}
	if string(received.Body) != testBody {
		t.Errorf("body: expected %q, got %q", testBody, string(received.Body))
	}
}

func TestRabbitMQNonBlockingHandler(t *testing.T) {
	ctx := t.Context()
	url := setupRabbitMQ(t, ctx)

	reqExchange := uniqueName("req-exchange")
	reqQueue := uniqueName("req-rmq-nb")
	resQueue := uniqueName("res-rmq-nb")

	cfg := &Config{
		RabbitMQ: RabbitMQConfig{URL: url},
		Request: RequestConfig{
			Queue:        reqQueue,
			Exchange:     reqExchange,
			ExchangeType: "direct",
			RoutingKeys:  []string{"upper"},
		},
		Response: ResponseConfig{
			Queue: resQueue,
		},
		Handlers: []HandlerConfig{
			{
				Name:           "upper",
				Match:          map[string]string{"rabbitmq.routing_key": "upper"},
				Command:        []string{"tr", "a-z", "A-Z"},
				Timeout:        "5s",
				Blocking:       false,
				MaxConcurrency: 3,
				Response:       true,
			},
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	appCtx, appCancel := context.WithCancel(ctx)
	defer appCancel()

	go app.Run(appCtx)
	time.Sleep(500 * time.Millisecond)

	publishToRabbitMQ(t, url, reqExchange, "upper", []byte("hello"), nil)

	received := consumeFromRabbitMQ(t, url, resQueue, 10*time.Second)
	appCancel()

	if received == nil {
		t.Fatal("expected response message, got nil")
	}
	if string(received.Body) != "HELLO" {
		t.Errorf("body: expected %q, got %q", "HELLO", string(received.Body))
	}
}

func TestRabbitMQCommandFailure(t *testing.T) {
	ctx := t.Context()
	url := setupRabbitMQ(t, ctx)

	reqExchange := uniqueName("req-exchange")
	reqQueue := uniqueName("req-rmq-fail")
	resQueue := uniqueName("res-rmq-fail")

	cfg := &Config{
		RabbitMQ: RabbitMQConfig{URL: url},
		Request: RequestConfig{
			Queue:        reqQueue,
			Exchange:     reqExchange,
			ExchangeType: "direct",
			RoutingKeys:  []string{"fail"},
		},
		Response: ResponseConfig{
			Queue: resQueue,
		},
		Handlers: []HandlerConfig{
			{
				Name:     "fail",
				Match:    map[string]string{"rabbitmq.routing_key": "fail"},
				Command:  []string{"false"},
				Timeout:  "5s",
				Blocking: true,
				Response: true,
			},
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	appCtx, appCancel := context.WithCancel(ctx)
	defer appCancel()

	go app.Run(appCtx)
	time.Sleep(500 * time.Millisecond)

	publishToRabbitMQ(t, url, reqExchange, "fail", []byte("fail"), nil)

	received := consumeFromRabbitMQ(t, url, resQueue, 10*time.Second)
	appCancel()

	if received == nil {
		t.Fatal("expected error response, got nil")
	}
	// Check x-status header (direct, since no reply_to)
	if received.Headers["rabbitmq.header.x-status"] != "error" {
		t.Errorf("x-status: expected %q, got headers %v", "error", received.Headers)
	}
}
