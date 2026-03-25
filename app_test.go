package subscriber

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fujiwara/mqbridge"
	"github.com/fujiwara/simplemq-cli/localserver"
	"github.com/sacloud/simplemq-api-go/apis/v1/message"
)

const testAPIKey = "test-api-key"

func TestMain(m *testing.M) {
	ctx := context.Background()
	shutdown, err := setupOTelProviders(ctx)
	if err != nil {
		panic(err)
	}
	code := m.Run()
	shutdown(ctx)
	os.Exit(code)
}

type testSecuritySource struct {
	token string
}

func (s *testSecuritySource) ApiKeyAuth(_ context.Context, _ message.OperationName) (message.ApiKeyAuth, error) {
	return message.ApiKeyAuth{Token: s.token}, nil
}

func newTestSMQClient(t *testing.T, serverURL string) *message.Client {
	t.Helper()
	client, err := message.NewClient(serverURL, &testSecuritySource{token: testAPIKey})
	if err != nil {
		t.Fatalf("failed to create SimpleMQ client: %v", err)
	}
	return client
}

func sendTestMessage(t *testing.T, ctx context.Context, client *message.Client, queue string, msg *mqbridge.Message) {
	t.Helper()
	data, err := mqbridge.MarshalMessage(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	_, err = client.SendMessage(ctx,
		&message.SendRequest{Content: message.MessageContent(encoded)},
		message.SendMessageParams{QueueName: message.QueueName(queue)},
	)
	if err != nil {
		t.Fatalf("failed to send to SimpleMQ: %v", err)
	}
}

func receiveTestMessage(t *testing.T, ctx context.Context, client *message.Client, queue string) *mqbridge.Message {
	t.Helper()
	for range 30 {
		time.Sleep(100 * time.Millisecond)
		res, err := client.ReceiveMessage(ctx, message.ReceiveMessageParams{
			QueueName: message.QueueName(queue),
		})
		if err != nil {
			continue
		}
		recvOK, ok := res.(*message.ReceiveMessageOK)
		if !ok || len(recvOK.Messages) == 0 {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(string(recvOK.Messages[0].Content))
		if err != nil {
			t.Fatalf("failed to decode message: %v", err)
		}
		return mqbridge.UnmarshalMessage(decoded)
	}
	return nil
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// newTestSMQConfig creates a SimpleMQ Config for testing.
func newTestSMQConfig(apiURL, reqQueue, resQueue string, handlers []HandlerConfig) *Config {
	cfg := &Config{
		SimpleMQ:      &SimpleMQConfig{APIURL: apiURL},
		RequestQueue:  reqQueue,
		ResponseQueue: resQueue,
		SMQRequest: &SMQRequestConfig{
			Queue:           reqQueue,
			APIKey:          testAPIKey,
			APIURL:          apiURL,
			PollingInterval: "100ms",
		},
		SMQResponse: &SMQResponseConfig{
			Queue:  resQueue,
			APIKey: testAPIKey,
			APIURL: apiURL,
		},
		Handlers: handlers,
	}
	return cfg
}

func TestBlockingHandler(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-blocking")
	resQueue := uniqueName("res-blocking")

	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "echo",
			Match:    map[string]string{"rabbitmq.routing_key": "echo"},
			Command:  []string{"cat"},
			Timeout:  "5s",
			Blocking: true,
			Response: true,
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	testBody := fmt.Sprintf("blocking-test-%d", time.Now().UnixNano())
	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte(testBody),
		Headers: map[string]string{
			"rabbitmq.routing_key":    "echo",
			"rabbitmq.exchange":       "test-exchange",
			"rabbitmq.reply_to":       "reply-queue",
			"rabbitmq.correlation_id": "corr-123",
		},
	})

	received := receiveTestMessage(t, ctx, client, resQueue)
	if received == nil {
		t.Fatal("expected response message, got nil")
	}
	if string(received.Body) != testBody {
		t.Errorf("body: expected %q, got %q", testBody, string(received.Body))
	}
	// RPC routing: exchange should be empty, routing_key should be reply_to value
	if received.Headers["rabbitmq.exchange"] != "" {
		t.Errorf("exchange: expected empty, got %q", received.Headers["rabbitmq.exchange"])
	}
	if received.Headers["rabbitmq.routing_key"] != "reply-queue" {
		t.Errorf("routing_key: expected %q, got %q", "reply-queue", received.Headers["rabbitmq.routing_key"])
	}
	if received.Headers["rabbitmq.correlation_id"] != "corr-123" {
		t.Errorf("correlation_id: expected %q, got %q", "corr-123", received.Headers["rabbitmq.correlation_id"])
	}
	if _, ok := received.Headers["rabbitmq.reply_to"]; ok {
		t.Error("reply_to should be removed from response")
	}
}

func TestNonBlockingHandler(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-nonblocking")
	resQueue := uniqueName("res-nonblocking")

	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:           "upper",
			Match:          map[string]string{"rabbitmq.routing_key": "upper"},
			Command:        []string{"tr", "a-z", "A-Z"},
			Timeout:        "5s",
			Blocking:       false,
			MaxConcurrency: 3,
			Response:       true,
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("hello"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "upper",
			"rabbitmq.reply_to":    "reply-queue",
		},
	})

	received := receiveTestMessage(t, ctx, client, resQueue)
	if received == nil {
		t.Fatal("expected response message, got nil")
	}
	if string(received.Body) != "HELLO" {
		t.Errorf("body: expected %q, got %q", "HELLO", string(received.Body))
	}
}

func TestNoMatchingHandler(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-nomatch")
	resQueue := uniqueName("res-nomatch")

	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "echo",
			Match:    map[string]string{"rabbitmq.routing_key": "echo"},
			Command:  []string{"cat"},
			Blocking: true,
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	// Send message that doesn't match any handler
	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("unmatched"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "unknown",
		},
	})

	// Wait a bit, then verify no response was sent
	time.Sleep(500 * time.Millisecond)
	received := receiveTestMessage(t, ctx, client, resQueue)
	if received != nil {
		t.Errorf("expected no response for unmatched message, got %q", string(received.Body))
	}

	// Verify the request message was deleted (dropped)
	res, err := client.ReceiveMessage(ctx, message.ReceiveMessageParams{
		QueueName: message.QueueName(reqQueue),
	})
	if err != nil {
		t.Fatalf("failed to check request queue: %v", err)
	}
	recvOK, ok := res.(*message.ReceiveMessageOK)
	if ok && len(recvOK.Messages) > 0 {
		t.Error("expected request queue to be empty after drop")
	}
}

func TestMultipleHandlers(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-multi")
	resQueue := uniqueName("res-multi")

	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "echo",
			Match:    map[string]string{"rabbitmq.routing_key": "echo"},
			Command:  []string{"cat"},
			Blocking: true,
			Response: true,
		},
		{
			Name:           "upper",
			Match:          map[string]string{"rabbitmq.routing_key": "upper"},
			Command:        []string{"tr", "a-z", "A-Z"},
			Blocking:       false,
			MaxConcurrency: 2,
			Response:       true,
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	// Send echo message
	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("hello"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "echo",
			"rabbitmq.reply_to":    "reply-queue",
		},
	})

	received := receiveTestMessage(t, ctx, client, resQueue)
	if received == nil {
		t.Fatal("expected echo response, got nil")
	}
	if string(received.Body) != "hello" {
		t.Errorf("echo body: expected %q, got %q", "hello", string(received.Body))
	}

	// Send upper message
	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("world"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "upper",
			"rabbitmq.reply_to":    "reply-queue",
		},
	})

	received = receiveTestMessage(t, ctx, client, resQueue)
	if received == nil {
		t.Fatal("expected upper response, got nil")
	}
	if string(received.Body) != "WORLD" {
		t.Errorf("upper body: expected %q, got %q", "WORLD", string(received.Body))
	}
}

func TestCommandFailureResponseTrue(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-fail-res")
	resQueue := uniqueName("res-fail-res")

	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "fail",
			Match:    map[string]string{"rabbitmq.routing_key": "fail"},
			Command:  []string{"false"},
			Blocking: true,
			Response: true,
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("fail"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "fail",
		},
	})

	// response:true handler should send error response
	received := receiveTestMessage(t, ctx, client, resQueue)
	if received == nil {
		t.Fatal("expected error response, got nil")
	}
	if received.Headers["x-status"] != "error" {
		t.Errorf("x-status: expected %q, got %q", "error", received.Headers["x-status"])
	}
	if received.Headers["x-exit-code"] != "1" {
		t.Errorf("x-exit-code: expected %q, got %q", "1", received.Headers["x-exit-code"])
	}
}

func TestCommandFailureResponseFalse(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-fail-nores")
	resQueue := uniqueName("res-fail-nores")

	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "fail",
			Match:    map[string]string{"rabbitmq.routing_key": "fail"},
			Command:  []string{"false"},
			Blocking: true,
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("fail"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "fail",
		},
	})

	// response:false handler should NOT send response on failure
	time.Sleep(500 * time.Millisecond)
	received := receiveTestMessage(t, ctx, client, resQueue)
	if received != nil {
		t.Errorf("expected no response for failed command with response:false, got %q", string(received.Body))
	}
}

func TestResponseIgnoreExitCode(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-ignore")
	resQueue := uniqueName("res-ignore")

	ignoreExitCode := 99
	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "ignore-test",
			Match:    map[string]string{"rabbitmq.routing_key": "ignore"},
			Command:  []string{"sh", "-c", "exit 99"},
			Blocking: true,
			Response: true,
			ResponseIgnore: &ResponseIgnoreConfig{
				ExitCode: &ignoreExitCode,
			},
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("ignore-me"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "ignore",
		},
	})

	// response should be suppressed because exit code matches response_ignore
	time.Sleep(500 * time.Millisecond)
	received := receiveTestMessage(t, ctx, client, resQueue)
	if received != nil {
		t.Errorf("expected no response for ignored exit code, got %q", string(received.Body))
	}

	// request message should be deleted
	res, err := client.ReceiveMessage(ctx, message.ReceiveMessageParams{
		QueueName: message.QueueName(reqQueue),
	})
	if err != nil {
		t.Fatalf("failed to check request queue: %v", err)
	}
	recvOK, ok := res.(*message.ReceiveMessageOK)
	if ok && len(recvOK.Messages) > 0 {
		t.Error("expected request queue to be empty after response_ignore")
	}
}

func TestResponseIgnoreExitCodeNonMatch(t *testing.T) {
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	reqQueue := uniqueName("req-ignore-nomatch")
	resQueue := uniqueName("res-ignore-nomatch")

	ignoreExitCode := 99
	cfg := newTestSMQConfig(srv.TestURL(), reqQueue, resQueue, []HandlerConfig{
		{
			Name:     "ignore-nomatch",
			Match:    map[string]string{"rabbitmq.routing_key": "fail"},
			Command:  []string{"sh", "-c", "exit 98"}, // exits with 98, not 99
			Blocking: true,
			Response: true,
			ResponseIgnore: &ResponseIgnoreConfig{
				ExitCode: &ignoreExitCode,
			},
		},
	})

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go app.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	client := newTestSMQClient(t, srv.TestURL())

	sendTestMessage(t, ctx, client, reqQueue, &mqbridge.Message{
		Body: []byte("fail"),
		Headers: map[string]string{
			"rabbitmq.routing_key": "fail",
		},
	})

	// exit code 1 != 99, so error response should still be sent
	received := receiveTestMessage(t, ctx, client, resQueue)
	if received == nil {
		t.Fatal("expected error response for non-matching exit code, got nil")
	}
	if received.Headers["x-status"] != "error" {
		t.Errorf("x-status: expected %q, got %q", "error", received.Headers["x-status"])
	}
}
