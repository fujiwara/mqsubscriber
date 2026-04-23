package subscriber

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fujiwara/mqbridge"
	"github.com/fujiwara/simplemq-cli/localserver"
)

func TestPublishCmd(t *testing.T) {
	ctx := t.Context()
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	requestQueue := uniqueName("pub-basic")
	cfg := &Config{
		SimpleMQ:     &SimpleMQConfig{APIURL: srv.TestURL()},
		RequestQueue: requestQueue,
		SMQRequest: &SMQRequestConfig{
			Queue:  requestQueue,
			APIKey: testAPIKey,
			APIURL: srv.TestURL(),
		},
	}

	pub, err := newRequestPublisher(cfg)
	if err != nil {
		t.Fatalf("newRequestPublisher failed: %v", err)
	}
	defer pub.Close()

	msg := &mqbridge.Message{
		Headers: map[string]string{"x-type": "test", "x-action": "deploy"},
		Body:    []byte(`{"key":"value"}`),
	}
	if err := pub.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive and verify
	client := newTestSMQClient(t, srv.TestURL())
	got := receiveTestMessage(t, ctx, client, requestQueue)
	if got == nil {
		t.Fatal("expected a message but got nil")
	}
	if v := got.Headers["x-type"]; v != "test" {
		t.Errorf("header x-type = %q, want %q", v, "test")
	}
	if v := got.Headers["x-action"]; v != "deploy" {
		t.Errorf("header x-action = %q, want %q", v, "deploy")
	}
	if v := string(got.Body); v != `{"key":"value"}` {
		t.Errorf("body = %q, want %q", v, `{"key":"value"}`)
	}
}

func TestPublishCmdBodyFile(t *testing.T) {
	ctx := t.Context()
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	requestQueue := uniqueName("pub-bodyfile")
	cfg := &Config{
		SimpleMQ:     &SimpleMQConfig{APIURL: srv.TestURL()},
		RequestQueue: requestQueue,
		SMQRequest: &SMQRequestConfig{
			Queue:  requestQueue,
			APIKey: testAPIKey,
			APIURL: srv.TestURL(),
		},
	}

	// Write body to a temp file
	bodyFile := filepath.Join(t.TempDir(), "body.json")
	if err := os.WriteFile(bodyFile, []byte(`{"from":"file"}`), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := &PublishCmd{
		Header:   map[string]string{"x-source": "file"},
		BodyFile: bodyFile,
	}
	body, err := cmd.readBody()
	if err != nil {
		t.Fatal(err)
	}

	pub, err := newRequestPublisher(cfg)
	if err != nil {
		t.Fatalf("newRequestPublisher failed: %v", err)
	}
	defer pub.Close()

	msg := &mqbridge.Message{
		Headers: cmd.Header,
		Body:    body,
	}
	if err := pub.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	client := newTestSMQClient(t, srv.TestURL())
	got := receiveTestMessage(t, ctx, client, requestQueue)
	if got == nil {
		t.Fatal("expected a message but got nil")
	}
	if v := string(got.Body); v != `{"from":"file"}` {
		t.Errorf("body = %q, want %q", v, `{"from":"file"}`)
	}
}

func TestPublishCmdResponse(t *testing.T) {
	ctx := t.Context()
	srv := localserver.NewTestServer(localserver.Config{APIKey: testAPIKey})
	defer srv.Close()

	requestQueue := uniqueName("pub-req")
	responseQueue := uniqueName("pub-res")
	cfg := &Config{
		SimpleMQ:      &SimpleMQConfig{APIURL: srv.TestURL()},
		RequestQueue:  requestQueue,
		ResponseQueue: responseQueue,
		SMQRequest: &SMQRequestConfig{
			Queue:  requestQueue,
			APIKey: testAPIKey,
			APIURL: srv.TestURL(),
		},
		SMQResponse: &SMQResponseConfig{
			Queue:  responseQueue,
			APIKey: testAPIKey,
			APIURL: srv.TestURL(),
		},
	}

	// Publish to response queue
	pub, err := newResponsePublisher(cfg)
	if err != nil {
		t.Fatalf("newResponsePublisher failed: %v", err)
	}
	defer pub.Close()

	msg := &mqbridge.Message{
		Headers: map[string]string{"x-type": "response"},
		Body:    []byte(`{"result":"ok"}`),
	}
	if err := pub.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify message arrived in response queue, not request queue
	client := newTestSMQClient(t, srv.TestURL())
	got := receiveTestMessage(t, ctx, client, responseQueue)
	if got == nil {
		t.Fatal("expected a message in response queue but got nil")
	}
	if v := got.Headers["x-type"]; v != "response" {
		t.Errorf("header x-type = %q, want %q", v, "response")
	}
	if v := string(got.Body); v != `{"result":"ok"}` {
		t.Errorf("body = %q, want %q", v, `{"result":"ok"}`)
	}
}

func TestNewResponsePublisherNoConfig(t *testing.T) {
	cfg := &Config{
		SimpleMQ: &SimpleMQConfig{},
		SMQRequest: &SMQRequestConfig{
			Queue:  "req",
			APIKey: "key",
		},
	}
	_, err := newResponsePublisher(cfg)
	if err == nil {
		t.Error("expected error when response queue is not configured")
	}
}

// flakyPublisher is a QueueClient that fails the first failUntil Publish calls
// then succeeds. Only Publish is implemented; other methods panic.
type flakyPublisher struct {
	failUntil int
	calls     int
	err       error
}

func (p *flakyPublisher) Publish(_ context.Context, _ *mqbridge.Message) error {
	p.calls++
	if p.calls <= p.failUntil {
		return p.err
	}
	return nil
}

func (p *flakyPublisher) Receive(context.Context) (*QueueMessage, error) { panic("unused") }
func (p *flakyPublisher) Ack(context.Context, *QueueMessage) error       { panic("unused") }
func (p *flakyPublisher) Nack(context.Context, *QueueMessage) error      { panic("unused") }
func (p *flakyPublisher) Close() error                                   { return nil }

// withShortBackoff swaps publishRetryBaseInterval for the duration of a test.
func withShortBackoff(t *testing.T) {
	t.Helper()
	orig := publishRetryBaseInterval
	publishRetryBaseInterval = time.Millisecond
	t.Cleanup(func() { publishRetryBaseInterval = orig })
}

func TestPublishWithRetrySucceedsAfterTransient(t *testing.T) {
	withShortBackoff(t)

	pub := &flakyPublisher{failUntil: 2, err: errors.New("transient")}
	err := publishWithRetry(t.Context(), pub, &mqbridge.Message{})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if pub.calls != 3 {
		t.Errorf("expected 3 Publish calls (2 fail + 1 success), got %d", pub.calls)
	}
}

func TestPublishWithRetryExhausted(t *testing.T) {
	withShortBackoff(t)

	wantErr := errors.New("persistent")
	pub := &flakyPublisher{failUntil: 100, err: wantErr}
	err := publishWithRetry(t.Context(), pub, &mqbridge.Message{})
	if !errors.Is(err, wantErr) {
		t.Errorf("expected last error %v, got %v", wantErr, err)
	}
	if pub.calls != publishRetryCount {
		t.Errorf("expected %d Publish calls, got %d", publishRetryCount, pub.calls)
	}
}

func TestPublishWithRetryContextCanceled(t *testing.T) {
	// Use the production backoff (1s) so the first retry will definitely
	// wait long enough for us to cancel.
	pub := &flakyPublisher{failUntil: 100, err: errors.New("boom")}
	ctx, cancel := context.WithCancel(t.Context())
	// Cancel shortly after the first failure, while the retry is sleeping.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := publishWithRetry(ctx, pub, &mqbridge.Message{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if pub.calls != 1 {
		t.Errorf("expected 1 Publish call before cancellation, got %d", pub.calls)
	}
}

func TestReadBodyMutuallyExclusive(t *testing.T) {
	cmd := &PublishCmd{
		Body:     "hello",
		BodyFile: "/tmp/somefile",
	}
	if _, err := cmd.readBody(); err == nil {
		t.Error("expected error for mutually exclusive --body and --body-file")
	}
}
