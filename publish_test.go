package subscriber

import (
	"os"
	"path/filepath"
	"testing"

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

	msg := buildPublishMessage(
		[]string{"x-type=test", "x-action=deploy"},
		[]byte(`{"key":"value"}`),
	)
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
		Header:   []string{"x-source=file"},
		BodyFile: bodyFile,
	}
	body, err := cmd.readBody()
	if err != nil {
		t.Fatal(err)
	}
	headers, err := parseHeaders(cmd.Header)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := newRequestPublisher(cfg)
	if err != nil {
		t.Fatalf("newRequestPublisher failed: %v", err)
	}
	defer pub.Close()

	msg := buildPublishMessage(nil, body)
	msg.Headers = headers
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

func TestParseHeaders(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    map[string]string
		wantErr bool
	}{
		{
			name:  "valid headers",
			input: []string{"key1=value1", "key2=value2"},
			want:  map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			name:  "value with equals sign",
			input: []string{"key=val=ue"},
			want:  map[string]string{"key": "val=ue"},
		},
		{
			name:  "empty value",
			input: []string{"key="},
			want:  map[string]string{"key": ""},
		},
		{
			name:    "invalid format",
			input:   []string{"no-equals"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHeaders(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseHeaders() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				for k, v := range tt.want {
					if got[k] != v {
						t.Errorf("parseHeaders()[%q] = %q, want %q", k, got[k], v)
					}
				}
			}
		})
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

func buildPublishMessage(headers []string, body []byte) *mqbridge.Message {
	h, _ := parseHeaders(headers)
	return &mqbridge.Message{
		Body:    body,
		Headers: h,
	}
}
