package subscriber

import (
	"context"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	ctx := context.Background()
	cfg, err := LoadConfig(ctx, "testdata/config.jsonnet")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Request.Queue != "request-queue" {
		t.Errorf("request.queue: expected %q, got %q", "request-queue", cfg.Request.Queue)
	}
	if cfg.Response.Queue != "response-queue" {
		t.Errorf("response.queue: expected %q, got %q", "response-queue", cfg.Response.Queue)
	}
	if len(cfg.Handlers) != 2 {
		t.Fatalf("handlers: expected 2, got %d", len(cfg.Handlers))
	}
	if cfg.Handlers[0].Name != "echo" {
		t.Errorf("handlers[0].name: expected %q, got %q", "echo", cfg.Handlers[0].Name)
	}
	if !cfg.Handlers[0].Blocking {
		t.Error("handlers[0].blocking: expected true")
	}
	if cfg.Handlers[1].Blocking {
		t.Error("handlers[1].blocking: expected false")
	}
	if cfg.Handlers[1].MaxConcurrency != 3 {
		t.Errorf("handlers[1].max_concurrency: expected 3, got %d", cfg.Handlers[1].MaxConcurrency)
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid without response queue",
			config: Config{
				Request: RequestConfig{Queue: "q", APIKey: "k"},
				Handlers: []HandlerConfig{
					{Name: "h", Match: map[string]string{"k": "v"}, Command: []string{"echo"}},
				},
			},
		},
		{
			name: "valid with response queue and response handler",
			config: Config{
				Request:  RequestConfig{Queue: "q", APIKey: "k"},
				Response: ResponseConfig{Queue: "q", APIKey: "k"},
				Handlers: []HandlerConfig{
					{Name: "h", Match: map[string]string{"k": "v"}, Command: []string{"echo"}, Response: true},
				},
			},
		},
		{
			name: "missing request queue",
			config: Config{
				Request: RequestConfig{APIKey: "k"},
				Handlers: []HandlerConfig{
					{Name: "h", Match: map[string]string{"k": "v"}, Command: []string{"echo"}},
				},
			},
			wantErr: true,
		},
		{
			name: "response handler without response queue",
			config: Config{
				Request: RequestConfig{Queue: "q", APIKey: "k"},
				Handlers: []HandlerConfig{
					{Name: "h", Match: map[string]string{"k": "v"}, Command: []string{"echo"}, Response: true},
				},
			},
			wantErr: true,
		},
		{
			name: "no handlers",
			config: Config{
				Request: RequestConfig{Queue: "q", APIKey: "k"},
			},
			wantErr: true,
		},
		{
			name: "handler missing name",
			config: Config{
				Request: RequestConfig{Queue: "q", APIKey: "k"},
				Handlers: []HandlerConfig{
					{Match: map[string]string{"k": "v"}, Command: []string{"echo"}},
				},
			},
			wantErr: true,
		},
		{
			name: "handler missing match",
			config: Config{
				Request: RequestConfig{Queue: "q", APIKey: "k"},
				Handlers: []HandlerConfig{
					{Name: "h", Command: []string{"echo"}},
				},
			},
			wantErr: true,
		},
		{
			name: "handler missing command",
			config: Config{
				Request: RequestConfig{Queue: "q", APIKey: "k"},
				Handlers: []HandlerConfig{
					{Name: "h", Match: map[string]string{"k": "v"}},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	cfg := &Config{
		SimpleMQ: SimpleMQConfig{APIURL: "https://example.com"},
		Request:  RequestConfig{Queue: "q", APIKey: "k"},
		Response: ResponseConfig{Queue: "q", APIKey: "k"},
	}
	cfg.applyDefaults()
	if cfg.Request.APIURL != "https://example.com" {
		t.Errorf("request api_url: expected %q, got %q", "https://example.com", cfg.Request.APIURL)
	}
	if cfg.Response.APIURL != "https://example.com" {
		t.Errorf("response api_url: expected %q, got %q", "https://example.com", cfg.Response.APIURL)
	}
}

func TestGetPollingInterval(t *testing.T) {
	r := &RequestConfig{}
	if d := r.GetPollingInterval(); d.String() != "1s" {
		t.Errorf("default polling interval: expected 1s, got %s", d)
	}
	r.PollingInterval = "500ms"
	if d := r.GetPollingInterval(); d.String() != "500ms" {
		t.Errorf("custom polling interval: expected 500ms, got %s", d)
	}
}

func TestGetTimeout(t *testing.T) {
	h := &HandlerConfig{}
	if d := h.GetTimeout(); d.String() != "30s" {
		t.Errorf("default timeout: expected 30s, got %s", d)
	}
	h.Timeout = "10s"
	if d := h.GetTimeout(); d.String() != "10s" {
		t.Errorf("custom timeout: expected 10s, got %s", d)
	}
}

func TestGetMaxConcurrency(t *testing.T) {
	h := &HandlerConfig{}
	if c := h.GetMaxConcurrency(); c != 1 {
		t.Errorf("default max_concurrency: expected 1, got %d", c)
	}
	h.MaxConcurrency = 5
	if c := h.GetMaxConcurrency(); c != 5 {
		t.Errorf("custom max_concurrency: expected 5, got %d", c)
	}
}
