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
	if cfg.RequestQueue != "request-queue" {
		t.Errorf("request.queue: expected %q, got %q", "request-queue", cfg.RequestQueue)
	}
	if cfg.ResponseQueue != "response-queue" {
		t.Errorf("response.queue: expected %q, got %q", "response-queue", cfg.ResponseQueue)
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

func mustParseConfig(t *testing.T, jsonStr string) *Config {
	t.Helper()
	cfg, err := parseConfig([]byte(jsonStr))
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	return cfg
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
	}{
		{
			name: "valid simplemq without response queue",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
		},
		{
			name: "valid simplemq with response queue",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"response": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true}]
			}`,
		},
		{
			name: "missing request queue",
			json: `{
				"request": {"api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		{
			name: "missing api_key for simplemq",
			json: `{
				"request": {"queue": "q"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		{
			name: "response handler without response queue",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true}]
			}`,
			wantErr: true,
		},
		{
			name: "no handlers",
			json: `{
				"request": {"queue": "q", "api_key": "k"}
			}`,
			wantErr: true,
		},
		{
			name: "handler missing name",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		{
			name: "handler missing match",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		{
			name: "handler missing command",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}}]
			}`,
			wantErr: true,
		},
		{
			name: "response_ignore without response",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response_ignore": {"exit_code": 99}}]
			}`,
			wantErr: true,
		},
		{
			name: "response_ignore without exit_code",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"response": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true, "response_ignore": {}}]
			}`,
			wantErr: true,
		},
		{
			name: "valid response_ignore",
			json: `{
				"request": {"queue": "q", "api_key": "k"},
				"response": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true, "response_ignore": {"exit_code": 99}}]
			}`,
		},
		// Backend exclusivity
		{
			name: "simplemq and rabbitmq both configured",
			json: `{
				"simplemq": {},
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		{
			name: "simplemq empty object with rabbitmq",
			json: `{
				"simplemq": {"api_url": ""},
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		// RabbitMQ
		{
			name: "valid rabbitmq config",
			json: `{
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
		},
		{
			name: "valid rabbitmq reply_to",
			json: `{
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q"},
				"response": {"reply_to": true},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true}]
			}`,
		},
		{
			name: "rabbitmq reply_to with response queue",
			json: `{
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q"},
				"response": {"queue": "q", "reply_to": true},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true}]
			}`,
			wantErr: true,
		},
		{
			name: "rabbitmq response without queue or reply_to",
			json: `{
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"], "response": true}]
			}`,
			wantErr: true,
		},
		// Unknown fields rejected per backend
		{
			name: "simplemq request with rabbitmq field",
			json: `{
				"request": {"queue": "q", "api_key": "k", "exchange": "e"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
		{
			name: "rabbitmq request with simplemq field",
			json: `{
				"rabbitmq": {"url": "amqp://localhost"},
				"request": {"queue": "q", "api_key": "k"},
				"handlers": [{"name": "h", "match": {"k": "v"}, "command": ["echo"]}]
			}`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseConfig([]byte(tt.json))
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parseConfig/Validate error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			err = cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	cfg := mustParseConfig(t, `{
		"simplemq": {"api_url": "https://example.com"},
		"request": {"queue": "q", "api_key": "k"},
		"response": {"queue": "q", "api_key": "k"}
	}`)
	if cfg.SMQRequest.APIURL != "https://example.com" {
		t.Errorf("request api_url: expected %q, got %q", "https://example.com", cfg.SMQRequest.APIURL)
	}
	if cfg.SMQResponse.APIURL != "https://example.com" {
		t.Errorf("response api_url: expected %q, got %q", "https://example.com", cfg.SMQResponse.APIURL)
	}
}

func TestGetPollingInterval(t *testing.T) {
	r := &SMQRequestConfig{}
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
