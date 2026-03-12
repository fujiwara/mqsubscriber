package subscriber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	armed "github.com/fujiwara/jsonnet-armed"
)

const (
	// DefaultPollingInterval is the default interval for polling the request queue.
	DefaultPollingInterval = time.Second
	// DefaultCommandTimeout is the default timeout for command execution.
	DefaultCommandTimeout = 30 * time.Second
	// DefaultMaxConcurrency is the default max concurrency for non-blocking handlers.
	DefaultMaxConcurrency = 1
)

// Config is the top-level configuration.
type Config struct {
	SimpleMQ SimpleMQConfig  `json:"simplemq"`
	Request  RequestConfig   `json:"request"`
	Response ResponseConfig  `json:"response"`
	Handlers []HandlerConfig `json:"handlers"`
}

// SimpleMQConfig holds the global SimpleMQ settings.
type SimpleMQConfig struct {
	APIURL string `json:"api_url"`
}

// RequestConfig defines the request (inbound) queue.
type RequestConfig struct {
	SimpleMQConfig         // embedded: api_url (overrides global)
	Queue           string `json:"queue"`
	APIKey          string `json:"api_key"`
	PollingInterval string `json:"polling_interval"`
}

// GetPollingInterval returns the polling interval as a time.Duration.
func (c *RequestConfig) GetPollingInterval() time.Duration {
	if c.PollingInterval == "" {
		return DefaultPollingInterval
	}
	d, err := time.ParseDuration(c.PollingInterval)
	if err != nil {
		return DefaultPollingInterval
	}
	return d
}

// ResponseConfig defines the response (outbound) queue.
type ResponseConfig struct {
	SimpleMQConfig        // embedded: api_url (overrides global)
	Queue          string `json:"queue"`
	APIKey         string `json:"api_key"`
}

// HandlerConfig defines a handler that matches messages and executes a command.
type HandlerConfig struct {
	Name           string            `json:"name"`
	Match          map[string]string `json:"match"`
	Command        []string          `json:"command"`
	Timeout        string            `json:"timeout"`
	Blocking       bool              `json:"blocking"`
	MaxConcurrency int               `json:"max_concurrency"`
	Response       *bool             `json:"response"`
}

// GetTimeout returns the command timeout as a time.Duration.
func (c *HandlerConfig) GetTimeout() time.Duration {
	if c.Timeout == "" {
		return DefaultCommandTimeout
	}
	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return DefaultCommandTimeout
	}
	return d
}

// GetResponse returns whether the handler sends a response. Default is true.
func (c *HandlerConfig) GetResponse() bool {
	if c.Response == nil {
		return true
	}
	return *c.Response
}

// GetMaxConcurrency returns the max concurrency for non-blocking handlers.
func (c *HandlerConfig) GetMaxConcurrency() int {
	if c.MaxConcurrency <= 0 {
		return DefaultMaxConcurrency
	}
	return c.MaxConcurrency
}

// LoadConfig loads and parses a configuration file (Jsonnet or JSON).
func LoadConfig(ctx context.Context, path string) (*Config, error) {
	jsonBytes, err := evaluateJsonnet(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate config: %w", err)
	}
	return parseConfig(jsonBytes)
}

// RenderConfig evaluates a Jsonnet config file and returns the resulting JSON.
func RenderConfig(ctx context.Context, path string) ([]byte, error) {
	return evaluateJsonnet(ctx, path)
}

func evaluateJsonnet(ctx context.Context, path string) ([]byte, error) {
	var buf bytes.Buffer
	cli := &armed.CLI{Filename: path}
	cli.SetWriter(&buf)
	cli.AddFunctions(secretNativeFunction(ctx))
	if err := cli.Run(ctx); err != nil {
		return nil, fmt.Errorf("failed to evaluate jsonnet %q: %w", path, err)
	}
	return buf.Bytes(), nil
}

func parseConfig(data []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	return &cfg, nil
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	c.applyDefaults()

	if c.Request.Queue == "" {
		return fmt.Errorf("request.queue is required")
	}
	if c.Request.APIKey == "" {
		return fmt.Errorf("request.api_key is required")
	}
	if c.Response.Queue == "" {
		return fmt.Errorf("response.queue is required")
	}
	if c.Response.APIKey == "" {
		return fmt.Errorf("response.api_key is required")
	}
	if len(c.Handlers) == 0 {
		return fmt.Errorf("at least one handler is required")
	}
	for i, h := range c.Handlers {
		if err := h.validate(i); err != nil {
			return err
		}
	}
	return nil
}

func (h *HandlerConfig) validate(index int) error {
	if h.Name == "" {
		return fmt.Errorf("handlers[%d].name is required", index)
	}
	if len(h.Match) == 0 {
		return fmt.Errorf("handlers[%d].match must have at least one entry", index)
	}
	if len(h.Command) == 0 {
		return fmt.Errorf("handlers[%d].command is required", index)
	}
	if h.Timeout != "" {
		if _, err := time.ParseDuration(h.Timeout); err != nil {
			return fmt.Errorf("handlers[%d].timeout is invalid: %w", index, err)
		}
	}
	if !h.Blocking && h.MaxConcurrency < 0 {
		return fmt.Errorf("handlers[%d].max_concurrency must be positive", index)
	}
	return nil
}

// applyDefaults copies global config into per-queue configs where not already set.
func (c *Config) applyDefaults() {
	if c.Request.APIURL == "" {
		c.Request.SimpleMQConfig = c.SimpleMQ
	}
	if c.Response.APIURL == "" {
		c.Response.SimpleMQConfig = c.SimpleMQ
	}
}
