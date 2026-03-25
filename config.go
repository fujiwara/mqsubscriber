package subscriber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	armed "github.com/fujiwara/jsonnet-armed"
	sscli "github.com/fujiwara/sakura-secrets-cli"
)

const (
	// DefaultPollingInterval is the default interval for polling the request queue.
	DefaultPollingInterval = time.Second
	// DefaultCommandTimeout is the default timeout for command execution.
	DefaultCommandTimeout = 30 * time.Second
	// DefaultMaxConcurrency is the default max concurrency for non-blocking handlers.
	DefaultMaxConcurrency = 1
)

// Backend type constants.
const (
	BackendSimpleMQ = "simplemq"
	BackendRabbitMQ = "rabbitmq"
)

// Config is the top-level configuration.
type Config struct {
	SimpleMQ *SimpleMQConfig `json:"simplemq"`
	RabbitMQ *RabbitMQConfig `json:"rabbitmq"`
	Request  RequestConfig   `json:"request"`
	Response ResponseConfig  `json:"response"`
	Handlers []HandlerConfig `json:"handlers"`
}

// SimpleMQConfig holds the global SimpleMQ settings.
type SimpleMQConfig struct {
	APIURL string `json:"api_url"`
}

// RabbitMQConfig holds the global RabbitMQ settings.
type RabbitMQConfig struct {
	URL string `json:"url"`
}

// RequestConfig defines the request (inbound) queue.
type RequestConfig struct {
	SimpleMQConfig         // embedded: api_url (overrides global SimpleMQ)
	Queue           string `json:"queue"`
	APIKey          string `json:"api_key"`          // SimpleMQ only
	PollingInterval string `json:"polling_interval"` // SimpleMQ only

	// RabbitMQ only
	Exchange        string   `json:"exchange"`
	ExchangeType    string   `json:"exchange_type"`
	RoutingKeys     []string `json:"routing_keys"`
	ExchangePassive bool     `json:"exchange_passive"`
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
	SimpleMQConfig        // embedded: api_url (overrides global SimpleMQ)
	Queue          string `json:"queue"`
	APIKey         string `json:"api_key"` // SimpleMQ only

	// RabbitMQ only: fixed destination (optional; if empty, uses message headers)
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

// ResponseIgnoreConfig defines conditions under which a response is suppressed.
type ResponseIgnoreConfig struct {
	ExitCode *int `json:"exit_code"`
}

// HandlerConfig defines a handler that matches messages and executes a command.
type HandlerConfig struct {
	Name           string                `json:"name"`
	Match          map[string]string     `json:"match"`
	Command        []string              `json:"command"`
	Timeout        string                `json:"timeout"`
	Blocking       bool                  `json:"blocking"`
	MaxConcurrency int                   `json:"max_concurrency"`
	Response       bool                  `json:"response"`
	ResponseIgnore *ResponseIgnoreConfig `json:"response_ignore"`
	LogMessage     string                `json:"log_message"`
	LogBodyFields  []string              `json:"log_body_fields"`
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
	cli.AddFunctions(sscli.SecretNativeFunction(ctx))
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

// BackendType returns the MQ backend type based on configuration.
func (c *Config) BackendType() string {
	if c.RabbitMQ != nil {
		return BackendRabbitMQ
	}
	return BackendSimpleMQ
}

// needsResponseQueue returns true if any handler has response enabled.
func (c *Config) needsResponseQueue() bool {
	for _, h := range c.Handlers {
		if h.Response {
			return true
		}
	}
	return false
}

// hasResponseQueue returns true if the response queue is configured.
func (c *Config) hasResponseQueue() bool {
	switch c.BackendType() {
	case BackendRabbitMQ:
		return c.Response.Queue != ""
	default:
		return c.Response.Queue != "" && c.Response.APIKey != ""
	}
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	c.applyDefaults()

	// Backend exclusivity check
	if c.SimpleMQ != nil && c.RabbitMQ != nil {
		return fmt.Errorf("simplemq and rabbitmq cannot be configured simultaneously")
	}

	if c.Request.Queue == "" {
		return fmt.Errorf("request.queue is required")
	}

	switch c.BackendType() {
	case BackendRabbitMQ:
		if err := c.validateRabbitMQ(); err != nil {
			return err
		}
	default:
		if err := c.validateSimpleMQ(); err != nil {
			return err
		}
	}

	if len(c.Handlers) == 0 {
		return fmt.Errorf("at least one handler is required")
	}
	for i, h := range c.Handlers {
		if err := h.validate(i); err != nil {
			return err
		}
	}

	needsResponse := c.needsResponseQueue()
	hasResponse := c.hasResponseQueue()

	if needsResponse && !hasResponse {
		switch c.BackendType() {
		case BackendRabbitMQ:
			return fmt.Errorf("response.queue is required when any handler has response enabled")
		default:
			return fmt.Errorf("response.queue and response.api_key are required when any handler has response enabled")
		}
	}
	if !needsResponse && hasResponse {
		slog.Warn("response queue is configured but no handler has response enabled")
	}

	return nil
}

func (c *Config) validateSimpleMQ() error {
	if c.Request.APIKey == "" {
		return fmt.Errorf("request.api_key is required")
	}
	return nil
}

func (c *Config) validateRabbitMQ() error {
	if c.RabbitMQ == nil || c.RabbitMQ.URL == "" {
		return fmt.Errorf("rabbitmq.url is required")
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
	if h.ResponseIgnore != nil && !h.Response {
		return fmt.Errorf("handlers[%d].response_ignore requires response to be true", index)
	}
	if h.ResponseIgnore != nil && h.ResponseIgnore.ExitCode == nil {
		return fmt.Errorf("handlers[%d].response_ignore.exit_code is required", index)
	}
	return nil
}

// applyDefaults copies global config into per-queue configs where not already set.
func (c *Config) applyDefaults() {
	if c.SimpleMQ != nil {
		if c.Request.APIURL == "" {
			c.Request.SimpleMQConfig = *c.SimpleMQ
		}
		if c.Response.APIURL == "" {
			c.Response.SimpleMQConfig = *c.SimpleMQ
		}
	}
}
