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

// SimpleMQConfig holds the global SimpleMQ settings.
type SimpleMQConfig struct {
	APIURL string `json:"api_url"`
}

// RabbitMQConfig holds the global RabbitMQ settings.
type RabbitMQConfig struct {
	URL string `json:"url"`
}

// SMQRequestConfig defines the SimpleMQ request (inbound) queue.
type SMQRequestConfig struct {
	Queue           string `json:"queue"`
	APIKey          string `json:"api_key"`
	APIURL          string `json:"api_url"`
	PollingInterval string `json:"polling_interval"`
}

// GetPollingInterval returns the polling interval as a time.Duration.
func (c *SMQRequestConfig) GetPollingInterval() time.Duration {
	if c.PollingInterval == "" {
		return DefaultPollingInterval
	}
	d, err := time.ParseDuration(c.PollingInterval)
	if err != nil {
		return DefaultPollingInterval
	}
	return d
}

// SMQResponseConfig defines the SimpleMQ response (outbound) queue.
type SMQResponseConfig struct {
	Queue  string `json:"queue"`
	APIKey string `json:"api_key"`
	APIURL string `json:"api_url"`
}

// RoutingKeys is a []string that accepts both a single string and an array of strings in JSON.
type RoutingKeys []string

// UnmarshalJSON implements custom unmarshalling to accept both a string and an array of strings.
func (r *RoutingKeys) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*r = RoutingKeys{s}
		return nil
	}
	var a []string
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("routing_key must be a string or an array of strings")
	}
	*r = RoutingKeys(a)
	return nil
}

// RMQRequestConfig defines the RabbitMQ request (inbound) queue.
type RMQRequestConfig struct {
	Queue           string      `json:"queue"`
	Exchange        string      `json:"exchange"`
	ExchangeType    string      `json:"exchange_type"`
	RoutingKey      RoutingKeys `json:"routing_key"`
	ExchangePassive bool        `json:"exchange_passive"`
}

// RMQResponseConfig defines the RabbitMQ response (outbound) queue.
type RMQResponseConfig struct {
	Queue      string `json:"queue"`
	ReplyTo    bool   `json:"reply_to"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

// Config is the resolved top-level configuration.
// Backend-specific fields are in SMQ*/RMQ* structs (exactly one pair is non-nil).
type Config struct {
	SimpleMQ *SimpleMQConfig
	RabbitMQ *RabbitMQConfig

	RequestQueue  string
	ResponseQueue string

	SMQRequest  *SMQRequestConfig
	SMQResponse *SMQResponseConfig
	RMQRequest  *RMQRequestConfig
	RMQResponse *RMQResponseConfig

	Handlers []HandlerConfig
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
	Env            map[string]string     `json:"env"`
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

// rawConfig is the intermediate representation for two-phase JSON parsing.
type rawConfig struct {
	SimpleMQ *SimpleMQConfig `json:"simplemq"`
	RabbitMQ *RabbitMQConfig `json:"rabbitmq"`
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
	Handlers []HandlerConfig `json:"handlers"`
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

// decodeStrict decodes JSON with DisallowUnknownFields into dst.
func decodeStrict(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func parseConfig(data []byte) (*Config, error) {
	var raw rawConfig
	if err := decodeStrict(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Backend exclusivity
	if raw.SimpleMQ != nil && raw.RabbitMQ != nil {
		return nil, fmt.Errorf("simplemq and rabbitmq cannot be configured simultaneously")
	}

	cfg := &Config{
		SimpleMQ: raw.SimpleMQ,
		RabbitMQ: raw.RabbitMQ,
		Handlers: raw.Handlers,
	}

	// Parse backend-specific request/response configs
	switch cfg.BackendType() {
	case BackendRabbitMQ:
		if err := cfg.parseRabbitMQConfigs(raw); err != nil {
			return nil, err
		}
	default:
		if err := cfg.parseSimpleMQConfigs(raw); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func (c *Config) parseSimpleMQConfigs(raw rawConfig) error {
	var req SMQRequestConfig
	if len(raw.Request) > 0 {
		if err := decodeStrict(raw.Request, &req); err != nil {
			return fmt.Errorf("failed to parse request config: %w", err)
		}
	}
	// Apply global SimpleMQ api_url as default
	if req.APIURL == "" && c.SimpleMQ != nil {
		req.APIURL = c.SimpleMQ.APIURL
	}
	c.SMQRequest = &req
	c.RequestQueue = req.Queue

	var res SMQResponseConfig
	if len(raw.Response) > 0 {
		if err := decodeStrict(raw.Response, &res); err != nil {
			return fmt.Errorf("failed to parse response config: %w", err)
		}
	}
	if res.APIURL == "" && c.SimpleMQ != nil {
		res.APIURL = c.SimpleMQ.APIURL
	}
	c.SMQResponse = &res
	c.ResponseQueue = res.Queue

	return nil
}

func (c *Config) parseRabbitMQConfigs(raw rawConfig) error {
	var req RMQRequestConfig
	if len(raw.Request) > 0 {
		if err := decodeStrict(raw.Request, &req); err != nil {
			return fmt.Errorf("failed to parse request config: %w", err)
		}
	}
	c.RMQRequest = &req
	c.RequestQueue = req.Queue

	var res RMQResponseConfig
	if len(raw.Response) > 0 {
		if err := decodeStrict(raw.Response, &res); err != nil {
			return fmt.Errorf("failed to parse response config: %w", err)
		}
	}
	c.RMQResponse = &res
	c.ResponseQueue = res.Queue

	return nil
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

// hasResponseQueue returns true if the response destination is configured.
func (c *Config) hasResponseQueue() bool {
	switch c.BackendType() {
	case BackendRabbitMQ:
		return c.RMQResponse != nil && (c.RMQResponse.Queue != "" || c.RMQResponse.ReplyTo)
	default:
		return c.SMQResponse != nil && c.SMQResponse.Queue != "" && c.SMQResponse.APIKey != ""
	}
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	if c.RequestQueue == "" {
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
			return fmt.Errorf("response.queue or response.reply_to is required when any handler has response enabled")
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
	if c.SMQRequest == nil || c.SMQRequest.APIKey == "" {
		return fmt.Errorf("request.api_key is required")
	}
	return nil
}

func (c *Config) validateRabbitMQ() error {
	if c.RabbitMQ == nil || c.RabbitMQ.URL == "" {
		return fmt.Errorf("rabbitmq.url is required")
	}
	if c.RMQResponse != nil && c.RMQResponse.ReplyTo && c.RMQResponse.Queue != "" {
		return fmt.Errorf("response.reply_to and response.queue cannot both be set")
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
