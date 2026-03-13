# simplemq-subscriber

A daemon that subscribes to [SAKURA Cloud SimpleMQ](https://manual.sakura.ad.jp/cloud/appliance/simplemq/index.html) queues, dispatches messages to external commands based on header matching, and publishes results back to a response queue.

Designed to work with [mqbridge](https://github.com/fujiwara/mqbridge) for bridging on-premises RabbitMQ messaging to the cloud.

## Architecture

**Basic usage (SimpleMQ only):**

```
┌────────────────────────────────────┐
│            SimpleMQ                │
│  [request queue]  [response queue] │
└────────┬──────────────────▲────────┘
         │                  │
         ▼                  │
┌────────────────────────────────────┐
│       simplemq-subscriber          │
│  poll → match → execute → publish  │
└────────────────┬───────────────────┘
                 │
                 ▼
          ┌──────────────┐
          │   command    │
          │  stdin: body │
          │  env: headers│
          │  stdout: resp│
          └──────────────┘
```

**With [mqbridge](https://github.com/fujiwara/mqbridge) (RabbitMQ integration):**

```
           On-premises                Cloud (SAKURA Cloud)
        ┌──────────────┐
        │   RabbitMQ   │
        └──────┬───────┘
  Request      │  ▲      Response
               ▼  │
        ┌──────────────┐
        │   mqbridge   │
        └──────┬───────┘
               │  ▲
    ═══════════╪══╪═══════════════════════════════
               │  │
               ▼  │
        ┌──────────────────────────────┐
        │  SimpleMQ + subscriber       │
        │  (same as above)             │
        └──────────────────────────────┘
```

## Message Format

simplemq-subscriber uses the same wire format as [mqbridge](https://github.com/fujiwara/mqbridge) (`mqbridge.Message`). Messages on SimpleMQ are base64-encoded JSON with the following structure:

```json
{
  "headers": {
    "rabbitmq.exchange": "my-exchange",
    "rabbitmq.routing_key": "my.routing.key",
    "rabbitmq.header.x-custom": "value"
  },
  "body": "message body text",
  "body_encoding": "base64"
}
```

- `headers`: Key-value metadata. When originating from RabbitMQ via mqbridge, headers are prefixed with `rabbitmq.` (e.g., `rabbitmq.exchange`, `rabbitmq.routing_key`, `rabbitmq.correlation_id`, `rabbitmq.header.*` for custom AMQP headers)
- `body`: The message payload. Plain string if valid UTF-8, or base64-encoded for binary data
- `body_encoding`: Set to `"base64"` when the body is base64-encoded (binary-safe). Omitted for plain text

### Message flow detail

1. **Receive**: SimpleMQ delivers base64-encoded content → simplemq-subscriber decodes it → `mqbridge.UnmarshalMessage()` parses the JSON into `mqbridge.Message` (headers + body)
2. **Dispatch**: The `headers` are used for handler matching (e.g., match on `rabbitmq.routing_key`)
3. **Execute**: `body` is passed to the command's stdin. `headers` are available as `SIMPLEMQ_HEADER_*` environment variables
4. **Respond**: Command stdout becomes the new `body`. If `rabbitmq.reply_to` is present, the response is routed to the reply queue via the default exchange (RPC pattern). Otherwise, the original headers are preserved as-is
5. **Publish**: The response is serialized via `mqbridge.MarshalMessage()` → base64-encoded → sent to the response queue

This ensures full round-trip compatibility: RabbitMQ → mqbridge → SimpleMQ → simplemq-subscriber → SimpleMQ → mqbridge → RabbitMQ.

## Installation

### Homebrew

```bash
brew install fujiwara/tap/simplemq-subscriber
```

### Binary releases

Download the latest binary from [GitHub Releases](https://github.com/fujiwara/simplemq-subscriber/releases).

### Go install

```bash
go install github.com/fujiwara/simplemq-subscriber/cmd/simplemq-subscriber@latest
```

## Usage

```bash
# Run the subscriber daemon
simplemq-subscriber run -c config.jsonnet

# Validate configuration
simplemq-subscriber validate -c config.jsonnet

# Render configuration as JSON
simplemq-subscriber render -c config.jsonnet
```

### Options

- `-c`, `--config` (required): Config file path (Jsonnet/JSON). Env: `SIMPLEMQ_SUBSCRIBER_CONFIG`
- `--log-format`: Log format (`text` or `json`, default: `text`). Env: `SIMPLEMQ_SUBSCRIBER_LOG_FORMAT`
- `--log-level`: Log level (`debug`, `info`, `warn`, `error`, default: `info`). Env: `SIMPLEMQ_SUBSCRIBER_LOG_LEVEL`

## Configuration

Configuration is written in [Jsonnet](https://jsonnet.org/) (plain JSON is also supported). Jsonnet evaluation is powered by [jsonnet-armed](https://github.com/fujiwara/jsonnet-armed), which provides built-in functions for environment variables, hashing, and more. See the [jsonnet-armed README](https://github.com/fujiwara/jsonnet-armed#readme) for the full list of available functions.

```jsonnet
{
  simplemq: {
    api_url: "",  // optional, uses default SimpleMQ API URL
  },
  request: {
    queue: "request-queue",
    api_key: must_env("REQUEST_API_KEY"),
    polling_interval: "1s",  // optional, default: 1s
  },
  response: {
    queue: "response-queue",
    api_key: must_env("RESPONSE_API_KEY"),
  },
  handlers: [
    {
      name: "deploy",
      match: {
        "rabbitmq.routing_key": "deploy",
        "rabbitmq.header.x-env": "production",
      },
      command: ["/usr/local/bin/deploy.sh"],
      timeout: "60s",     // optional, default: 30s
      blocking: true,     // wait for completion before processing next message
    },
    {
      name: "notify",
      match: {
        "rabbitmq.routing_key": "notify",
      },
      command: ["/usr/local/bin/notify.sh"],
      timeout: "10s",
      blocking: false,       // run in background goroutine
      max_concurrency: 5,    // max concurrent executions (default: 1)
    },
  ],
}
```

### Handler Matching

- `match` defines header key-value pairs that must **all** match exactly (AND condition)
- Handlers are evaluated in order; the **first match wins**
- Messages that match no handler are logged and dropped

### Blocking vs Non-blocking

- **blocking: true** — The subscriber waits for the command to complete before polling the next message
- **blocking: false** — The command runs in a goroutine. The subscriber immediately proceeds to the next message. When `max_concurrency` is reached, the subscriber blocks until a slot is available (other subscribers can pick up messages in the meantime)

### Command Execution

- Message body is passed via **stdin**
- Message headers are passed as environment variables with the prefix `SIMPLEMQ_HEADER_` (dots and hyphens are converted to underscores, uppercased)
  - e.g., `rabbitmq.routing_key` → `SIMPLEMQ_HEADER_RABBITMQ_ROUTING_KEY`
- Command **stdout** becomes the response message body
- Command **stderr** is logged
- If the command fails (non-zero exit) and `response` is disabled, the message is **not deleted** and will be redelivered after the visibility timeout

### Response Status Headers

Response messages include status headers to indicate success or failure:

| Header | Description |
|--------|-------------|
| `x-status` | `success` or `error` |
| `x-exit-code` | Exit code (only set on error, e.g. `1`) |

When the message originates from RabbitMQ (`rabbitmq.reply_to` is present), these headers use the `rabbitmq.header.` prefix (e.g. `rabbitmq.header.x-status`) so they are mapped to AMQP headers by mqbridge.

**Error handling by `response` setting:**

- **`response: true`** (default): On command failure, an error response is sent with `x-status: error`, the last 4KB of stderr as the body, and the message is deleted. This ensures the caller is not left waiting indefinitely.
- **`response: false`**: On command failure, no response is sent and the message is **not deleted** (will be redelivered for retry).

### RPC Response Routing

When a message contains a `rabbitmq.reply_to` header (set by RabbitMQ RPC clients), the response is automatically routed to the reply queue:

- `rabbitmq.exchange` is set to `""` (default exchange)
- `rabbitmq.routing_key` is set to the `rabbitmq.reply_to` value
- `rabbitmq.reply_to` is removed from the response headers
- `rabbitmq.correlation_id` is preserved as-is

This enables the standard RabbitMQ RPC pattern: the response is delivered directly to the caller's exclusive reply queue via the default exchange.

If `rabbitmq.reply_to` is **not present**, the original headers are preserved as-is in the response. This allows simplemq-subscriber to be used independently of RabbitMQ, where messages are simply forwarded between SimpleMQ queues without RPC routing.

### Jsonnet Built-in Functions

The following functions are available in config files via [jsonnet-armed](https://github.com/fujiwara/jsonnet-armed):

- `must_env("VAR")` — Read environment variable (error if not set)
- `env("VAR", "default")` — Read environment variable with default
- `secret("vault-id", "name")` — Read from [SAKURA Cloud Secret Manager](https://manual.sakura.ad.jp/cloud/appliance/secretsmanager/index.html)
- `sha256(str)`, `md5(str)` — Hash functions
- See [jsonnet-armed README](https://github.com/fujiwara/jsonnet-armed#readme) for more

## Observability

OpenTelemetry metrics and traces are automatically enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

### Traces

Distributed tracing is supported via [W3C Trace Context](https://www.w3.org/TR/trace-context/) propagation through message headers.

**Trace context propagation:**
- On receive: extracts `traceparent`/`tracestate` from message headers (falls back to `rabbitmq.header.traceparent` for mqbridge compatibility)
- On response: injects `traceparent`/`tracestate` into response message headers

**Spans:**

| Span | Description | Key Attributes |
|------|-------------|----------------|
| `simplemq_subscriber.handle_message` | Per-message processing | `handler`, `message_id`, `blocking`, `request.header.*` |
| `simplemq_subscriber.execute` | Command execution | `handler`, `command` |
| `simplemq_subscriber.publish` | Response publish | `queue`, `response.header.*` |

Errors (command failure, publish failure) are recorded on spans with `Error` status.

### Metrics

| Metric | Type | Description | Attributes |
|--------|------|-------------|------------|
| `simplemq_subscriber.messages.received` | Counter | Messages received from request queue | — |
| `simplemq_subscriber.messages.processed` | Counter | Messages successfully processed | `handler` |
| `simplemq_subscriber.messages.errors` | Counter | Message processing errors | `handler` |
| `simplemq_subscriber.messages.dropped` | Counter | Messages dropped (no matching handler) | — |
| `simplemq_subscriber.command.duration` | Histogram | Command execution duration (seconds) | `handler` |

## LICENSE

MIT

## Author

fujiwara
