# mqsubscriber

A daemon that subscribes to message queues ([SAKURA Cloud SimpleMQ](https://manual.sakura.ad.jp/cloud/appliance/simplemq/index.html) or [RabbitMQ](https://www.rabbitmq.com/)), dispatches messages to external commands based on header matching, and publishes results back to a response queue.

Designed to work with [mqbridge](https://github.com/fujiwara/mqbridge) for bridging on-premises RabbitMQ messaging to the cloud.

## Architecture

**SimpleMQ backend (poll-based):**

```
┌────────────────────────────────────┐
│            SimpleMQ                │
│  [request queue]  [response queue] │
└────────┬──────────────────▲────────┘
         │                  │
         ▼                  │
┌────────────────────────────────────┐
│          mqsubscriber              │
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

**RabbitMQ backend (push-based):**

```
┌────────────────────────────────────┐
│            RabbitMQ                │
│  [request queue]  [response queue] │
└────────┬──────────────────▲────────┘
         │ consume          │ publish
         ▼                  │
┌────────────────────────────────────┐
│          mqsubscriber              │
│  recv → match → execute → publish  │
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

**With [mqbridge](https://github.com/fujiwara/mqbridge) (SimpleMQ + RabbitMQ bridging):**

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
        │  SimpleMQ + mqsubscriber     │
        └──────────────────────────────┘
```

## Message Format

### SimpleMQ backend

mqsubscriber uses the same wire format as [mqbridge](https://github.com/fujiwara/mqbridge) (`mqbridge.Message`). Messages on SimpleMQ are base64-encoded JSON with the following structure:

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

### RabbitMQ backend

Messages are native AMQP deliveries. AMQP metadata (exchange, routing key, reply-to, etc.) and custom headers are mapped to `rabbitmq.*` headers internally, providing the same handler interface regardless of backend.

### Message flow detail

1. **Receive**: The backend delivers a message → mqsubscriber parses it into headers + body
2. **Dispatch**: The `headers` are used for handler matching (e.g., match on `rabbitmq.routing_key`)
3. **Execute**: `body` is passed to the command's stdin. `headers` are available as `MQ_HEADER_*` environment variables
4. **Respond**: Command stdout becomes the new `body`. If `rabbitmq.reply_to` is present, the response is routed to the reply queue via the default exchange (RPC pattern). Otherwise, the response is sent to the configured response queue
5. **Publish**: The response is published via the same backend

## Installation

### Homebrew

```bash
brew install fujiwara/tap/mqsubscriber
```

### Binary releases

Download the latest binary from [GitHub Releases](https://github.com/fujiwara/mqsubscriber/releases).

### Go install

```bash
go install github.com/fujiwara/mqsubscriber/cmd/mqsubscriber@latest
```

## Usage

```bash
# Run the subscriber daemon
mqsubscriber run -c config.jsonnet

# Validate configuration
mqsubscriber validate -c config.jsonnet

# Render configuration as JSON
mqsubscriber render -c config.jsonnet

# Publish a message to the request queue
mqsubscriber publish -c config.jsonnet --body 'hello' -H rabbitmq.routing_key=upper
```

### Options

- `-c`, `--config` (required): Config file path (Jsonnet/JSON). Env: `MQSUBSCRIBER_CONFIG`
- `-e`, `--envfile`: Environment file to load. Variables defined in this file are exported to the process environment before config evaluation. Env: `MQSUBSCRIBER_ENVFILE`
- `--log-format`: Log format (`text` or `json`, default: `text`). Env: `MQSUBSCRIBER_LOG_FORMAT`
- `--log-level`: Log level (`debug`, `info`, `warn`, `error`, default: `info`). Env: `MQSUBSCRIBER_LOG_LEVEL`

## Configuration

Configuration is written in [Jsonnet](https://jsonnet.org/) (plain JSON is also supported). Jsonnet evaluation is powered by [jsonnet-armed](https://github.com/fujiwara/jsonnet-armed), which provides built-in functions for environment variables, hashing, and more. See the [jsonnet-armed README](https://github.com/fujiwara/jsonnet-armed#readme) for the full list of available functions.

Only one of `simplemq` or `rabbitmq` can be configured per process.

### SimpleMQ backend

```jsonnet
{
  simplemq: {
    api_url: "",  // optional, uses default SimpleMQ API URL
    timeout: "30s",  // optional, default: 30s — timeout for queue operations
  },
  request: {
    queue: "request-queue",
    api_key: must_env("REQUEST_API_KEY"),
    polling_interval: "1s",  // optional, default: 1s
  },
  // response queue is optional — required only when any handler has response: true
  response: {
    queue: "response-queue",
    api_key: must_env("RESPONSE_API_KEY"),
  },
  drop_unmatched: false,  // optional, default: false — when true, ack (delete) messages that match no handler
  handlers: [
    // ... (see Handler Configuration below)
  ],
}
```

### RabbitMQ backend

```jsonnet
{
  rabbitmq: {
    url: must_env("AMQP_URL"),  // e.g. "amqp://user:pass@host:5672/"
    timeout: "30s",  // optional, default: 30s — timeout for dial/publish/ack/nack
  },
  request: {
    queue: "request-queue",
    exchange: "my-exchange",       // optional
    exchange_type: "direct",       // optional, default: "direct"
    routing_key: ["deploy", "notify"],  // optional, default: ["#"]
    exchange_passive: false,       // optional, default: false
  },
  // response is optional — required only when any handler has response: true
  response: {
    queue: "response-queue",
    exchange: "",          // optional, default: "" (default exchange)
    routing_key: "",       // optional, default: response queue name
    reply_to: false,       // optional, default: false (see below)
  },
  drop_unmatched: false,  // optional, default: false — when true, ack (delete) messages that match no handler
  handlers: [
    // ... (see Handler Configuration below)
  ],
}
```

### Handler Configuration

```jsonnet
{
  handlers: [
    {
      name: "deploy",
      match: {
        "rabbitmq.routing_key": "deploy",
        "rabbitmq.header.x-env": "production",
      },
      command: ["/usr/local/bin/deploy.sh"],
      env: {              // optional: custom environment variables for this handler
        "DEPLOY_TARGET": "production",
      },
      timeout: "60s",     // optional, default: 30s
      blocking: true,     // wait for completion before processing next message
      response: true,     // send response back (requires response queue or reply_to)
      response_ignore: {  // optional: suppress response for specific exit code
        exit_code: 99,    // if command exits with 99, no response is sent
      },
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
      // response defaults to false (fire-and-forget)
      log_message: "processing notification",  // optional: custom log message per handler
      log_header_fields: ["rabbitmq.routing_key"],  // optional: header keys to include in log
      log_body_fields: ["notification_id", "channel"],  // optional: JSON body fields to include in log
    },
  ],
}
```

### Custom Handler Logging

Each handler can emit a custom log message when it starts processing a message, with selected fields extracted from message headers and/or the JSON body.

- `log_message` (string): Custom message to log at Info level. If not set, no custom log is emitted.
- `log_header_fields` ([]string): List of message header keys to include in the log as `header.<key>` attributes. Missing headers are silently skipped.
- `log_body_fields` ([]string): List of top-level JSON field names to extract from the message body and include in the log as `body.<field>` attributes. Only parsed when `log_body_fields` is set. If the body is not valid JSON, a warning is logged. Missing fields are silently skipped.

Example log output:
```
INFO processing notification  handler=notify message_id=abc123 header.rabbitmq.routing_key=notify body.notification_id=N-001 body.channel=slack
```

### Handler Matching

- `match` defines header key-value pairs that must **all** match (AND condition)
- By default, values must match **exactly**
- Set `match_pattern: true` to enable **AMQP topic-style pattern matching** on all match values:
  - `*` matches exactly **one** dot-delimited word (e.g., `order.*` matches `order.created` but not `order.created.v2`)
  - `#` matches **zero or more** dot-delimited words (e.g., `order.#` matches `order`, `order.created`, and `order.created.v2`)
  - Values without `*` or `#` still match exactly, so you can mix patterns and literal values
- Handlers are evaluated in order; the **first match wins**
- Messages that match no handler are nacked by default (SimpleMQ: redelivered after visibility timeout; RabbitMQ: nack without requeue, routed to dead-letter exchange if configured). Set `drop_unmatched: true` to ack (delete) them instead

Example with pattern matching:

```jsonnet
{
  handlers: [
    {
      name: "order-handler",
      match: {
        "rabbitmq.routing_key": "order.*",   // matches order.created, order.updated, etc.
        "rabbitmq.header.x-env": "production",  // exact match
      },
      match_pattern: true,
      command: ["/usr/local/bin/handle-order.sh"],
    },
  ],
}
```

### Blocking vs Non-blocking

- **blocking: true** — The subscriber waits for the command to complete before processing the next message
- **blocking: false** — The command runs in a goroutine. The subscriber immediately proceeds to the next message. When `max_concurrency` is reached, the subscriber blocks until a slot is available

### Command Execution

- Message body is passed via **stdin**
- Environment variables are inherited from the mqsubscriber process, with the following additions (later entries override earlier ones):
  1. **Parent process environment** — all environment variables from the mqsubscriber process
  2. **Handler `env`** — per-handler custom environment variables (see Handler Configuration)
  3. **Message headers** — passed as `MQ_HEADER_*` variables (dots and hyphens are converted to underscores, uppercased). e.g., `rabbitmq.routing_key` → `MQ_HEADER_RABBITMQ_ROUTING_KEY`
- Command **stdout** becomes the response message body
- Command **stderr** is logged
- If the command times out, **SIGTERM** is sent to the entire process group (including child processes) to allow graceful shutdown. If the process does not exit within 30 seconds, it is forcibly killed (SIGKILL)
- If the command fails (non-zero exit) and `response` is disabled, the message is **nacked** (SimpleMQ: not deleted, redelivered after visibility timeout; RabbitMQ: nack without requeue, routed to dead-letter exchange if configured). If `circuit_breaker` is configured, the message is dropped (acked) after reaching the error threshold (see [Circuit Breaker](#circuit-breaker))

### Circuit Breaker

When a fire-and-forget handler (`response: false`) encounters repeated errors for the same message, the message is normally nacked and redelivered indefinitely. The `circuit_breaker` option prevents this by dropping (acking) the message after a configured number of failures.

```jsonnet
{
  name: "process-task",
  match: { "type": "task" },
  command: ["./process.sh"],
  circuit_breaker: {
    max_errors: 5,    // drop the message after 5 failures
    ttl: "10m",       // reset error count after 10 minutes (default: 10m)
  },
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_errors` | int | (required) | Number of errors before dropping the message |
| `ttl` | string | `"10m"` | Duration after which the error count resets |

**Message identity across redeliveries:**

- **SimpleMQ**: Uses the message ID (stable across redeliveries)
- **RabbitMQ**: Uses the `rabbitmq.message_id` header (mapped from AMQP `MessageId` property). Publishers must set `MessageId` for the circuit breaker to work; without it, each redelivery gets a new delivery tag and the circuit breaker cannot track the message

Error counts are stored in-memory per handler (up to 1024 tracked messages, LRU eviction). Counts reset on process restart.

`circuit_breaker` cannot be used with `response: true` (response mode already acks on error).

### Response Publishing

Response messages are published with retry (3 attempts, exponential backoff: 1s, 2s, 4s). If all retries are exhausted, the request message is still acknowledged to prevent command re-execution on redelivery.

### Response Chain Guard

When a response message is published, mqsubscriber sets the `mqsubscriber.responded` header with an incrementing counter (starting at `1`). On receiving a message, if this counter reaches the configured `max_response_chain` limit, the message is logged as a warning and dropped (acked without processing). This prevents infinite loops when a response is accidentally routed back to the request queue.

The `max_response_chain` setting is a top-level config option:

```jsonnet
{
  // ...
  max_response_chain: 0,  // default: 0 (no chaining allowed)
  handlers: [ /* ... */ ],
}
```

| Value | Behavior |
|-------|----------|
| `0` (default) | Responses that arrive back as requests are dropped |
| `1` | One chain hop is allowed (response → re-process → drop) |
| `N` | Up to N chain hops are allowed |

### Response Status Headers

Response messages include status headers to indicate success or failure:

| Header | Description |
|--------|-------------|
| `x-status` | `success` or `error` |
| `x-exit-code` | Exit code (only set on error, e.g. `1`) |

When the message originates from RabbitMQ (has `rabbitmq.exchange` header), these headers use the `rabbitmq.header.` prefix (e.g. `rabbitmq.header.x-status`) so they are mapped to AMQP headers.

**Error handling by `response` setting:**

- **`response: true`**: On command failure, an error response is sent with `x-status: error`, the last 4KB of stderr as the body, and the message is acknowledged. This ensures the caller is not left waiting indefinitely.
- **`response: false`** (default): On command failure, no response is sent and the message is **nacked** for redelivery. No response queue is needed.

### Suppressing Response (`response_ignore`)

When `response: true` is set, you can selectively suppress the response based on the command's exit code using `response_ignore`. This is useful when running multiple subscriber instances where only one should respond (e.g., the command acquires a lock and only the winner responds).

```jsonnet
{
  name: "my-handler",
  response: true,
  response_ignore: {
    exit_code: 99,  // suppress response when command exits with 99
  },
  // ...
}
```

When the command exits with the specified `exit_code`:
- No response message is published
- The request message is acknowledged (the command already ran)
- The event is logged at Info level

For any other exit code, the normal behavior applies (success response for exit 0, error response for other non-zero codes).

### Response `reply_to` Mode (RabbitMQ)

When using the RabbitMQ backend with the RPC pattern, you can set `response.reply_to: true` instead of specifying a `response.queue`. In this mode, responses are routed exclusively via the `rabbitmq.reply_to` header from each incoming message — no static response queue is needed.

```jsonnet
{
  rabbitmq: { url: must_env("AMQP_URL") },
  request: { queue: "request-queue" },
  response: { reply_to: true },
  handlers: [
    { name: "rpc", match: { /* ... */ }, command: ["./handler.sh"], response: true },
  ],
}
```

`response.reply_to` and `response.queue` cannot both be set — they are mutually exclusive.

### RPC Response Routing

When a message contains a `rabbitmq.reply_to` header (set by RabbitMQ RPC clients), the response is automatically routed to the reply queue:

- `rabbitmq.exchange` is set to `""` (default exchange)
- `rabbitmq.routing_key` is set to the `rabbitmq.reply_to` value
- `rabbitmq.reply_to` is removed from the response headers
- `rabbitmq.correlation_id` is preserved as-is

This enables the standard RabbitMQ RPC pattern: the response is delivered directly to the caller's exclusive reply queue via the default exchange.

If `rabbitmq.reply_to` is **not present**, the request routing headers (`rabbitmq.exchange`, `rabbitmq.routing_key`) are removed from the response so that the publisher uses the configured response queue instead.

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
- On command execution: sets `TRACEPARENT`/`TRACESTATE` environment variables for child processes (W3C standard)
- On publish subcommand: reads `TRACEPARENT`/`TRACESTATE` environment variables to continue the trace from the parent process

**Spans:**

| Span | Description | Key Attributes |
|------|-------------|----------------|
| `mqsubscriber.handle_message` | Per-message processing | `handler`, `message_id`, `blocking`, `request.header.*` |
| `mqsubscriber.execute` | Command execution | `handler`, `command`, `command.timed_out`, `exit_code` |
| `mqsubscriber.publish` | Response publish | `queue`, `response.header.*` |
| `publish` | Publish subcommand | `messaging.destination.name`, `messaging.message.body.size` |

Errors (command failure, publish failure) are recorded on spans with `Error` status.

### Metrics

| Metric | Type | Description | Attributes |
|--------|------|-------------|------------|
| `mqsubscriber.messages.received` | Counter | Messages received from request queue | — |
| `mqsubscriber.messages.processed` | Counter | Messages successfully processed | `handler` |
| `mqsubscriber.messages.errors` | Counter | Message processing errors | `handler` |
| `mqsubscriber.messages.dropped` | Counter | Messages dropped/acked with no matching handler (`drop_unmatched: true`) | — |
| `mqsubscriber.messages.unmatched` | Counter | Messages nacked with no matching handler (`drop_unmatched: false`) | — |
| `mqsubscriber.messages.circuit_broken` | Counter | Messages dropped by circuit breaker after repeated failures | `handler` |
| `mqsubscriber.command.duration` | Histogram | Command execution duration (seconds) | `handler` |
| `mqsubscriber.command.timeouts` | Counter | Command execution timeouts | `handler` |
| `mqsubscriber.log.messages` | Counter | Number of log messages by level | `level` |

## Publish Subcommand

The `publish` subcommand sends a message to the request or response queue. This is useful for debugging handlers, testing configurations, and self-invoking commands.

```bash
mqsubscriber publish -c config.jsonnet [flags]
```

### Flags

| Flag | Description |
|------|-------------|
| `-H key=value` | Message header (repeatable) |
| `--body <string>` | Message body as a string |
| `--body-file <path>` | Read message body from a file |
| `--request` | Publish to the request queue (default) |
| `--response` | Publish to the response queue |
| (stdin) | If neither `--body` nor `--body-file` is given, body is read from stdin |

`--body` and `--body-file` are mutually exclusive. `--request` and `--response` are mutually exclusive.

### Destination Routing

By default, messages are sent to the request queue. Use `--response` to send to the response queue instead.

**RabbitMQ backend:**

By default, messages are sent to `request.queue` via the default exchange. You can override the destination by setting headers — this uses the same routing logic as response publishing.

| Headers provided | Destination |
|------------------|-------------|
| (none) | Default exchange → `request.queue` |
| `-H rabbitmq.routing_key=KEY` | Default exchange → `KEY` |
| `-H rabbitmq.exchange=EX -H rabbitmq.routing_key=KEY` | Exchange `EX` → routing key `KEY` |

### Usage Examples

```bash
# Send to the request queue (default)
mqsubscriber publish -c config.jsonnet --body 'hello'

# Send to the response queue
mqsubscriber publish -c config.jsonnet --response --body 'hello'

# Send with routing key (matched by handler's match condition)
mqsubscriber publish -c config.jsonnet --body 'hello' \
  -H rabbitmq.routing_key=upper

# Send to a named exchange with a routing key
mqsubscriber publish -c config.jsonnet --body 'hello' \
  -H rabbitmq.exchange=my-exchange -H rabbitmq.routing_key=deploy

# Set custom headers (works for both SimpleMQ and RabbitMQ)
# For RabbitMQ, non-rabbitmq.* headers are sent as AMQP headers
# and received as rabbitmq.header.* (e.g. x-priority → rabbitmq.header.x-priority)
mqsubscriber publish -c config.jsonnet --body 'hello' \
  -H rabbitmq.routing_key=upper -H x-priority=high

# Pipe body from stdin
echo '{"action":"deploy"}' | mqsubscriber publish -c config.jsonnet \
  -H rabbitmq.routing_key=deploy

# Read body from file
mqsubscriber publish -c config.jsonnet \
  --body-file payload.json -H rabbitmq.routing_key=deploy
```

## Examples

- [End-to-end RPC example with Docker Compose](examples/rpc/) — Demonstrates the full RabbitMQ RPC round-trip locally with OpenTelemetry trace visualization

## LICENSE

MIT

## Author

fujiwara
