# mqsubscriber

MQ subscriber daemon that receives messages from SAKURA Cloud SimpleMQ or RabbitMQ, dispatches them to external commands based on header matching, and optionally publishes results back to a response queue. Designed to work with [mqbridge](https://github.com/fujiwara/mqbridge).

## Build & Test

```bash
# Build
go build ./cmd/mqsubscriber

# Test (no external services required — uses simplemq-cli/localserver in-process)
go test -v -race ./...

# RabbitMQ integration tests (requires Docker)
go test -v -race -tags integration ./...

# Format (must run before commit)
go fmt ./...
```

## Architecture

- `queue.go` — QueueClient interface (Receive/Publish/Ack/Nack/Close), QueueMessage type
- `queue_simplemq.go` — SimpleMQ QueueClient implementations (SimpleMQReceiver, SimpleMQPublisher)
- `queue_rabbitmq.go` — RabbitMQ QueueClient implementations (RabbitMQReceiver, RabbitMQPublisher)
- `app.go` — App struct, main loop (poll-based for SimpleMQ, push-based for RabbitMQ), message dispatch (blocking/non-blocking), graceful shutdown
- `handler.go` — Handler matching (exact header match), command execution, semaphore concurrency control
- `circuit_breaker.go` — CircuitBreaker struct (per-message error tracking with expirable LRU), messageKey helper
- `config.go` — Config structs, Jsonnet loading via `jsonnet-armed`, validation, default constants, backend type detection
- `publish.go` — `PublishCmd` subcommand, `newRequestPublisher` helper, header parsing
- `cli.go` — CLI definition (`kong`), logger setup, `RunCLI()` entry point
- `otel.go` — OpenTelemetry metrics + traces, W3C Trace Context propagation, `headerCarrier`
- `trace_log.go` — `slog.Handler` wrapper that injects `trace_id`/`span_id` into log output
- `secretmanager.go` — SAKURA Cloud Secret Manager native function for Jsonnet `secret()`
- `version.go` — Version variable for tagpr
- `cmd/mqsubscriber/main.go` — Minimal main, signal handling

## Conventions

- Package name is `subscriber` (not `mqsubscriber` — invalid Go identifier)
- CLI logic lives in `subscriber` package (not `main`) for testability
- Config uses Jsonnet via `jsonnet-armed` which provides `must_env()`, `env()`, `secret()`, hash functions — do not use `std.extVar()`
- Unknown fields in config JSON cause validation errors (`DisallowUnknownFields`)
- Only one MQ backend per process — `simplemq` and `rabbitmq` config sections are mutually exclusive
- QueueClient interface abstracts backend differences — app.go is backend-agnostic
- SimpleMQ message content is base64-encoded; message body uses mqbridge wire format (JSON with headers/body/body_encoding)
- RabbitMQ messages are native AMQP deliveries; metadata mapped to `rabbitmq.*` headers by `messageFromDelivery`
- Import `mqbridge.Message` directly — do not copy the struct
- SimpleMQ default API URL comes from `simplemq.DefaultMessageAPIRootURL` (SDK), not hardcoded
- Default constants (`DefaultPollingInterval`, `DefaultCommandTimeout`, `DefaultMaxConcurrency`, `DefaultQueueTimeout`) are defined in `config.go`
- `SimpleMQConfig.Timeout` and `RabbitMQConfig.Timeout` control queue operation timeouts (default 30s). SimpleMQ uses `context.WithTimeout`; RabbitMQ uses dial timeout + `withTimeout` goroutine wrapper for publish/ack/nack (amqp library ignores context)
- `HandlerConfig.MatchPattern` is `bool` (defaults to false) — when true, match values use AMQP topic-style patterns (`*` = one word, `#` = zero or more words). Patterns are compiled to regexps at handler creation time
- `HandlerConfig.Response` is `bool` (defaults to false) — fire-and-forget by default; set `true` for RPC-style request/response
- `HandlerConfig.ResponseIgnore` is `*ResponseIgnoreConfig` — when set with `exit_code`, suppresses response for that exit code (message is acked but no response published). Requires `response: true`
- `HandlerConfig.LogMessage` is `string` — custom log message emitted at Info level when handling a message. No-op if empty
- `HandlerConfig.LogHeaderFields` is `[]string` — message header keys to include in log as `header.<key>` attributes. Missing headers are silently skipped
- `HandlerConfig.LogBodyFields` is `[]string` — top-level JSON fields to extract from message body and include in log. Body is only parsed when this is set; parse failure logs a warning
- `HandlerConfig.CircuitBreaker` is `*CircuitBreakerConfig` — when set, drops (acks) messages after `max_errors` consecutive failures for the same message. Only valid for fire-and-forget handlers (`response: false`). Uses `hashicorp/golang-lru/v2/expirable` for TTL-based entry expiry. Message identity: SimpleMQ uses `QueueMessage.ID` (stable); RabbitMQ uses `rabbitmq.message_id` header (requires publisher to set `MessageId`). `DefaultCircuitBreakerMaxEntries` (1024) caps tracked message keys per handler
- `Config.MaxResponseChain` is `int` (default 0) — number of allowed response chain hops. 0 means responses routed back as requests are dropped. N allows up to N chain hops
- `Config.DropUnmatched` is `bool` (default false) — when false, messages matching no handler are nacked (SimpleMQ: redelivered after visibility timeout; RabbitMQ: nack without requeue). When true, unmatched messages are acked (deleted)
- `QueueClient.Nack` always nacks without requeue — SimpleMQ: no-op (visibility timeout handles redelivery); RabbitMQ: nack with requeue=false (message routed to dead-letter exchange if configured)
- Response queue is optional — required only when any handler has `response: true`
- Response publishing retries 3 times with exponential backoff (1s, 2s, 4s). On exhaustion, the request message is still acked to prevent command re-execution
- `publish` subcommand uses the same retry policy (`publishWithRetry` in `publish.go`) — each invocation opens a new connection, so transient dial failures are retried. Backoff waits respect context cancellation (Ctrl+C aborts immediately)
- Command timeout sends SIGTERM to the process group (Setpgid + negative PID kill), then SIGKILL after 30s WaitDelay. `CommandResult.TimedOut` tracks timeout state. Timeout is recorded as `command.timed_out` span attribute and `mqsubscriber.command.timeouts` metric counter
- Commands inherit the parent process environment, overlaid with handler `env`, then `MQ_HEADER_*` from message headers
- Environment variable prefix for headers is `MQ_HEADER_` (not `SIMPLEMQ_HEADER_`)

## Key Design Decisions

- **QueueClient abstraction**: `QueueClient` interface with `Receive`/`Publish`/`Ack`/`Nack`/`Close` methods. SimpleMQ uses HTTP polling; RabbitMQ uses AMQP push via `ch.Consume()`. The `internal any` field in `QueueMessage` carries backend-specific data (SimpleMQ MessageId, RabbitMQ *amqp.Delivery).
- **Graceful shutdown**: `context.WithoutCancel(ctx)` is used so that all in-flight work (command execution → response publish → ack) completes atomically even during shutdown. `sync.WaitGroup` tracks non-blocking goroutines.
- **Poll vs push loops**: SimpleMQ uses `runPollLoop` (ticker + drain). RabbitMQ uses `runPushLoop` (blocking Receive in a loop). Both share the same `poll()` → `handleMessage()` path.
- **Trace propagation**: W3C `traceparent`/`tracestate` headers are embedded in mqbridge message headers. Falls back to `rabbitmq.header.traceparent` because mqbridge prefixes custom AMQP headers with `rabbitmq.header.`. Commands receive `TRACEPARENT`/`TRACESTATE` environment variables; `publish` subcommand reads these to continue the trace from the parent process.
- **Blocking vs non-blocking handlers**: Blocking handlers process inline. Non-blocking handlers use goroutines with a semaphore (`max_concurrency`); `Acquire()` uses the cancellable context so shutdown can interrupt the semaphore wait.
- **Command stderr**: Logged at Info level (not Warn) because commands must use stderr for logging since stdout is captured as response body.
- **OpenTelemetry**: Metrics and traces are enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Service name is `mqsubscriber`.
- **RabbitMQ reconnection**: Exponential backoff (initial 1s, max 30s), same pattern as mqbridge.
- **RabbitMQ prefetch**: Set to the sum of all handlers' `max_concurrency` to prevent over-fetching.
- **Non-RPC response routing**: `buildResponse` removes `rabbitmq.exchange`/`rabbitmq.routing_key` from non-RPC responses so the publisher uses the configured response queue instead of routing back to the request exchange.
- **Response chain guard**: `buildResponse` increments `mqsubscriber.responded` header on every response. `poll()` checks this counter against `max_response_chain` before dispatching — messages exceeding the limit are warn-logged and ack-dropped. Default limit is 0 (no chaining).
- **Circuit breaker**: Per-handler `CircuitBreaker` tracks error counts per message key using `expirable.LRU` (thread-safe, TTL-based eviction). On fire-and-forget failure, `shouldCircuitBreak` increments the count and returns true at threshold — the message is acked (dropped) instead of nacked. On success, `clearCircuitBreaker` removes the entry. In-memory only; counts reset on process restart.

## Testing

- SimpleMQ tests use `github.com/fujiwara/simplemq-cli/localserver` (in-process SimpleMQ mock) — no external services needed
- RabbitMQ integration tests use `testcontainers-go` (Docker required) — gated by `//go:build integration` build tag
- `TestMain` in `app_test.go` calls `setupOTelProviders` so traces are exported when `OTEL_EXPORTER_OTLP_ENDPOINT` is set during `go test`
- Use `t.Context()` instead of `context.Background()` in tests

## Dependencies

| Library | Purpose |
|---------|---------|
| `github.com/alecthomas/kong` | CLI parser |
| `github.com/fujiwara/mqbridge` | Message format (`mqbridge.Message`, marshal/unmarshal, header constants) |
| `github.com/fujiwara/jsonnet-armed` | Jsonnet config evaluation with built-in functions |
| `github.com/fujiwara/simplemq-cli` | SimpleMQ local server for testing |
| `github.com/fujiwara/sloghandler` | Structured log handler (colored text with source) |
| `github.com/rabbitmq/amqp091-go` | RabbitMQ AMQP client |
| `github.com/sacloud/simplemq-api-go` | SimpleMQ API client |
| `github.com/sacloud/secretmanager-api-go` | SAKURA Cloud Secret Manager client |
| `github.com/testcontainers/testcontainers-go` | RabbitMQ integration tests (Docker) |
| `go.opentelemetry.io/otel` | OpenTelemetry API, SDK, exporters (metrics + traces) |

## Release

- Uses tagpr for version management + GoReleaser for cross-platform builds
- `.tagpr` config: `vPrefix=true`, version tracked in `version.go`
