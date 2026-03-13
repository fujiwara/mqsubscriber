# simplemq-subscriber

SimpleMQ subscriber daemon that polls messages from SAKURA Cloud SimpleMQ, dispatches them to external commands based on header matching, and optionally publishes results back to a response queue. Designed to work with [mqbridge](https://github.com/fujiwara/mqbridge).

## Build & Test

```bash
# Build
go build ./cmd/simplemq-subscriber

# Test (no external services required — uses simplemq-cli/localserver in-process)
go test -v -race ./...

# Format (must run before commit)
go fmt ./...
```

## Architecture

- `app.go` — App struct, main poll loop, message dispatch (blocking/non-blocking), graceful shutdown
- `handler.go` — Handler matching (exact header match), command execution, semaphore concurrency control
- `config.go` — Config structs, Jsonnet loading via `jsonnet-armed`, validation, default constants
- `cli.go` — CLI definition (`kong`), logger setup, `RunCLI()` entry point
- `otel.go` — OpenTelemetry metrics + traces, W3C Trace Context propagation, `headerCarrier`
- `trace_log.go` — `slog.Handler` wrapper that injects `trace_id`/`span_id` into log output
- `secretmanager.go` — SAKURA Cloud Secret Manager native function for Jsonnet `secret()`
- `version.go` — Version variable for tagpr
- `cmd/simplemq-subscriber/main.go` — Minimal main, signal handling

## Conventions

- Package name is `subscriber` (not `simplemq-subscriber` — invalid Go identifier)
- CLI logic lives in `subscriber` package (not `main`) for testability
- Config uses Jsonnet via `jsonnet-armed` which provides `must_env()`, `env()`, `secret()`, hash functions — do not use `std.extVar()`
- Unknown fields in config JSON cause validation errors (`DisallowUnknownFields`)
- SimpleMQ message content is base64-encoded; message body uses mqbridge wire format (JSON with headers/body/body_encoding)
- Import `mqbridge.Message` directly — do not copy the struct
- SimpleMQ default API URL comes from `simplemq.DefaultMessageAPIRootURL` (SDK), not hardcoded
- Default constants (`DefaultPollingInterval`, `DefaultCommandTimeout`, `DefaultMaxConcurrency`) are defined in `config.go`
- `HandlerConfig.Response` is `bool` (defaults to false) — fire-and-forget by default; set `true` for RPC-style request/response
- Response queue (`response.queue` / `response.api_key`) is optional — required only when any handler has `response: true`

## Key Design Decisions

- **Graceful shutdown**: `context.WithoutCancel(ctx)` is used at the poll level (`msgCtx`) so that all in-flight work (command execution → response publish → request delete) completes atomically even during shutdown. `sync.WaitGroup` tracks non-blocking goroutines.
- **Trace propagation**: W3C `traceparent`/`tracestate` headers are embedded in mqbridge message headers. Falls back to `rabbitmq.header.traceparent` because mqbridge prefixes custom AMQP headers with `rabbitmq.header.`.
- **Blocking vs non-blocking handlers**: Blocking handlers process inline in the poll loop. Non-blocking handlers use goroutines with a semaphore (`max_concurrency`); `Acquire()` uses the cancellable context so shutdown can interrupt the semaphore wait.
- **Command stderr**: Logged at Info level (not Warn) because commands must use stderr for logging since stdout is captured as response body.
- **OpenTelemetry**: Metrics and traces are enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Service name is fixed to `simplemq-subscriber`.

## Testing

- Integration tests use `github.com/fujiwara/simplemq-cli/localserver` (in-process SimpleMQ mock) — no external services needed
- `TestMain` in `app_test.go` calls `setupOTelProviders` so traces are exported when `OTEL_EXPORTER_OTLP_ENDPOINT` is set during `go test`
- Use `t.Context()` instead of `context.Background()` in tests

## Dependencies

| Library | Purpose |
|---------|---------|
| `github.com/alecthomas/kong` | CLI parser |
| `github.com/fujiwara/mqbridge` | Message format (`mqbridge.Message`, marshal/unmarshal) |
| `github.com/fujiwara/jsonnet-armed` | Jsonnet config evaluation with built-in functions |
| `github.com/fujiwara/simplemq-cli` | SimpleMQ local server for testing |
| `github.com/fujiwara/sloghandler` | Structured log handler (colored text with source) |
| `github.com/sacloud/simplemq-api-go` | SimpleMQ API client |
| `github.com/sacloud/secretmanager-api-go` | SAKURA Cloud Secret Manager client |
| `go.opentelemetry.io/otel` | OpenTelemetry API, SDK, exporters (metrics + traces) |

## Release

- Uses tagpr for version management + GoReleaser for cross-platform builds
- `.tagpr` config: `vPrefix=true`, version tracked in `version.go`
