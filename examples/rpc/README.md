# End-to-end RPC Example

Demonstrates a full RabbitMQ RPC round-trip using Docker Compose:

```
Client → RabbitMQ → mqbridge → SimpleMQ → mqsubscriber → SimpleMQ → mqbridge → RabbitMQ → Client
```

The client sends `"hello world"` with routing key `upper`. The subscriber runs `tr a-z A-Z` and returns `"HELLO WORLD"`. All components export OpenTelemetry traces to [otel-front](https://github.com/mesaglio/otel-front) for visualization.

## Quick Start

```bash
cd examples/rpc
docker compose up --build
```

The client sends RPC requests every 5 seconds and prints responses. Press Ctrl+C to stop all services.

## Services

| Service | Description | Ports |
|---------|-------------|-------|
| rabbitmq | Message broker | 5672 (AMQP), 15672 (Management UI) |
| simplemq-localserver | In-process SimpleMQ mock server | 18080 (internal) |
| mqbridge | Bridges RabbitMQ ↔ SimpleMQ (request & response) | — |
| subscriber | Polls SimpleMQ, executes `tr a-z A-Z` | — |
| otel-front | OpenTelemetry trace viewer | 8000 (UI) |
| client | Sends RPC requests every 5 seconds | — |

## Verification

1. Client output shows `"HELLO WORLD"` as the response body
2. Open http://localhost:8000 to view distributed traces in otel-front
3. Open http://localhost:15672 to view RabbitMQ management (guest/guest)

## Architecture

### Message Flow

1. **Client** publishes a message to RabbitMQ exchange `commands` with routing key `upper`, setting `ReplyTo` and `traceparent` headers
2. **mqbridge** (request bridge) consumes from RabbitMQ queue `rpc-request` and publishes to SimpleMQ queue `smq-request`
3. **mqsubscriber** polls `smq-request`, matches the `upper` handler, pipes the message body through `tr a-z A-Z`, and publishes the result to SimpleMQ queue `smq-response`
4. **mqbridge** (response bridge) polls `smq-response` and routes the response back to RabbitMQ using the `ReplyTo` header
5. **Client** receives the response on its exclusive reply queue

### Trace Propagation

W3C `traceparent` header flows through all components, creating a connected distributed trace visible in otel-front.

## Cleanup

```bash
docker compose down -v
```
