package subscriber

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/fujiwara/simplemq-subscriber"

// headerCarrier adapts mqbridge.Message.Headers for OTel propagation.
type headerCarrier map[string]string

func (c headerCarrier) Get(key string) string { return c[key] }
func (c headerCarrier) Set(key, val string)   { c[key] = val }
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

const (
	// W3C Trace Context header keys
	headerTraceparent = "traceparent"
	headerTracestate  = "tracestate"

	// Fallback: RabbitMQ custom header prefix used by mqbridge
	headerRMQTraceparent = "rabbitmq.header.traceparent"
	headerRMQTracestate  = "rabbitmq.header.tracestate"
)

// extractTraceContext extracts trace context from message headers.
// Checks top-level traceparent first, falls back to rabbitmq.header.traceparent.
func extractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	if headers == nil {
		return ctx
	}
	carrier := make(headerCarrier)
	// Try top-level first
	if v, ok := headers[headerTraceparent]; ok {
		carrier[headerTraceparent] = v
		if v, ok := headers[headerTracestate]; ok {
			carrier[headerTracestate] = v
		}
	} else if v, ok := headers[headerRMQTraceparent]; ok {
		// Fallback to rabbitmq.header.* prefix
		carrier[headerTraceparent] = v
		if v, ok := headers[headerRMQTracestate]; ok {
			carrier[headerTracestate] = v
		}
	}
	if carrier[headerTraceparent] == "" {
		return ctx
	}
	prop := propagation.TraceContext{}
	return prop.Extract(ctx, carrier)
}

// injectTraceContext injects trace context into message headers.
func injectTraceContext(ctx context.Context, headers map[string]string) {
	if headers == nil {
		return
	}
	carrier := make(headerCarrier)
	prop := propagation.TraceContext{}
	prop.Inject(ctx, carrier)
	for k, v := range carrier {
		headers[k] = v
	}
}

// Metrics holds OpenTelemetry metric instruments.
type Metrics struct {
	messagesReceived  metric.Int64Counter
	messagesProcessed metric.Int64Counter
	messageErrors     metric.Int64Counter
	messagesDropped   metric.Int64Counter
	commandDuration   metric.Float64Histogram
}

func newMetrics() (*Metrics, error) {
	meter := otel.Meter(tracerName)

	received, err := meter.Int64Counter("simplemq_subscriber.messages.received",
		metric.WithDescription("Messages received from request queue"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.received counter: %w", err)
	}

	processed, err := meter.Int64Counter("simplemq_subscriber.messages.processed",
		metric.WithDescription("Messages successfully processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.processed counter: %w", err)
	}

	errors, err := meter.Int64Counter("simplemq_subscriber.messages.errors",
		metric.WithDescription("Message processing errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.errors counter: %w", err)
	}

	dropped, err := meter.Int64Counter("simplemq_subscriber.messages.dropped",
		metric.WithDescription("Messages dropped due to no matching handler"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.dropped counter: %w", err)
	}

	duration, err := meter.Float64Histogram("simplemq_subscriber.command.duration",
		metric.WithDescription("Command execution duration (seconds)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command.duration histogram: %w", err)
	}

	return &Metrics{
		messagesReceived:  received,
		messagesProcessed: processed,
		messageErrors:     errors,
		messagesDropped:   dropped,
		commandDuration:   duration,
	}, nil
}

func newTracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// setupOTelProviders initializes OpenTelemetry MeterProvider and TracerProvider
// if OTEL_EXPORTER_OTLP_ENDPOINT is set.
func setupOTelProviders(ctx context.Context) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return noop, nil
	}

	protocol := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")

	// Metrics
	var metricExporter sdkmetric.Exporter
	var traceExporter sdktrace.SpanExporter
	var err error

	switch protocol {
	case "grpc":
		metricExporter, err = otlpmetricgrpc.New(ctx)
		if err != nil {
			return noop, fmt.Errorf("failed to create OTLP metrics exporter: %w", err)
		}
		traceExporter, err = otlptracegrpc.New(ctx)
		if err != nil {
			return noop, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
	case "http/protobuf", "":
		metricExporter, err = otlpmetrichttp.New(ctx)
		if err != nil {
			return noop, fmt.Errorf("failed to create OTLP metrics exporter: %w", err)
		}
		traceExporter, err = otlptracehttp.New(ctx)
		if err != nil {
			return noop, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
	default:
		return noop, fmt.Errorf("unsupported OTEL_EXPORTER_OTLP_PROTOCOL: %s", protocol)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("simplemq-subscriber"),
	)

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	otel.SetMeterProvider(meterProvider)

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)
	otel.SetTracerProvider(tracerProvider)

	if protocol == "" {
		protocol = "http/protobuf"
	}
	slog.Info("OpenTelemetry enabled", "protocol", protocol)

	shutdown := func(ctx context.Context) error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown tracer provider", "error", err)
		}
		return meterProvider.Shutdown(ctx)
	}
	return shutdown, nil
}
