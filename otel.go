package subscriber

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

const tracerName = "github.com/fujiwara/mqsubscriber"

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

// extractTraceContextFromEnv extracts W3C trace context from TRACEPARENT/TRACESTATE environment variables.
func extractTraceContextFromEnv(ctx context.Context) context.Context {
	traceparent := os.Getenv("TRACEPARENT")
	if traceparent == "" {
		return ctx
	}
	carrier := make(headerCarrier)
	carrier[headerTraceparent] = traceparent
	if tracestate := os.Getenv("TRACESTATE"); tracestate != "" {
		carrier[headerTracestate] = tracestate
	}
	return propagation.TraceContext{}.Extract(ctx, carrier)
}

// injectTraceContext injects trace context into message headers.
func injectTraceContext(ctx context.Context, headers map[string]string) {
	if headers == nil {
		return
	}
	carrier := make(headerCarrier)
	prop := propagation.TraceContext{}
	prop.Inject(ctx, carrier)
	maps.Copy(headers, carrier)
}

// headerAttributes converts message headers to span attributes with the given prefix.
func headerAttributes(prefix string, headers map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(headers))
	for k, v := range headers {
		attrs = append(attrs, attribute.String(prefix+k, v))
	}
	return attrs
}

// Metrics holds OpenTelemetry metric instruments.
type Metrics struct {
	messagesReceived  metric.Int64Counter
	messagesProcessed metric.Int64Counter
	messageErrors     metric.Int64Counter
	messagesDropped   metric.Int64Counter
	messagesUnmatched metric.Int64Counter
	commandDuration   metric.Float64Histogram
	commandTimeouts   metric.Int64Counter
}

func newMetrics() (*Metrics, error) {
	meter := otel.Meter(tracerName)

	received, err := meter.Int64Counter("mqsubscriber.messages.received",
		metric.WithDescription("Messages received from request queue"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.received counter: %w", err)
	}

	processed, err := meter.Int64Counter("mqsubscriber.messages.processed",
		metric.WithDescription("Messages successfully processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.processed counter: %w", err)
	}

	errors, err := meter.Int64Counter("mqsubscriber.messages.errors",
		metric.WithDescription("Message processing errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.errors counter: %w", err)
	}

	dropped, err := meter.Int64Counter("mqsubscriber.messages.dropped",
		metric.WithDescription("Messages dropped (acked) due to no matching handler (drop_unmatched: true)"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.dropped counter: %w", err)
	}

	unmatched, err := meter.Int64Counter("mqsubscriber.messages.unmatched",
		metric.WithDescription("Messages nacked due to no matching handler (drop_unmatched: false)"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messages.unmatched counter: %w", err)
	}

	duration, err := meter.Float64Histogram("mqsubscriber.command.duration",
		metric.WithDescription("Command execution duration (seconds)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command.duration histogram: %w", err)
	}

	timeouts, err := meter.Int64Counter("mqsubscriber.command.timeouts",
		metric.WithDescription("Command execution timeouts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create command.timeouts counter: %w", err)
	}

	return &Metrics{
		messagesReceived:  received,
		messagesProcessed: processed,
		messageErrors:     errors,
		messagesDropped:   dropped,
		messagesUnmatched: unmatched,
		commandDuration:   duration,
		commandTimeouts:   timeouts,
	}, nil
}

// initCounters initializes all counters to zero so they appear in metrics output
// even before any events occur. Handler-specific counters are initialized per handler.
func (m *Metrics) initCounters(ctx context.Context, handlers []*Handler) {
	m.messagesReceived.Add(ctx, 0)
	m.messagesDropped.Add(ctx, 0)
	m.messagesUnmatched.Add(ctx, 0)
	for _, h := range handlers {
		opts := metric.WithAttributeSet(h.attrs)
		m.messagesProcessed.Add(ctx, 0, opts)
		m.messageErrors.Add(ctx, 0, opts)
		m.commandTimeouts.Add(ctx, 0, opts)
	}
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
		semconv.ServiceName("mqsubscriber"),
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
	slog.Debug("OpenTelemetry enabled", "protocol", protocol)

	shutdown := func(ctx context.Context) error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown tracer provider", "error", err)
		}
		return meterProvider.Shutdown(ctx)
	}
	return shutdown, nil
}
