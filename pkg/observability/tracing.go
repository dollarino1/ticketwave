package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// TracingDisabled turns tracing off, the same convention METRICS_ADDR uses: the
// config library applies the default for a set-but-empty variable, so a plain
// empty string can never mean "off".
const TracingDisabled = "off"

// SetupTracing registers the global tracer provider and the W3C traceparent
// propagator, which is what lets a span opened in one process continue in another:
// otelgrpc and otelhttp read and write that header automatically once this has run.
//
// With TracingDisabled (or no endpoint), it registers nothing and leaves the
// default no-op provider in place. Code that starts a span still runs — it just
// produces spans nobody records — so a service needs no separate "is tracing on"
// branch anywhere else.
//
// The returned shutdown func flushes any spans still buffered. Call it before the
// process exits, or the last few spans of a graceful shutdown never reach Tempo.
func SetupTracing(ctx context.Context, service, endpoint string) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if endpoint == "" || endpoint == TracingDisabled {
		return noop, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		// Inside the compose network only. Add TLS before this ever crosses a
		// network boundary that isn't trusted.
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return noop, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	// Deliberately not merged with resource.Default(): that resource's schema URL
	// tracks whatever semconv version the SDK build pulled in, which drifts out of
	// sync with a pinned semconv import and makes resource.Merge fail on a schema
	// mismatch. The service name is what every query in Tempo/Grafana keys on, so a
	// resource carrying just that is enough.
	res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(service))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Every request traced. This system's traffic is nowhere near the volume
		// where a sampler (say, TraceIDRatioBased) would earn its cost in lost detail.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// TraceID returns the current span's trace ID as the hex string Tempo and Grafana
// show, or "" if ctx carries no recording span (tracing is off, or nothing upstream
// started one). Logging it is what lets a log line and a trace find each other.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanID returns the current span's ID, or "".
func SpanID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasSpanID() {
		return ""
	}
	return sc.SpanID().String()
}

// ClientTracing is the dial option that propagates the caller's trace to the
// service being dialed and records a client span for each call. Add it to every
// grpc.NewClient that calls one of our own services.
func ClientTracing() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}
