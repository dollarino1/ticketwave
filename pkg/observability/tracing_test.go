package observability

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	noop "go.opentelemetry.io/otel/trace/noop"
)

func TestTraceIDAndSpanID_EmptyWithNoSpan(t *testing.T) {
	if got := TraceID(context.Background()); got != "" {
		t.Errorf("TraceID on a bare context = %q, want empty", got)
	}
	if got := SpanID(context.Background()); got != "" {
		t.Errorf("SpanID on a bare context = %q, want empty", got)
	}
}

func TestTraceIDAndSpanID_EmptyWithANoopSpan(t *testing.T) {
	// The default global tracer (tracing off, or SetupTracing never called with a
	// real endpoint) is a no-op: its spans carry an invalid, all-zero context. That
	// is what makes it safe to always log trace_id/span_id: they are just "" until
	// tracing is actually configured.
	ctx, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "x")
	defer span.End()

	if got := TraceID(ctx); got != "" {
		t.Errorf("TraceID with a no-op span = %q, want empty", got)
	}
	if got := SpanID(ctx); got != "" {
		t.Errorf("SpanID with a no-op span = %q, want empty", got)
	}
}

func TestTraceIDAndSpanID_ReportARealSpan(t *testing.T) {
	tp := sdktrace.NewTracerProvider() // records real, valid span contexts; exports nowhere
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "x")
	defer span.End()

	traceID, spanID := TraceID(ctx), SpanID(ctx)

	if len(traceID) != 32 { // a W3C trace ID is 16 bytes, hex-encoded
		t.Errorf("TraceID = %q, want a 32-character hex string", traceID)
	}
	if len(spanID) != 16 { // a span ID is 8 bytes
		t.Errorf("SpanID = %q, want a 16-character hex string", spanID)
	}
	if span.SpanContext().TraceID().String() != traceID {
		t.Error("TraceID does not match the span actually in the context")
	}
}

// withTestProvider installs a recording tracer provider and the W3C propagator for
// the duration of the test. Tests must not leak this into others: the tracer
// provider is process-global. Cleanup resets to an explicit no-op provider rather
// than whatever GetTracerProvider() returned before the test — OTel's global package
// installs a one-way delegate on the first real SetTracerProvider call in a process,
// and re-installing that captured pre-test value does not reliably undo it (as
// opposed to just always resetting to a fresh, known no-op).
func withTestProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return exporter
}

func TestSetupTracing_OffLeavesTheDefaultProviderInPlace(t *testing.T) {
	before := otel.GetTracerProvider()

	shutdown, err := SetupTracing(t.Context(), "svc", TracingDisabled)
	if err != nil {
		t.Fatalf("SetupTracing(off) returned an error: %v", err)
	}
	if otel.GetTracerProvider() != before {
		t.Error("SetupTracing(off) replaced the global tracer provider")
	}
	if err := shutdown(t.Context()); err != nil {
		t.Errorf("the no-op shutdown returned %v, want nil", err)
	}
}

func TestSetupTracing_EmptyEndpointAlsoMeansOff(t *testing.T) {
	before := otel.GetTracerProvider()

	if _, err := SetupTracing(t.Context(), "svc", ""); err != nil {
		t.Fatalf("SetupTracing(\"\") returned an error: %v", err)
	}
	if otel.GetTracerProvider() != before {
		t.Error("an empty endpoint configured a real provider; only TracingDisabled should count as an explicit choice, but empty must be just as inert")
	}
}

func TestSetupTracing_ConfiguresTheGlobalProviderAndPropagator(t *testing.T) {
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})

	// otlptracegrpc dials lazily, so an unreachable endpoint is not itself an error
	// here: only a real export attempt would fail, and this test makes none.
	shutdown, err := SetupTracing(t.Context(), "svc", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("SetupTracing returned an error: %v", err)
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("tracer provider = %T, want a real *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
	if _, ok := otel.GetTextMapPropagator().(propagation.TraceContext); !ok {
		t.Errorf("propagator = %T, want propagation.TraceContext (W3C traceparent)", otel.GetTextMapPropagator())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown returned %v, want nil even though nothing was exported", err)
	}
}
