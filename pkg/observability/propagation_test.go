package observability

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func spanNamed(spans tracetest.SpanStubs, kind trace.SpanKind) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].SpanKind == kind {
			return &spans[i]
		}
	}
	return nil
}

// This is the property the whole feature exists for: a span opened by a CLIENT dial
// with ClientTracing, and the span the SERVER (built by NewGRPCServer, which always
// carries the otelgrpc stats handler) opens to handle that call, must be part of the
// SAME trace — and the logging interceptor must report that trace ID in its log line.
// Without this, a request could not actually be followed across a service boundary;
// everything else in this package would be instrumenting in a vacuum.
//
// The chain a gRPC call actually produces is: caller's span (root here) -> an
// otelgrpc CLIENT span for the call -> an otelgrpc SERVER span handling it. All three
// share one trace ID, each a child of the one before.
func TestPropagation_ClientAndServerSpansShareOneTraceAndTheLogAgrees(t *testing.T) {
	exporter := withTestProvider(t)
	r := newRig(t, ClientTracing())

	ctx, rootSpan := otel.Tracer("test").Start(t.Context(), "test-root")
	if err := r.call(ctx); err != nil {
		t.Fatal(err)
	}
	rootSpan.End()

	spans := exporter.GetSpans()
	clientSpan := spanNamed(spans, trace.SpanKindClient)
	serverSpan := spanNamed(spans, trace.SpanKindServer)
	if clientSpan == nil || serverSpan == nil {
		t.Fatalf("got %d span(s), want a client span and a server span: %+v", len(spans), spans)
	}

	rootID := rootSpan.SpanContext().TraceID()
	if clientSpan.SpanContext.TraceID() != rootID || serverSpan.SpanContext.TraceID() != rootID {
		t.Errorf("trace IDs: root=%s client=%s server=%s, want all three equal",
			rootID, clientSpan.SpanContext.TraceID(), serverSpan.SpanContext.TraceID())
	}
	if clientSpan.Parent.SpanID() != rootSpan.SpanContext().SpanID() {
		t.Error("the client span is not a child of the caller's span")
	}
	if serverSpan.Parent.SpanID() != clientSpan.SpanContext.SpanID() {
		t.Error("the server span is not a child of the client span: the trace did not cross the gRPC call")
	}

	wantTraceID, wantSpanID := rootID.String(), serverSpan.SpanContext.SpanID().String()
	found := false
	for _, l := range r.logLines(t) {
		if l["msg"] == "rpc" {
			if l["trace_id"] != wantTraceID {
				t.Errorf("the rpc log line has trace_id %v, want %s", l["trace_id"], wantTraceID)
			}
			if l["span_id"] != wantSpanID {
				t.Errorf("the rpc log line has span_id %v, want %s (the server span's own ID)", l["span_id"], wantSpanID)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no rpc log line was produced")
	}
}
