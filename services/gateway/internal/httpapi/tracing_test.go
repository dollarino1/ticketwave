package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

// withTracing installs a recording tracer provider and the W3C propagator for the
// duration of the test. The provider is process-global, so a leaked one would affect
// every other test in the package: cleanup resets to an explicit no-op provider
// rather than whatever GetTracerProvider() returned before, because OTel's global
// package installs a one-way delegate on the first real SetTracerProvider call, and
// re-installing that captured pre-test value does not reliably undo it.
func withTracing(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return exporter
}

// logLines parses the harness's captured JSON log output, one object per line.
func (h *harness) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func TestTracing_TheRequestGetsARootSpanNamedAfterItsRoute(t *testing.T) {
	exporter := withTracing(t)
	h := newHarness(t)
	h.inventory.getEvent = func(*inventoryv1.GetEventRequest) (*inventoryv1.GetEventResponse, error) {
		return &inventoryv1.GetEventResponse{Event: &inventoryv1.Event{Id: eventID, Name: "x"}}, nil
	}

	rec := h.do("GET", "/api/events/"+eventID, "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d span(s), want exactly 1 for the request", len(spans))
	}
	if got := spans[0].Name; got != "GET /api/events/{id}" {
		t.Errorf("span name = %q, want the route pattern GET /api/events/{id}", got)
	}
}

func TestTracing_ARefusedRequestStillCarriesItsRouteName(t *testing.T) {
	exporter := withTracing(t)
	h := newHarness(t) // no fake prepared: authenticate must refuse before any handler runs

	rec := h.do("POST", "/api/orders", `{}`)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "POST /api/orders" {
		t.Errorf("spans = %+v, want one span named POST /api/orders", spans)
	}
}

func TestTracing_AnUnmatchedPathGetsTheSharedUnmatchedName(t *testing.T) {
	exporter := withTracing(t)
	h := newHarness(t)

	h.do("GET", "/nothing/here", "")

	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != unmatched {
		t.Errorf("spans = %+v, want one span named %q, not the raw path (unbounded cardinality)", spans, unmatched)
	}
}

// The request log line must be able to find its own span, or trace_id in the log is
// useless: it would name a span that never actually ran.
func TestTracing_TheRequestLogLineCarriesTheSpansTraceID(t *testing.T) {
	exporter := withTracing(t)
	h := newHarness(t)

	h.do("GET", "/healthz", "")

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d span(s), want 1", len(spans))
	}
	wantTraceID, wantSpanID := spans[0].SpanContext.TraceID().String(), spans[0].SpanContext.SpanID().String()

	var found bool
	for _, l := range h.logLines(t) {
		if l["msg"] == "request" {
			if l["trace_id"] != wantTraceID {
				t.Errorf("request log trace_id = %v, want %s (the span actually recorded)", l["trace_id"], wantTraceID)
			}
			if l["span_id"] != wantSpanID {
				t.Errorf("request log span_id = %v, want %s", l["span_id"], wantSpanID)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no request log line was produced")
	}
}

// With no tracer configured, tracing must be a true no-op: no crash, and no value
// that looks like a real trace ID in the logs.
func TestTracing_WithNoTracerConfiguredTheRequestStillWorksAndLogsNoTraceID(t *testing.T) {
	h := newHarness(t) // no withTracing: whatever the global default is (untouched)

	rec := h.do("GET", "/healthz", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var found bool
	for _, l := range h.logLines(t) {
		if l["msg"] == "request" {
			if l["trace_id"] != "" {
				t.Errorf("trace_id = %v with no tracer configured, want empty", l["trace_id"])
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no request log line was produced")
	}
}
