package httpapi

import (
	"net/http"
	"strconv"
	"testing"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// find returns the metric in family `name` whose labels include all of `want`.
func find(t *testing.T, name string, want map[string]string) *dto.Metric {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
				}
			}
			if match {
				return m
			}
		}
	}
	return nil
}

func counter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	if m := find(t, name, labels); m != nil {
		return m.GetCounter().GetValue()
	}
	return 0
}

func histogramCount(t *testing.T, name string, labels map[string]string) uint64 {
	t.Helper()
	if m := find(t, name, labels); m != nil {
		return m.GetHistogram().GetSampleCount()
	}
	return 0
}

func TestMetrics_CountRequestsByRoutePatternMethodAndStatus(t *testing.T) {
	labels := map[string]string{"route": "GET /api/events/{id}", "method": "GET", "code": "404"}
	before := counter(t, "ticketwave_http_requests_total", labels)
	h := newHarness(t)
	h.inventory.getEvent = func(*inventoryv1.GetEventRequest) (*inventoryv1.GetEventResponse, error) {
		return nil, notFoundErr()
	}

	h.do("GET", "/api/events/"+eventID, "")
	h.do("GET", "/api/events/"+otherID, "") // a different path, the same route

	if got := counter(t, "ticketwave_http_requests_total", labels) - before; got != 2 {
		t.Errorf("counted %v requests for the route, want 2: the label must be the pattern, not the path", got)
	}
	if find(t, "ticketwave_http_requests_total", map[string]string{"route": "GET /api/events/" + eventID}) != nil {
		t.Error("a raw path became a label value: unbounded cardinality")
	}
}

func TestMetrics_RecordLatencyPerRoute(t *testing.T) {
	labels := map[string]string{"route": "GET /healthz", "method": "GET"}
	before := histogramCount(t, "ticketwave_http_request_duration_seconds", labels)
	h := newHarness(t)

	h.do("GET", "/healthz", "")

	if got := histogramCount(t, "ticketwave_http_request_duration_seconds", labels) - before; got != 1 {
		t.Errorf("recorded %d latency samples, want 1", got)
	}
}

// Requests the router cannot place must not mint a series per URL an attacker tries.
func TestMetrics_UnknownPathsShareOneLabel(t *testing.T) {
	labels := map[string]string{"route": unmatched, "code": "404"}
	before := counter(t, "ticketwave_http_requests_total", labels)
	h := newHarness(t)

	for i := 0; i < 5; i++ {
		h.do("GET", "/scan/"+strconv.Itoa(i)+"/wp-admin", "")
	}

	if got := counter(t, "ticketwave_http_requests_total", labels) - before; got != 5 {
		t.Errorf("counted %v unmatched requests, want 5 under a single label", got)
	}
	if find(t, "ticketwave_http_requests_total", map[string]string{"route": "/scan/0/wp-admin"}) != nil {
		t.Error("an attacker-chosen path became a label value")
	}
}

// A request refused before it reaches its handler is still traffic worth seeing.
func TestMetrics_CountRequestsRefusedByAuth(t *testing.T) {
	labels := map[string]string{"route": "POST /api/orders", "code": "401"}
	before := counter(t, "ticketwave_http_requests_total", labels)
	h := newHarness(t)

	h.do("POST", "/api/orders", `{}`)

	if got := counter(t, "ticketwave_http_requests_total", labels) - before; got != 1 {
		t.Errorf("counted %v refused requests, want 1: metrics must sit outside auth", got)
	}
}

func TestMetrics_CountRateLimitRejectionsByWhichLimitWasHit(t *testing.T) {
	loginBefore := counter(t, "ticketwave_ratelimit_rejections_total", map[string]string{"limit": "login-email"})
	h := newHarness(t)
	h.limiter.deny = func(key string) bool { return len(key) > 11 && key[:11] == "login-email" }

	h.do("POST", "/api/auth/login", loginBody)

	if got := counter(t, "ticketwave_ratelimit_rejections_total", map[string]string{"limit": "login-email"}) - loginBefore; got != 1 {
		t.Errorf("counted %v login-email rejections, want 1", got)
	}
	// The label is the limit's name, never the key, which contains a hashed email.
	if find(t, "ticketwave_ratelimit_rejections_total", map[string]string{"limit": "login-email:" + hashKey("Ann@Example.com")}) != nil {
		t.Error("a per-user rate-limit key became a label value")
	}
}

func TestLimitName(t *testing.T) {
	cases := map[string]string{
		"login-email:ab12cd34": "login-email",
		"orders:user:1234":     "orders",
		"browse:203.0.113.9":   "browse",
		"nocolon":              "nocolon",
	}
	for key, want := range cases {
		if got := limitName(key); got != want {
			t.Errorf("limitName(%q) = %q, want %q", key, got, want)
		}
	}
}

// The request ID the gateway logs must be the one the services receive.
func TestRequestID_IsForwardedToDownstreamServices(t *testing.T) {
	h := newHarness(t)
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		return &inventoryv1.ListEventsResponse{}, nil
	}

	rec := h.do("GET", "/api/events", "", "X-Request-ID", "req-from-nginx")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := observability.RequestID(h.inventory.lastCtx); got != "req-from-nginx" {
		t.Errorf("the downstream call carried request ID %q, want req-from-nginx", got)
	}
}
