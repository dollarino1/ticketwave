package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/dollarino1/ticketwave/pkg/ratelimit"
)

func TestHealthz(t *testing.T) {
	h := newHarness(t)

	rec := h.do("GET", "/healthz", "")

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestUnknownRoutesGetJSON404(t *testing.T) {
	h := newHarness(t)

	rec := h.do("GET", "/api/nope", "")

	if rec.Code != http.StatusNotFound || errCode(t, rec) != "not_found" {
		t.Errorf("got %d %q, want a JSON 404", rec.Code, errCode(t, rec))
	}
}

func TestSecurityHeaders(t *testing.T) {
	h := newHarness(t)

	rec := h.do("GET", "/healthz", "")

	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
	if got := rec.Header().Get("Referrer-Policy"); got != "no-referrer" {
		t.Errorf("Referrer-Policy = %q", got)
	}
}

func TestRequestID(t *testing.T) {
	cases := map[string]struct {
		sent     string
		wantSame bool
	}{
		"a plain ID is kept":                {"abc-123_XYZ", true},
		"none sent gets a fresh one":        {"", false},
		"newline (log forging) is replaced": {"abc\nlevel=ERROR forged", false},
		"spaces are replaced":               {"has space", false},
		"an over-long ID is replaced":       {strings.Repeat("a", 65), false},
		"the longest allowed ID is kept":    {strings.Repeat("a", 64), true},
		"punctuation in an ID is replaced":  {`a"b`, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			var headers []string
			if tc.sent != "" {
				headers = []string{"X-Request-ID", tc.sent}
			}

			rec := h.do("GET", "/healthz", "", headers...)

			got := rec.Header().Get("X-Request-ID")
			if got == "" {
				t.Fatal("no X-Request-ID on the response")
			}
			if (got == tc.sent) != tc.wantSame {
				t.Errorf("sent %q, got back %q", tc.sent, got)
			}
		})
	}
}

func TestRequestLogLineCarriesTheRequestIDAndUser(t *testing.T) {
	h := newHarness(t)
	stubOrder(h, userID)

	h.do("GET", "/api/orders/"+eventID, "", append(bearer("user-token"), "X-Request-ID", "req-42")...)

	logs := h.logs.String()
	if !strings.Contains(logs, `"request_id":"req-42"`) || !strings.Contains(logs, `"user_id":"`+userID+`"`) {
		t.Errorf("the access log lacks the request ID or the user:\n%s", logs)
	}
}

func TestAccessLogNeverContainsSecrets(t *testing.T) {
	h := newHarness(t)
	h.auth.login = loggedIn

	h.do("POST", "/api/auth/login", `{"email":"ann@example.com","password":"hunter2-secret"}`, "Authorization", "Bearer user-token")

	logs := h.logs.String()
	for _, secret := range []string{"hunter2-secret", refreshSecret, "access-token", "user-token", "ann@example.com"} {
		if strings.Contains(logs, secret) {
			t.Errorf("the logs contain %q:\n%s", secret, logs)
		}
	}
}

func TestPanicBecomesA500WithoutLeakingAndIsLogged(t *testing.T) {
	h := newHarness(t)
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		panic("boom: secret internal state")
	}

	rec := h.do("GET", "/api/events", "")

	if rec.Code != http.StatusInternalServerError || errCode(t, rec) != "internal" {
		t.Errorf("got %d %q, want 500 internal", rec.Code, errCode(t, rec))
	}
	if strings.Contains(rec.Body.String(), "secret internal state") {
		t.Error("the panic value reached the client")
	}
	logs := h.logs.String()
	if !strings.Contains(logs, "panic in handler") || !strings.Contains(logs, "secret internal state") {
		t.Error("the panic was not logged with its value")
	}
	if !strings.Contains(logs, `"status":500`) {
		t.Error("the access log did not record the 500")
	}
}

func TestRateLimit_AnswersWith429AndRetryAfter(t *testing.T) {
	h := newHarness(t)
	h.limiter.deny = func(string) bool { return true }

	rec := h.do("GET", "/api/events", "") // inventory-svc must not be called

	if rec.Code != http.StatusTooManyRequests || errCode(t, rec) != "rate_limited" {
		t.Fatalf("got %d %q, want 429 rate_limited", rec.Code, errCode(t, rec))
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want 2 (1.5s rounded up)", got)
	}
}

func TestRateLimit_RetryAfterIsAtLeastOneSecond(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/x", nil)

	a := &API{limiter: zeroReset{}, log: slog.New(slog.DiscardHandler)}
	if a.allow(rec, req, "k", 1, time.Minute) {
		t.Fatal("request was allowed")
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1, never 0", got)
	}
}

// zeroReset refuses every request and says the window has already ended.
type zeroReset struct{}

func (zeroReset) Allow(context.Context, string, int, time.Duration) (ratelimit.Result, error) {
	return ratelimit.Result{Allowed: false}, nil
}

func TestRateLimit_FailsOpenWhenRedisIsDown(t *testing.T) {
	h := newHarness(t)
	h.limiter.err = errors.New("redis: connection refused")
	reached := false
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		reached = true
		return &inventoryv1.ListEventsResponse{}, nil
	}

	rec := h.do("GET", "/api/events", "")

	if rec.Code != http.StatusOK || !reached {
		t.Errorf("status = %d, handler reached = %v; a Redis outage must not become a site outage", rec.Code, reached)
	}
	if !strings.Contains(h.logs.String(), "rate limiter unavailable") {
		t.Error("the limiter failure was not logged")
	}
}

func TestRateLimit_OrdersAreCountedPerUserNotPerAddress(t *testing.T) {
	h := newHarness(t)
	prepare(h)

	h.do("POST", "/api/orders", orderBody(seatA), bearer("user-token")...)
	h.do("POST", "/api/orders", orderBody(seatA), bearer("other-token")...)

	if !h.limiter.seen("orders:user:"+userID) || !h.limiter.seen("orders:user:"+otherID) {
		t.Errorf("keys = %v, want one counter per user, so users behind one office NAT do not share a budget", h.limiter.keys)
	}
}

func TestClientIP(t *testing.T) {
	cases := map[string]struct {
		remote string
		header string
		trust  bool
		want   string
	}{
		"direct connection":               {"203.0.113.9:5555", "", false, "203.0.113.9"},
		"header ignored when not trusted": {"203.0.113.9:5555", "198.51.100.1", false, "203.0.113.9"},
		"header used behind our proxy":    {"10.0.0.2:5555", "198.51.100.1", true, "198.51.100.1"},
		"garbage header is ignored":       {"10.0.0.2:5555", "not-an-ip", true, "10.0.0.2"},
		"missing header falls back":       {"10.0.0.2:5555", "", true, "10.0.0.2"},
		"ipv6":                            {"[2001:db8::1]:5555", "", false, "2001:db8::1"},
		"remote without a port":           {"weird", "", false, "weird"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), "GET", "/", nil)
			req.RemoteAddr = tc.remote
			if tc.header != "" {
				req.Header.Set("X-Real-IP", tc.header)
			}
			if got := clientIP(req, tc.trust); got != tc.want {
				t.Errorf("clientIP = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHashKey(t *testing.T) {
	if hashKey("Ann@Example.com") != hashKey("  ann@example.COM") {
		t.Error("case and surrounding spaces must not change the key")
	}
	if hashKey("a@x.co") == hashKey("b@x.co") {
		t.Error("different accounts share a key")
	}
	if got := hashKey("a@x.co"); len(got) != 16 || strings.Contains(got, "@") {
		t.Errorf("hashKey = %q, want 16 hex characters", got)
	}
}
