package httpapi

import (
	"net/http"
	"strings"
	"testing"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
)

// protectedRoutes lists every route that needs a sign-in, and the minimum role.
var protectedRoutes = []struct {
	name   string
	method string
	path   string
	body   string
	role   string // "user" or "organizer"
}{
	{"place an order", "POST", "/api/orders", `{"event_id":"` + eventID + `","seat_ids":["` + seatA + `"]}`, "user"},
	{"view an order", "GET", "/api/orders/" + eventID, "", "user"},
	{"create an event", "POST", "/api/events", `{"name":"Jazz","seat_count":10}`, "organizer"},
	{"view analytics", "GET", "/api/analytics/events", "", "organizer"},
}

// prepare gives every downstream fake a harmless success, so that a request
// which is allowed through gets a normal answer.
func prepare(h *harness) {
	h.orders.createOrder = func(*orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
		return &orderv1.CreateOrderResponse{OrderId: eventID, Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED}, nil
	}
	h.orders.getOrder = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return &orderv1.GetOrderResponse{OrderId: eventID, UserId: userID}, nil
	}
	h.inventory.createEvent = func(*inventoryv1.CreateEventRequest) (*inventoryv1.CreateEventResponse, error) {
		return &inventoryv1.CreateEventResponse{EventId: eventID}, nil
	}
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		return &inventoryv1.ListEventsResponse{}, nil
	}
	h.analytics.listEventStats = func(*analyticsv1.ListEventStatsRequest) (*analyticsv1.ListEventStatsResponse, error) {
		return &analyticsv1.ListEventStatsResponse{}, nil
	}
}

func TestProtectedRoutes_RejectRequestsWithoutAValidToken(t *testing.T) {
	credentials := map[string][]string{
		"no header":            nil,
		"wrong scheme":         {"Authorization", "Basic dXNlcjpwYXNz"},
		"scheme without token": {"Authorization", "Bearer"},
		"empty token":          {"Authorization", "Bearer "},
		"unknown token":        {"Authorization", "Bearer not-a-real-token"},
	}
	for _, route := range protectedRoutes {
		for name, headers := range credentials {
			t.Run(route.name+"/"+name, func(t *testing.T) {
				h := newHarness(t) // no downstream fake is prepared: none may be called

				rec := h.do(route.method, route.path, route.body, headers...)

				if rec.Code != http.StatusUnauthorized || errCode(t, rec) != "unauthenticated" {
					t.Fatalf("got %d %q, want 401 unauthenticated", rec.Code, errCode(t, rec))
				}
				if rec.Header().Get("WWW-Authenticate") == "" {
					t.Error("no WWW-Authenticate header on a 401")
				}
			})
		}
	}
}

// Why a token was refused (expired, forged, wrong algorithm) helps only someone
// forging tokens, so it must appear in the logs and never in the response.
func TestAuthenticate_DoesNotExplainWhyATokenWasRefused(t *testing.T) {
	h := newHarness(t)

	rec := h.do("GET", "/api/orders/"+eventID, "", bearer("expired-or-forged")...)

	if strings.Contains(rec.Body.String(), "expired") || strings.Contains(rec.Body.String(), "2020") {
		t.Errorf("the response explains the rejection: %s", rec.Body.String())
	}
	if !strings.Contains(h.logs.String(), "token rejected") {
		t.Error("the rejection reason was not logged")
	}
}

func TestRoles_UsersCannotReachOrganizerRoutes(t *testing.T) {
	for _, route := range protectedRoutes {
		if route.role != "organizer" {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			h := newHarness(t) // the downstream fake is not prepared: it must not be reached

			rec := h.do(route.method, route.path, route.body, bearer("user-token")...)

			if rec.Code != http.StatusForbidden || errCode(t, rec) != "permission_denied" {
				t.Errorf("got %d %q, want 403 permission_denied", rec.Code, errCode(t, rec))
			}
		})
	}
}

func TestRoles_AnUnknownRoleInAValidTokenGetsNothing(t *testing.T) {
	// The token verifies, but its role claim is one nobody issues.
	for _, route := range protectedRoutes {
		if route.role != "organizer" {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			h := newHarness(t)

			rec := h.do(route.method, route.path, route.body, bearer("forged-token")...)

			if rec.Code != http.StatusForbidden {
				t.Errorf("status = %d, want 403: an unrecognised role must fail closed", rec.Code)
			}
		})
	}
}

func TestRoles_OrganizersAndAdminsMayUseOrganizerRoutes(t *testing.T) {
	for _, token := range []string{"organizer-token", "admin-token"} {
		for _, route := range protectedRoutes {
			if route.role != "organizer" {
				continue // reading a customer's order is a separate rule, tested in orders_test.go
			}
			t.Run(token+"/"+route.name, func(t *testing.T) {
				h := newHarness(t)
				prepare(h)

				rec := h.do(route.method, route.path, route.body, bearer(token)...)

				if rec.Code >= 400 {
					t.Errorf("status = %d (%s), want the request to be allowed", rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestRoles_OrdinaryUsersMayUseTheUserRoutes(t *testing.T) {
	for _, route := range protectedRoutes {
		if route.role != "user" {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			h := newHarness(t)
			prepare(h)

			rec := h.do(route.method, route.path, route.body, bearer("user-token")...)

			if rec.Code >= 400 {
				t.Errorf("status = %d (%s), want the request to be allowed", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestBearerToken(t *testing.T) {
	cases := map[string]struct {
		header string
		token  string
		ok     bool
	}{
		"normal":             {"Bearer abc.def", "abc.def", true},
		"scheme is any case": {"bearer abc", "abc", true},
		"extra spaces":       {"Bearer   abc  ", "abc", true},
		"missing":            {"", "", false},
		"other scheme":       {"Basic abc", "", false},
		"no token":           {"Bearer", "", false},
		"blank token":        {"Bearer    ", "", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			req, _ := http.NewRequestWithContext(t.Context(), "GET", "/", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			token, ok := bearerToken(req)
			if token != tc.token || ok != tc.ok {
				t.Errorf("bearerToken(%q) = %q, %v; want %q, %v", tc.header, token, ok, tc.token, tc.ok)
			}
		})
	}
}
