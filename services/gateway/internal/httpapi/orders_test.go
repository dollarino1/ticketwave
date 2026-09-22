package httpapi

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	apierr "github.com/dollarino1/ticketwave/pkg/apierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func orderBody(seats ...string) string {
	quoted := make([]string, len(seats))
	for i, s := range seats {
		quoted[i] = `"` + s + `"`
	}
	return `{"event_id":"` + eventID + `","seat_ids":[` + strings.Join(quoted, ",") + `]}`
}

func TestCreateOrder_TrustsTheTokenAndComputesThePrice(t *testing.T) {
	h := newHarness(t)
	var got *orderv1.CreateOrderRequest
	h.orders.createOrder = func(in *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
		got = in
		return &orderv1.CreateOrderResponse{OrderId: eventID, Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED}, nil
	}

	rec := h.do("POST", "/api/orders", orderBody(seatA, seatB, seatC), bearer("user-token")...)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	if got.UserId != userID {
		t.Errorf("order-svc was told the buyer is %q, want the token's user %q", got.UserId, userID)
	}
	if got.AmountCents != 3*5000 {
		t.Errorf("amount = %d, want 15000 (3 seats x 5000)", got.AmountCents)
	}
	if got.EventId != eventID || len(got.SeatIds) != 3 {
		t.Errorf("request = %v", got)
	}
	if v := decode[orderView](t, rec); v.Status != "confirmed" || v.OrderID != eventID {
		t.Errorf("body = %+v", v)
	}
}

func TestCreateOrder_TheClientCannotNameItsOwnUserOrPrice(t *testing.T) {
	forged := map[string]string{
		"another user": `{"event_id":"` + eventID + `","seat_ids":["` + seatA + `"],"user_id":"` + otherID + `"}`,
		"a free order": `{"event_id":"` + eventID + `","seat_ids":["` + seatA + `"],"amount_cents":0}`,
	}
	for name, body := range forged {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t) // order-svc must not be called

			rec := h.do("POST", "/api/orders", body, bearer("user-token")...)

			if rec.Code != http.StatusBadRequest || errCode(t, rec) != "invalid_json" {
				t.Errorf("got %d %q, want 400 invalid_json", rec.Code, errCode(t, rec))
			}
		})
	}
}

func TestCreateOrder_SeatCountLimits(t *testing.T) {
	seats := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		}
		return out
	}
	cases := []struct {
		n    int
		want int
	}{{0, 400}, {1, 201}, {10, 201}, {11, 400}, {1000, 400}}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d seats", tc.n), func(t *testing.T) {
			h := newHarness(t)
			if tc.want == 201 {
				h.orders.createOrder = func(*orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
					return &orderv1.CreateOrderResponse{OrderId: eventID}, nil
				}
			}

			rec := h.do("POST", "/api/orders", orderBody(seats(tc.n)...), bearer("user-token")...)

			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestCreateOrder_PriceFollowsConfiguration(t *testing.T) {
	h := newHarness(t, func(d *Deps) { d.SeatPriceCents = 1234 })
	var got int64
	h.orders.createOrder = func(in *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
		got = in.AmountCents
		return &orderv1.CreateOrderResponse{OrderId: eventID}, nil
	}

	h.do("POST", "/api/orders", orderBody(seatA, seatB), bearer("user-token")...)

	if got != 2468 {
		t.Errorf("amount = %d, want 2468", got)
	}
}

func TestCreateOrder_MapsSagaFailures(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
		code string
	}{
		"seats taken":      {apierr.New(codes.FailedPrecondition, apierr.ReasonSeatsUnavailable, "internal detail"), 409, "seats_unavailable"},
		"card declined":    {apierr.New(codes.FailedPrecondition, apierr.ReasonPaymentDeclined, "internal detail"), 402, "payment_declined"},
		"a service is out": {apierr.New(codes.Unavailable, apierr.ReasonServiceUnavailable, "internal detail"), 503, "service_unavailable"},
		"order-svc crash":  {status.Error(codes.Internal, "pq: deadlock detected"), 500, "internal"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.orders.createOrder = func(*orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) { return nil, tc.err }

			rec := h.do("POST", "/api/orders", orderBody(seatA), bearer("user-token")...)

			if rec.Code != tc.want || errCode(t, rec) != tc.code {
				t.Errorf("got %d %q, want %d %q", rec.Code, errCode(t, rec), tc.want, tc.code)
			}
			if strings.Contains(rec.Body.String(), "internal detail") || strings.Contains(rec.Body.String(), "pq:") {
				t.Errorf("internal message leaked: %s", rec.Body.String())
			}
		})
	}
}

func stubOrder(h *harness, owner string) {
	h.orders.getOrder = func(in *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return &orderv1.GetOrderResponse{
			OrderId: in.OrderId, UserId: owner, EventId: eventID, SeatIds: []string{seatA},
			AmountCents: 5000, Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED,
		}, nil
	}
}

func TestGetOrder_OwnerSeesTheirOrder(t *testing.T) {
	h := newHarness(t)
	stubOrder(h, userID)

	rec := h.do("GET", "/api/orders/"+eventID, "", bearer("user-token")...)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	v := decode[orderView](t, rec)
	if v.Status != "confirmed" || v.AmountCents != 5000 || len(v.SeatIDs) != 1 {
		t.Errorf("order = %+v", v)
	}
}

// A 403 would confirm the ID exists; a 404 gives an enumerator nothing.
func TestGetOrder_SomeoneElsesOrderLooksMissing(t *testing.T) {
	h := newHarness(t)
	stubOrder(h, userID)

	rec := h.do("GET", "/api/orders/"+eventID, "", bearer("other-token")...)

	if rec.Code != http.StatusNotFound || errCode(t, rec) != "not_found" {
		t.Errorf("got %d %q, want 404 not_found", rec.Code, errCode(t, rec))
	}
	if strings.Contains(rec.Body.String(), userID) || strings.Contains(rec.Body.String(), "5000") {
		t.Errorf("the response leaks the order: %s", rec.Body.String())
	}
}

func TestGetOrder_MissingAndForeignOrdersAreIndistinguishable(t *testing.T) {
	missing := newHarness(t)
	missing.orders.getOrder = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	foreign := newHarness(t)
	stubOrder(foreign, userID)

	a := missing.do("GET", "/api/orders/"+eventID, "", bearer("other-token")...)
	b := foreign.do("GET", "/api/orders/"+eventID, "", bearer("other-token")...)

	if a.Code != b.Code || a.Body.String() != b.Body.String() {
		t.Errorf("an attacker can tell them apart:\n missing: %d %s\n foreign: %d %s", a.Code, a.Body, b.Code, b.Body)
	}
}

func TestGetOrder_AdminsMaySeeAnyOrder(t *testing.T) {
	h := newHarness(t)
	stubOrder(h, userID)

	rec := h.do("GET", "/api/orders/"+eventID, "", bearer("admin-token")...)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for an admin", rec.Code)
	}
}

func TestGetOrder_OrganizersDoNotGetAdminPowers(t *testing.T) {
	h := newHarness(t)
	stubOrder(h, userID)

	rec := h.do("GET", "/api/orders/"+eventID, "", bearer("organizer-token")...)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404: organizers run events, they do not read customers' orders", rec.Code)
	}
}

func TestOrderStatusName(t *testing.T) {
	cases := map[orderv1.OrderStatus]string{
		orderv1.OrderStatus_ORDER_STATUS_PENDING:     "pending",
		orderv1.OrderStatus_ORDER_STATUS_CONFIRMED:   "confirmed",
		orderv1.OrderStatus_ORDER_STATUS_FAILED:      "failed",
		orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED: "unknown",
	}
	for in, want := range cases {
		if got := orderStatusName(in); got != want {
			t.Errorf("orderStatusName(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestCreateOrder_ForwardsTheIdempotencyKey(t *testing.T) {
	h := newHarness(t)
	var got *orderv1.CreateOrderRequest
	h.orders.createOrder = func(in *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
		got = in
		return &orderv1.CreateOrderResponse{OrderId: eventID}, nil
	}

	h.do("POST", "/api/orders", orderBody(seatA), append(bearer("user-token"), "Idempotency-Key", "checkout-7f3a")...)

	if got.IdempotencyKey != "checkout-7f3a" {
		t.Errorf("order-svc received key %q, want checkout-7f3a", got.IdempotencyKey)
	}
}

func TestCreateOrder_TheKeyIsOptional(t *testing.T) {
	h := newHarness(t)
	var got *orderv1.CreateOrderRequest
	h.orders.createOrder = func(in *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
		got = in
		return &orderv1.CreateOrderResponse{OrderId: eventID}, nil
	}

	rec := h.do("POST", "/api/orders", orderBody(seatA), bearer("user-token")...)

	if rec.Code != http.StatusCreated || got.IdempotencyKey != "" {
		t.Errorf("status %d, key %q: a request without a key must work and carry none", rec.Code, got.IdempotencyKey)
	}
}

func TestCreateOrder_AMalformedKeyIsRefusedAtTheEdge(t *testing.T) {
	for name, key := range map[string]string{
		"too long":    strings.Repeat("a", 65),
		"a space":     "has space",
		"a slash":     "a/b",
		"a semicolon": "a;b",
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t) // order-svc must not be called

			rec := h.do("POST", "/api/orders", orderBody(seatA), append(bearer("user-token"), "Idempotency-Key", key)...)

			if rec.Code != http.StatusBadRequest || errCode(t, rec) != "invalid_argument" {
				t.Errorf("got %d %q, want 400 invalid_argument", rec.Code, errCode(t, rec))
			}
		})
	}
}

func TestCreateOrder_MapsTheIdempotencyOutcomes(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
		code string
	}{
		"still processing": {apierr.New(codes.Aborted, apierr.ReasonRequestInProgress, "internal"), 409, "request_in_progress"},
		"key reused":       {apierr.New(codes.InvalidArgument, apierr.ReasonIdempotencyKeyReused, "internal"), 422, "idempotency_key_reused"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.orders.createOrder = func(*orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) { return nil, tc.err }

			rec := h.do("POST", "/api/orders", orderBody(seatA), append(bearer("user-token"), "Idempotency-Key", "k1")...)

			if rec.Code != tc.want || errCode(t, rec) != tc.code {
				t.Errorf("got %d %q, want %d %q", rec.Code, errCode(t, rec), tc.want, tc.code)
			}
			if strings.Contains(rec.Body.String(), "internal") {
				t.Errorf("an internal message leaked: %s", rec.Body.String())
			}
		})
	}
}
