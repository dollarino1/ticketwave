package httpapi

import (
	"net/http"
	"regexp"

	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/dollarino1/ticketwave/pkg/authz"
)

const maxSeatsPerOrder = 10

type createOrderBody struct {
	EventID string   `json:"event_id"`
	SeatIDs []string `json:"seat_ids"`
}

type orderView struct {
	OrderID     string   `json:"order_id"`
	EventID     string   `json:"event_id,omitempty"`
	SeatIDs     []string `json:"seat_ids,omitempty"`
	AmountCents int64    `json:"amount_cents,omitempty"`
	Status      string   `json:"status"`
}

func orderStatusName(s orderv1.OrderStatus) string {
	switch s {
	case orderv1.OrderStatus_ORDER_STATUS_PENDING:
		return "pending"
	case orderv1.OrderStatus_ORDER_STATUS_CONFIRMED:
		return "confirmed"
	case orderv1.OrderStatus_ORDER_STATUS_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func (a *API) createOrder(w http.ResponseWriter, r *http.Request) {
	claims, ok := claimsFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "sign in to continue")
		return
	}

	// Unknown fields are rejected by decodeJSON, so a body that tries to name its
	// own user_id or amount_cents never gets this far.
	var body createOrderBody
	if !decodeJSON(w, r, &body) {
		return
	}
	if n := len(body.SeatIDs); n < 1 || n > maxSeatsPerOrder {
		writeError(w, http.StatusBadRequest, "invalid_argument", "an order must contain 1 to 10 seats")
		return
	}

	// Optional. With it, a retry of the same request (after a timeout, a dropped
	// connection, a double click) returns the first outcome instead of buying twice.
	key := r.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(key) {
		writeError(w, http.StatusBadRequest, "invalid_argument",
			"Idempotency-Key must be 1 to 64 letters, digits, dashes or underscores")
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.orders.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		UserId:  claims.UserID, // from the verified token, never from the request
		EventId: body.EventID,
		SeatIds: body.SeatIDs,
		// Computed here so the client cannot name its own price.
		AmountCents:    int64(len(body.SeatIDs)) * a.seatPriceCents,
		IdempotencyKey: key,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, orderView{OrderID: resp.GetOrderId(), Status: orderStatusName(resp.GetStatus())})
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	claims, ok := claimsFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "sign in to continue")
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.orders.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: r.PathValue("id")})
	if err != nil {
		a.fail(w, r, err)
		return
	}

	// Someone else's order is reported as missing, not forbidden: a 403 would
	// confirm that the ID exists, letting a caller probe for other people's orders.
	if resp.GetUserId() != claims.UserID && !authz.Role(claims.Role).AtLeast(authz.RoleAdmin) {
		writeError(w, http.StatusNotFound, "not_found", "order not found")
		return
	}
	writeJSON(w, http.StatusOK, orderView{
		OrderID:     resp.GetOrderId(),
		EventID:     resp.GetEventId(),
		SeatIDs:     resp.GetSeatIds(),
		AmountCents: resp.GetAmountCents(),
		Status:      orderStatusName(resp.GetStatus()),
	})
}

// validIdempotencyKey accepts the empty key ("not idempotent") and otherwise a short,
// plain token. It mirrors order-svc's rule, so a bad key is refused here, at the edge,
// with a 400, before any service is called.
var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func validIdempotencyKey(k string) bool { return k == "" || idempotencyKeyPattern.MatchString(k) }
