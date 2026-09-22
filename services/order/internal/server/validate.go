package server

import (
	"regexp"

	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxSeatsPerOrder = 10
	// A sanity ceiling, not a business rule: it stops an absurd amount from ever
	// reaching payment-svc or overflowing a sum further down the line.
	maxAmountCents = 100_000_000
)

func invalid(message string) error {
	return status.Error(codes.InvalidArgument, message)
}

// validateCreateOrder rejects a malformed order before it is stored or any other
// service is called. Without it a value that is not a UUID would surface as a
// database error halfway through the saga, after the order row already exists.
func validateCreateOrder(req *orderv1.CreateOrderRequest) error {
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return invalid("user_id must be a UUID")
	}
	if _, err := uuid.Parse(req.GetEventId()); err != nil {
		return invalid("event_id must be a UUID")
	}

	seats := req.GetSeatIds()
	if len(seats) == 0 || len(seats) > maxSeatsPerOrder {
		return invalid("an order must contain 1 to 10 seats")
	}
	seen := make(map[uuid.UUID]struct{}, len(seats))
	for _, raw := range seats {
		id, err := uuid.Parse(raw)
		if err != nil {
			return invalid("seat_ids must all be UUIDs")
		}
		if _, dup := seen[id]; dup {
			return invalid("seat_ids must not repeat a seat")
		}
		seen[id] = struct{}{}
	}

	if req.GetAmountCents() <= 0 || req.GetAmountCents() > maxAmountCents {
		return invalid("amount_cents must be positive")
	}
	if !validIdempotencyKey(req.GetIdempotencyKey()) {
		return invalid("idempotency_key must be 1 to 64 letters, digits, dashes or underscores")
	}
	return nil
}

// idempotencyKey is what a client may send to make a retry safe. Short and plain, like
// a request ID: it is stored, indexed and logged, so it must not be an arbitrary blob.
var idempotencyKey = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// validIdempotencyKey accepts the empty key, which means "not idempotent".
func validIdempotencyKey(k string) bool { return k == "" || idempotencyKey.MatchString(k) }
