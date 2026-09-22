package server

import (
	"context"
	"errors"
	"fmt"
	"slices"

	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/dollarino1/ticketwave/pkg/apierr"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
)

// idempotencyIndex is the unique index that enforces one order per (user, key).
const idempotencyIndex = "orders_idempotency_idx"

// The three ways an order can be refused, built in one place so that the FIRST
// attempt and a REPLAY of it return exactly the same error.

func errSeatsUnavailable() error {
	return apierr.New(codes.FailedPrecondition, apierr.ReasonSeatsUnavailable, "unable to reserve seats")
}

func errPaymentDeclined() error {
	return apierr.New(codes.FailedPrecondition, apierr.ReasonPaymentDeclined, "payment failed")
}

func errServiceUnavailable() error {
	return apierr.New(codes.Unavailable, apierr.ReasonServiceUnavailable, "service temporarily unavailable, please try again")
}

// isDuplicateKey reports whether err is the database refusing a second order with the
// same (user, idempotency key). It matches the constraint by name, so an unrelated
// unique violation (say a duplicate order ID) is never mistaken for a retry.
func isDuplicateKey(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == idempotencyIndex
}

// nullIfEmpty stores "no key" as SQL NULL, which the partial unique index ignores.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type storedOrder struct {
	ID            uuid.UUID
	EventID       string
	SeatIDs       []string
	AmountCents   int64
	Status        string
	FailureReason *string
}

func (s *Server) findByKey(ctx context.Context, userID, key string) (storedOrder, error) {
	var o storedOrder
	var eventID uuid.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT id, event_id, seat_ids, amount_cents, status, failure_reason
		   FROM orders WHERE user_id = $1 AND idempotency_key = $2`,
		userID, key,
	).Scan(&o.ID, &eventID, &o.SeatIDs, &o.AmountCents, &o.Status, &o.FailureReason)
	o.EventID = eventID.String()
	return o, err
}

// sameRequest reports whether a stored order is what req asks for. Seats are
// compared as a set: asking for [a, b] and [b, a] is the same request.
func sameRequest(o storedOrder, req *orderv1.CreateOrderRequest) bool {
	if o.EventID != req.GetEventId() || o.AmountCents != req.GetAmountCents() {
		return false
	}
	a, b := slices.Clone(o.SeatIDs), slices.Clone(req.GetSeatIds())
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// replay answers a request whose key has been used before, with what the first
// attempt produced. Nothing is reserved, charged or created again.
func (s *Server) replay(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	o, err := s.findByKey(ctx, req.GetUserId(), req.GetIdempotencyKey())
	if err != nil {
		return nil, fmt.Errorf("look up the order this key belongs to: %w", err)
	}

	// The same key attached to a different request is a bug in the client. Handing
	// back the first request's result would look like success while doing something
	// other than what was asked, so it is refused.
	if !sameRequest(o, req) {
		return nil, apierr.New(codes.InvalidArgument, apierr.ReasonIdempotencyKeyReused,
			"this idempotency key was already used for a different request")
	}

	switch o.Status {
	case "CONFIRMED":
		return &orderv1.CreateOrderResponse{OrderId: o.ID.String(), Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED}, nil
	case "FAILED":
		return nil, failureFor(o.FailureReason)
	default:
		// Still being processed (or stuck mid-saga). Starting another would be the very
		// duplicate this mechanism exists to prevent, so the client is told to wait.
		return nil, apierr.New(codes.Aborted, apierr.ReasonRequestInProgress,
			"a request with this idempotency key is still being processed")
	}
}

// failureFor rebuilds the error the failed attempt returned, from its stored reason.
func failureFor(reason *string) error {
	if reason == nil {
		return errServiceUnavailable()
	}
	switch *reason {
	case "seats_unavailable":
		return errSeatsUnavailable()
	case "payment_declined":
		return errPaymentDeclined()
	default:
		return errServiceUnavailable()
	}
}
