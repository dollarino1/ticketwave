package server

import (
	"context"
	"errors"
	"fmt"
	"log"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	paymentv1 "github.com/dollarino1/ticketwave/gen/ticketwave/payment/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	orderv1.UnimplementedOrderServiceServer
	pool      *pgxpool.Pool
	inventory inventoryv1.InventoryServiceClient
	payment   paymentv1.PaymentServiceClient
}

func New(pool *pgxpool.Pool, inventory inventoryv1.InventoryServiceClient, payment paymentv1.PaymentServiceClient) *Server {
	return &Server{pool: pool, inventory: inventory, payment: payment}
}

func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if err := validateCreateOrder(req); err != nil {
		return nil, err
	}

	order := orderSnapshot{
		ID:          uuid.New(),
		UserID:      req.UserId,
		EventID:     req.EventId,
		SeatIDs:     req.SeatIds,
		AmountCents: req.AmountCents,
	}

	// The insert is the idempotency check. If this key was used before, the unique
	// index refuses the row, and the request is answered from the first attempt. Doing
	// it by insert rather than "look, then insert" is what makes two simultaneous
	// identical requests safe: the database lets exactly one of them through.
	if err := s.createOrder(ctx, order, req.GetIdempotencyKey()); err != nil {
		if isDuplicateKey(err) {
			return s.replay(ctx, req)
		}
		return nil, err
	}

	_, err := s.inventory.ReserveSeats(ctx, &inventoryv1.ReserveSeatsRequest{
		EventId: order.EventID,
		SeatIds: order.SeatIDs,
		OrderId: order.ID.String(),
	})
	if err != nil {
		log.Printf("order %s: reserve seats failed: %v", order.ID, err)
		if isRefusal(err) {
			s.fail(ctx, order, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE)
			return nil, errSeatsUnavailable()
		}
		// A breakdown, not a refusal: the reservation may have succeeded with only
		// the reply lost, so release defensively before giving up.
		s.releaseSeats(ctx, order)
		s.fail(ctx, order, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE)
		return nil, errServiceUnavailable()
	}

	_, err = s.payment.Charge(ctx, &paymentv1.ChargeRequest{
		OrderId:     order.ID.String(),
		AmountCents: order.AmountCents,
	})
	if err != nil {
		log.Printf("order %s: payment failed: %v", order.ID, err)

		// Compensation: undo the hold placed above.
		s.releaseSeats(ctx, order)

		if isRefusal(err) {
			s.fail(ctx, order, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED)
			return nil, errPaymentDeclined()
		}
		// payment-svc broke or timed out, so the customer may or may not have been
		// charged. payment-svc records every attempt against the order ID, which is
		// what a reconciliation job would check. Until one exists, say so loudly.
		log.Printf("order %s: CRITICAL — payment outcome unknown, reconcile against the payments table", order.ID)
		s.fail(ctx, order, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE)
		return nil, errServiceUnavailable()
	}

	_, err = s.inventory.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{
		EventId: order.EventID,
		SeatIds: order.SeatIDs,
		OrderId: order.ID.String(),
	})
	if err != nil {
		// Worst case in this saga: money was already taken but the seats never
		// got marked SOLD. The order is left PENDING on purpose: there is no
		// honest event to announce, and a reconciliation job has to resolve it.
		log.Printf("order %s: CRITICAL — payment succeeded but confirm seats failed: %v", order.ID, err)
		return nil, status.Error(codes.Internal, "order could not be finalized, contact support")
	}

	err = s.transition(ctx, order, "CONFIRMED",
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED,
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED)
	if err != nil {
		return nil, fmt.Errorf("mark order confirmed: %w", err)
	}

	return &orderv1.CreateOrderResponse{
		OrderId: order.ID.String(),
		Status:  orderv1.OrderStatus_ORDER_STATUS_CONFIRMED,
	}, nil
}

// isRefusal tells a service saying "no" (the seats are taken, the card was
// declined) from a service breaking. Both come back as gRPC errors, but only the
// first is a decision about this order; the second says nothing about it and the
// customer should simply retry.
func isRefusal(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

// releaseSeats is the saga's compensation for a reservation. If it fails there is
// no synchronous recovery: it is logged loudly, and the hold's own five minute
// TTL (inventory-svc's sweeper) is the safety net that frees the seats anyway.
func (s *Server) releaseSeats(ctx context.Context, o orderSnapshot) {
	_, err := s.inventory.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{
		EventId: o.EventID,
		SeatIds: o.SeatIDs,
		OrderId: o.ID.String(),
	})
	if err != nil {
		log.Printf("order %s: COMPENSATION FAILED, seats not released: %v", o.ID, err)
	}
}

// createOrder inserts the order and its ORDER_CREATED outbox row in one transaction.
func (s *Server) createOrder(ctx context.Context, o orderSnapshot, idempotencyKey string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO orders (id, user_id, event_id, seat_ids, amount_cents, status, idempotency_key)
			 VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)`,
			o.ID, o.UserID, o.EventID, o.SeatIDs, o.AmountCents, nullIfEmpty(idempotencyKey),
		)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		return insertOutbox(ctx, tx, o,
			eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED,
			eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED)
	})
}

// transition moves the order to a new status and writes the matching outbox row
// in one transaction, so the status and its announcement can never disagree.
func (s *Server) transition(
	ctx context.Context,
	o orderSnapshot,
	newStatus string,
	typ eventsv1.OrderEventType,
	reason eventsv1.OrderFailureReason,
) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The reason is stored with a failure so a replayed request can be answered
		// with the same one. A confirmed order has none.
		var failureReason *string
		if typ == eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED {
			label := reasonLabel(reason)
			failureReason = &label
		}
		if _, err := tx.Exec(ctx,
			`UPDATE orders SET status = $1, failure_reason = $3 WHERE id = $2`,
			newStatus, o.ID, failureReason,
		); err != nil {
			return fmt.Errorf("update order status: %w", err)
		}
		return insertOutbox(ctx, tx, o, typ, reason)
	})
	if err == nil {
		recordOutcome(typ, reason)
	}
	return err
}

// fail records a FAILED transition. The caller is already on an error path, so
// a failure here is logged rather than returned.
func (s *Server) fail(ctx context.Context, o orderSnapshot, reason eventsv1.OrderFailureReason) {
	err := s.transition(ctx, o, "FAILED", eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED, reason)
	if err != nil {
		log.Printf("order %s: failed to record FAILED transition: %v", o.ID, err)
	}
}

func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if _, err := uuid.Parse(req.GetOrderId()); err != nil {
		return nil, invalid("order_id must be a UUID")
	}

	var userID, eventID uuid.UUID
	var seatIDs []string
	var amountCents int64
	var dbStatus string

	err := s.pool.QueryRow(ctx,
		`SELECT user_id, event_id, seat_ids, amount_cents, status FROM orders WHERE id = $1`,
		req.OrderId,
	).Scan(&userID, &eventID, &seatIDs, &amountCents, &dbStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	if err != nil {
		// Any other failure is ours, not "not found": reporting it as NotFound would
		// hide a database outage behind a client error.
		return nil, fmt.Errorf("query order: %w", err)
	}

	return &orderv1.GetOrderResponse{
		OrderId:     req.OrderId,
		UserId:      userID.String(),
		EventId:     eventID.String(),
		SeatIds:     seatIDs,
		AmountCents: amountCents,
		Status:      toOrderStatus(dbStatus),
	}, nil
}

func toOrderStatus(s string) orderv1.OrderStatus {
	switch s {
	case "PENDING":
		return orderv1.OrderStatus_ORDER_STATUS_PENDING
	case "CONFIRMED":
		return orderv1.OrderStatus_ORDER_STATUS_CONFIRMED
	case "FAILED":
		return orderv1.OrderStatus_ORDER_STATUS_FAILED
	default:
		return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
