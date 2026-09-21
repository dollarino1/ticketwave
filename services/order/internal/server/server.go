package server

import (
	"context"
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
	order := orderSnapshot{
		ID:          uuid.New(),
		UserID:      req.UserId,
		EventID:     req.EventId,
		SeatIDs:     req.SeatIds,
		AmountCents: req.AmountCents,
	}

	if err := s.createOrder(ctx, order); err != nil {
		return nil, err
	}

	_, err := s.inventory.ReserveSeats(ctx, &inventoryv1.ReserveSeatsRequest{
		EventId: order.EventID,
		SeatIds: order.SeatIDs,
		OrderId: order.ID.String(),
	})
	if err != nil {
		log.Printf("order %s: reserve seats failed: %v", order.ID, err)
		s.fail(ctx, order, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE)
		return nil, status.Error(codes.FailedPrecondition, "unable to reserve seats")
	}

	_, err = s.payment.Charge(ctx, &paymentv1.ChargeRequest{
		OrderId:     order.ID.String(),
		AmountCents: order.AmountCents,
	})
	if err != nil {
		log.Printf("order %s: payment failed: %v", order.ID, err)

		// Compensation: undo the hold placed above. If this itself fails there is
		// no synchronous recovery, so it is logged loudly and the seat hold's own
		// 5-minute TTL (see inventory-svc's sweeper) is the safety net.
		if _, relErr := s.inventory.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{
			EventId: order.EventID,
			SeatIds: order.SeatIDs,
			OrderId: order.ID.String(),
		}); relErr != nil {
			log.Printf("order %s: COMPENSATION FAILED, seats not released: %v", order.ID, relErr)
		}

		s.fail(ctx, order, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED)
		return nil, status.Error(codes.FailedPrecondition, "payment failed")
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

// createOrder inserts the order and its ORDER_CREATED outbox row in one transaction.
func (s *Server) createOrder(ctx context.Context, o orderSnapshot) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO orders (id, user_id, event_id, seat_ids, amount_cents, status) VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
			o.ID, o.UserID, o.EventID, o.SeatIDs, o.AmountCents,
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
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE orders SET status = $1 WHERE id = $2`, newStatus, o.ID); err != nil {
			return fmt.Errorf("update order status: %w", err)
		}
		return insertOutbox(ctx, tx, o, typ, reason)
	})
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
	var userID, eventID uuid.UUID
	var seatIDs []string
	var amountCents int64
	var dbStatus string

	err := s.pool.QueryRow(ctx,
		`SELECT user_id, event_id, seat_ids, amount_cents, status FROM orders WHERE id = $1`,
		req.OrderId,
	).Scan(&userID, &eventID, &seatIDs, &amountCents, &dbStatus)
	if err != nil {
		return nil, status.Error(codes.NotFound, "order not found")
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
