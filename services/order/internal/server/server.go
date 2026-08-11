package server

import (
	"context"
	"fmt"
	"log"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	paymentv1 "github.com/dollarino1/ticketwave/gen/ticketwave/payment/v1"
	"github.com/google/uuid"
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
	orderID := uuid.New()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO orders (id, user_id, event_id, seat_ids, amount_cents, status) VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
		orderID, req.UserId, req.EventId, req.SeatIds, req.AmountCents,
	)
	if err != nil {
		return nil, fmt.Errorf("insert order: %w", err)
	}

	_, err = s.inventory.ReserveSeats(ctx, &inventoryv1.ReserveSeatsRequest{
		EventId: req.EventId,
		SeatIds: req.SeatIds,
		OrderId: orderID.String(),
	})
	if err != nil {
		log.Printf("order %s: reserve seats failed: %v", orderID, err)
		s.markFailed(ctx, orderID)
		return nil, status.Error(codes.FailedPrecondition, "unable to reserve seats")
	}

	_, err = s.payment.Charge(ctx, &paymentv1.ChargeRequest{
		OrderId:     orderID.String(),
		AmountCents: req.AmountCents,
	})
	if err != nil {
		log.Printf("order %s: payment failed: %v", orderID, err)

		// Compensation: undo the hold placed above. If this itself fails, there's
		// no good synchronous recovery with today's tools — log loudly and move
		// on. Closing this gap for real is exactly what phase 5's outbox pattern
		// and a reconciliation consumer are for.
		if _, relErr := s.inventory.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{
			EventId: req.EventId,
			SeatIds: req.SeatIds,
			OrderId: orderID.String(),
		}); relErr != nil {
			log.Printf("order %s: COMPENSATION FAILED, seats not released: %v", orderID, relErr)
		}

		s.markFailed(ctx, orderID)
		return nil, status.Error(codes.FailedPrecondition, "payment failed")
	}

	_, err = s.inventory.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{
		EventId: req.EventId,
		SeatIds: req.SeatIds,
		OrderId: orderID.String(),
	})
	if err != nil {
		// Worst case in this saga: money was already taken but the seats never
		// got marked SOLD. No synchronous fix — same reconciliation gap as above.
		log.Printf("order %s: CRITICAL — payment succeeded but confirm seats failed: %v", orderID, err)
		return nil, status.Error(codes.Internal, "order could not be finalized, contact support")
	}

	if _, err := s.pool.Exec(ctx, `UPDATE orders SET status = 'CONFIRMED' WHERE id = $1`, orderID); err != nil {
		return nil, fmt.Errorf("mark order confirmed: %w", err)
	}

	return &orderv1.CreateOrderResponse{
		OrderId: orderID.String(),
		Status:  orderv1.OrderStatus_ORDER_STATUS_CONFIRMED,
	}, nil
}

func (s *Server) markFailed(ctx context.Context, orderID uuid.UUID) {
	if _, err := s.pool.Exec(ctx, `UPDATE orders SET status = 'FAILED' WHERE id = $1`, orderID); err != nil {
		log.Printf("order %s: failed to mark order FAILED: %v", orderID, err)
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
