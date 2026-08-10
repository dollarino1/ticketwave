package server

import (
	"context"
	"fmt"

	paymentv1 "github.com/dollarino1/ticketwave/gen/ticketwave/payment/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	paymentv1.UnimplementedPaymentServiceServer
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Server {
	return &Server{pool: pool}
}

// Charge is deliberately fake: it declines any amount that's a multiple of 7
// cents, so the saga's compensation path can be triggered on demand instead
// of waiting for a real payment processor to fail.
func (s *Server) Charge(ctx context.Context, req *paymentv1.ChargeRequest) (*paymentv1.ChargeResponse, error) {
	paymentID := uuid.New()

	if req.AmountCents%7 == 0 {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO payments (id, order_id, amount_cents, status) VALUES ($1, $2, $3, 'FAILED')`,
			paymentID, req.OrderId, req.AmountCents,
		)
		if err != nil {
			return nil, fmt.Errorf("record failed payment: %w", err)
		}
		return nil, status.Error(codes.FailedPrecondition, "payment declined")
	}

	_, err := s.pool.Exec(ctx,
		`INSERT INTO payments (id, order_id, amount_cents, status) VALUES ($1, $2, $3, 'SUCCEEDED')`,
		paymentID, req.OrderId, req.AmountCents,
	)
	if err != nil {
		return nil, fmt.Errorf("record successful payment: %w", err)
	}

	return &paymentv1.ChargeResponse{PaymentId: paymentID.String()}, nil
}
