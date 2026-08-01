package server

import (
	"context"
	"fmt"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Server {
	return &Server{pool: pool}
}

func (s *Server) CreateEvent(ctx context.Context, req *inventoryv1.CreateEventRequest) (*inventoryv1.CreateEventResponse, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventID := uuid.New()
	_, err = tx.Exec(ctx, "INSERT INTO events (id, name) VALUES ($1, $2)", eventID, req.Name)
	if err != nil {
		return nil, fmt.Errorf("could not insert event: %w", err)
	}

	batch := &pgx.Batch{}
	for i := int32(1); i <= req.SeatCount; i++ {
		label := fmt.Sprintf("Seat %d", i)
		batch.Queue("INSERT INTO seats (id, event_id, label) VALUES ($1, $2, $3)", uuid.New(), eventID, label)
	}
	br := tx.SendBatch(ctx, batch)
	err = br.Close()
	if err != nil {
		return nil, fmt.Errorf("execute seat batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	return &inventoryv1.CreateEventResponse{EventId: eventID.String()}, nil
}

func (s *Server) ListSeats(ctx context.Context, req *inventoryv1.ListSeatsRequest) (*inventoryv1.ListSeatsResponse, error) {

	return &inventoryv1.ListSeatsResponse{}, nil
}

func (s *Server) ReserveSeats(ctx context.Context, req *inventoryv1.ReserveSeatsRequest) (*inventoryv1.ReserveSeatsResponse, error) {

	return &inventoryv1.ReserveSeatsResponse{}, nil
}

func (s *Server) ConfirmSeats(ctx context.Context, req *inventoryv1.ConfirmSeatsRequest) (*inventoryv1.ConfirmSeatsResponse, error) {
	return &inventoryv1.ConfirmSeatsResponse{}, nil
}

func (s *Server) ReleaseSeats(ctx context.Context, req *inventoryv1.ReleaseSeatsRequest) (*inventoryv1.ReleaseSeatsResponse, error) {
	return &inventoryv1.ReleaseSeatsResponse{}, nil
}
