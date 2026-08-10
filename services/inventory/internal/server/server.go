package server

import (
	"context"
	"fmt"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	rows, err := s.pool.Query(ctx, "SELECT id, label, status FROM seats WHERE event_id = $1", req.EventId)
	if err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}
	var respSeats []*inventoryv1.Seat
	for rows.Next() {
		var id uuid.UUID
		var label, dbStatus string
		if err := rows.Scan(&id, &label, &dbStatus); err != nil {
			return nil, fmt.Errorf("could not scan row: %w", err)
		}
		respSeats = append(respSeats, &inventoryv1.Seat{
			Id:      id.String(),
			EventId: req.EventId,
			Label:   label,
			Status:  toProtoStatus(dbStatus),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return &inventoryv1.ListSeatsResponse{Seats: respSeats}, nil
}

func toProtoStatus(s string) inventoryv1.SeatStatus {
	switch s {
	case "AVAILABLE":
		return inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE
	case "HELD":
		return inventoryv1.SeatStatus_SEAT_STATUS_HELD
	case "SOLD":
		return inventoryv1.SeatStatus_SEAT_STATUS_SOLD
	default:
		return inventoryv1.SeatStatus_SEAT_STATUS_UNSPECIFIED
	}
}

func (s *Server) ReserveSeats(ctx context.Context, req *inventoryv1.ReserveSeatsRequest) (*inventoryv1.ReserveSeatsResponse, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `SELECT id, status FROM seats WHERE id = ANY($1) FOR UPDATE`, req.SeatIds)
	if err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}

	var seats []struct {
		ID     uuid.UUID
		Status string
	}

	for rows.Next() {
		var id uuid.UUID
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			return nil, fmt.Errorf("could not scan row: %w", err)
		}
		seats = append(seats, struct {
			ID     uuid.UUID
			Status string
		}{id, status})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("could not iterate row: %w", err)
	}

	if len(seats) != len(req.SeatIds) {
		return nil, status.Error(codes.FailedPrecondition, "one or more seats do not exist")
	}

	for _, seat := range seats {
		if seat.Status != "AVAILABLE" {
			return nil, status.Error(codes.FailedPrecondition, "one or more seats are not available")
		}
	}

	_, err = tx.Exec(ctx, "UPDATE seats SET status = 'HELD', held_by = $1 WHERE id = ANY($2)", req.OrderId, req.SeatIds)
	if err != nil {
		return nil, fmt.Errorf("update seats to held: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	respSeats := make([]*inventoryv1.Seat, 0, len(seats))
	for _, seat := range seats {
		respSeats = append(respSeats, &inventoryv1.Seat{
			Id:      seat.ID.String(),
			EventId: req.EventId,
			Status:  inventoryv1.SeatStatus_SEAT_STATUS_HELD,
		})
	}

	return &inventoryv1.ReserveSeatsResponse{
		Seats: respSeats,
	}, nil
}

func (s *Server) ConfirmSeats(ctx context.Context, req *inventoryv1.ConfirmSeatsRequest) (*inventoryv1.ConfirmSeatsResponse, error) {
	return &inventoryv1.ConfirmSeatsResponse{}, nil
}

func (s *Server) ReleaseSeats(ctx context.Context, req *inventoryv1.ReleaseSeatsRequest) (*inventoryv1.ReleaseSeatsResponse, error) {
	return &inventoryv1.ReleaseSeatsResponse{}, nil
}
