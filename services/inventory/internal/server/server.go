package server

import (
	"context"
	"fmt"
	"log"
	"time"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	holdTTL       = 5 * time.Minute
	sweepInterval = 30 * time.Second
)

type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer
	pool  *pgxpool.Pool
	redis *goredis.Client
}

func New(pool *pgxpool.Pool, redis *goredis.Client) *Server {
	return &Server{pool: pool, redis: redis}
}

func holdKey(seatID string) string {
	return "hold:seat:" + seatID
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

	for _, seat := range seats {
		if err := s.redis.Set(ctx, holdKey(seat.ID.String()), req.OrderId, holdTTL).Err(); err != nil {
			log.Printf("reserve: failed to set redis hold key for seat %s: %v", seat.ID, err)
		}
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
	tag, err := s.pool.Exec(ctx,
		`UPDATE seats SET status = 'SOLD', held_by = NULL WHERE id = ANY($1) AND held_by = $2 AND status = 'HELD'`,
		req.SeatIds, req.OrderId,
	)
	if err != nil {
		return nil, fmt.Errorf("update seats to sold: %w", err)
	}
	if int(tag.RowsAffected()) != len(req.SeatIds) {
		return nil, status.Error(codes.FailedPrecondition, "one or more seats could not be confirmed")
	}

	for _, id := range req.SeatIds {
		if err := s.redis.Del(ctx, holdKey(id)).Err(); err != nil {
			log.Printf("confirm: failed to delete redis hold key for seat %s: %v", id, err)
		}
	}

	respSeats := make([]*inventoryv1.Seat, 0, len(req.SeatIds))
	for _, id := range req.SeatIds {
		respSeats = append(respSeats, &inventoryv1.Seat{
			Id:      id,
			EventId: req.EventId,
			Status:  inventoryv1.SeatStatus_SEAT_STATUS_SOLD,
		})
	}

	return &inventoryv1.ConfirmSeatsResponse{Seats: respSeats}, nil
}

func (s *Server) ReleaseSeats(ctx context.Context, req *inventoryv1.ReleaseSeatsRequest) (*inventoryv1.ReleaseSeatsResponse, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE seats SET status = 'AVAILABLE', held_by = NULL WHERE id = ANY($1) AND held_by = $2 AND status = 'HELD'`,
		req.SeatIds, req.OrderId,
	)
	if err != nil {
		return nil, fmt.Errorf("update seats to available: %w", err)
	}
	if int(tag.RowsAffected()) != len(req.SeatIds) {
		return nil, status.Error(codes.FailedPrecondition, "one or more seats could not be released")
	}

	for _, id := range req.SeatIds {
		if err := s.redis.Del(ctx, holdKey(id)).Err(); err != nil {
			log.Printf("release: failed to delete redis hold key for seat %s: %v", id, err)
		}
	}

	respSeats := make([]*inventoryv1.Seat, 0, len(req.SeatIds))
	for _, id := range req.SeatIds {
		respSeats = append(respSeats, &inventoryv1.Seat{
			Id:      id,
			EventId: req.EventId,
			Status:  inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE,
		})
	}

	return &inventoryv1.ReleaseSeatsResponse{Seats: respSeats}, nil
}

// StartHoldSweeper runs a background reconciliation loop: every sweepInterval,
// it finds seats marked HELD in Postgres whose Redis hold key has expired (or
// never existed) and releases them back to AVAILABLE. This is what actually
// enforces the 5-minute hold TTL — Redis's own key expiry has no way to call
// back into Postgres on its own, so something has to poll and reconcile.
func (s *Server) StartHoldSweeper(ctx context.Context) {
	ticker := time.NewTicker(sweepInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweepExpiredHolds(ctx)
			}
		}
	}()
}

func (s *Server) sweepExpiredHolds(ctx context.Context) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM seats WHERE status = 'HELD'`)
	if err != nil {
		log.Printf("sweep: query held seats: %v", err)
		return
	}

	var heldIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			log.Printf("sweep: scan held seat: %v", err)
			return
		}
		heldIDs = append(heldIDs, id)
	}
	if err := rows.Err(); err != nil {
		log.Printf("sweep: iterate held seats: %v", err)
		return
	}

	var expired []uuid.UUID
	for _, id := range heldIDs {
		exists, err := s.redis.Exists(ctx, holdKey(id.String())).Result()
		if err != nil {
			log.Printf("sweep: redis exists check for seat %s: %v", id, err)
			continue
		}
		if exists == 0 {
			expired = append(expired, id)
		}
	}

	if len(expired) == 0 {
		return
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE seats SET status = 'AVAILABLE', held_by = NULL WHERE id = ANY($1) AND status = 'HELD'`,
		expired,
	)
	if err != nil {
		log.Printf("sweep: release expired holds: %v", err)
		return
	}
	log.Printf("sweep: released %d expired hold(s)", tag.RowsAffected())
}
