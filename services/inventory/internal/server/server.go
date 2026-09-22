package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
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
	if err := validateCreateEvent(req); err != nil {
		return nil, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventID := uuid.New()
	_, err = tx.Exec(ctx, "INSERT INTO events (id, name) VALUES ($1, $2)", eventID, strings.TrimSpace(req.Name))
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
	if err := validateEventID(req.GetEventId()); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, "SELECT id, label, status FROM seats WHERE event_id = $1 ORDER BY label", req.EventId)
	if err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}
	// Without this, an error part-way through the loop below would leave the rows
	// open and the pooled connection checked out forever.
	defer rows.Close()

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
	if err := validateSeatRequest(req.GetEventId(), req.GetSeatIds(), req.GetOrderId()); err != nil {
		return nil, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Scoped to the event: a seat that belongs to another event counts as "does not
	// exist", so a caller cannot reserve it under the wrong event (and the seat
	// event published below always names the seat's real event).
	rows, err := tx.Query(ctx,
		`SELECT id, status FROM seats WHERE id = ANY($1) AND event_id = $2 FOR UPDATE`,
		req.SeatIds, req.EventId)
	if err != nil {
		return nil, fmt.Errorf("could not execute query: %w", err)
	}
	defer rows.Close()

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
		reservations.WithLabelValues(conflict).Inc()
		return nil, status.Error(codes.FailedPrecondition, "one or more seats do not exist")
	}

	for _, seat := range seats {
		if seat.Status != "AVAILABLE" {
			reservations.WithLabelValues(conflict).Inc()
			return nil, status.Error(codes.FailedPrecondition, "one or more seats are not available")
		}
	}

	_, err = tx.Exec(ctx, "UPDATE seats SET status = 'HELD', held_by = $1 WHERE id = ANY($2)", req.OrderId, req.SeatIds)
	if err != nil {
		return nil, fmt.Errorf("update seats to held: %w", err)
	}
	if err := emitSeatEvent(ctx, tx, req.EventId, req.SeatIds, eventsv1.SeatState_SEAT_STATE_HELD); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}
	reservations.WithLabelValues(reserved).Inc()

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
	if err := validateSeatRequest(req.GetEventId(), req.GetSeatIds(), req.GetOrderId()); err != nil {
		return nil, err
	}

	err := s.moveHeldSeats(ctx, req.EventId, req.SeatIds, req.OrderId, "SOLD",
		eventsv1.SeatState_SEAT_STATE_SOLD, "one or more seats could not be confirmed")
	if err != nil {
		return nil, err
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
	if err := validateSeatRequest(req.GetEventId(), req.GetSeatIds(), req.GetOrderId()); err != nil {
		return nil, err
	}

	err := s.moveHeldSeats(ctx, req.EventId, req.SeatIds, req.OrderId, "AVAILABLE",
		eventsv1.SeatState_SEAT_STATE_AVAILABLE, "one or more seats could not be released")
	if err != nil {
		return nil, err
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

// moveHeldSeats moves seats this order is holding to a new state and records the
// change in the outbox, in one transaction. All of the seats must be held by the
// order, or nothing changes and failure is returned.
func (s *Server) moveHeldSeats(ctx context.Context, eventID string, seatIDs []string, orderID, newStatus string,
	state eventsv1.SeatState, failure string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE seats SET status = $4, held_by = NULL
			  WHERE id = ANY($1) AND event_id = $2 AND held_by = $3 AND status = 'HELD'`,
			seatIDs, eventID, orderID, newStatus,
		)
		if err != nil {
			return fmt.Errorf("update seats to %s: %w", newStatus, err)
		}
		if int(tag.RowsAffected()) != len(seatIDs) {
			return status.Error(codes.FailedPrecondition, failure)
		}
		return emitSeatEvent(ctx, tx, eventID, seatIDs, state)
	})
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

	released := 0
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`UPDATE seats SET status = 'AVAILABLE', held_by = NULL
			  WHERE id = ANY($1) AND status = 'HELD'
			RETURNING id, event_id`,
			expired,
		)
		if err != nil {
			return fmt.Errorf("release expired holds: %w", err)
		}
		defer rows.Close()

		// One event per Kafka event, listing all of its freed seats.
		byEvent := map[string][]string{}
		for rows.Next() {
			var id, eventID uuid.UUID
			if err := rows.Scan(&id, &eventID); err != nil {
				return fmt.Errorf("scan released seat: %w", err)
			}
			byEvent[eventID.String()] = append(byEvent[eventID.String()], id.String())
			released++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate released seats: %w", err)
		}
		rows.Close() // the transaction's connection is busy until then

		for eventID, seatIDs := range byEvent {
			if err := emitSeatEvent(ctx, tx, eventID, seatIDs, eventsv1.SeatState_SEAT_STATE_AVAILABLE); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("sweep: %v", err)
		return
	}
	expiredHolds.Add(float64(released))
	log.Printf("sweep: released %d expired hold(s)", released)
}
