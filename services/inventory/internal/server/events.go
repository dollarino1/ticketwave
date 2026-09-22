package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultEventLimit = 50
	maxEventLimit     = 200

	// One row per event with its seat totals. The LEFT JOIN keeps an event with
	// no seats; grouping by the primary key lets the other event columns ride along.
	selectEvents = `SELECT e.id, e.name, e.created_at,
	                       count(s.id),
	                       count(s.id) FILTER (WHERE s.status = 'AVAILABLE')
	                  FROM events e
	                  LEFT JOIN seats s ON s.event_id = e.id`
)

func (s *Server) GetEvent(ctx context.Context, req *inventoryv1.GetEventRequest) (*inventoryv1.GetEventResponse, error) {
	if err := validateEventID(req.GetEventId()); err != nil {
		return nil, err
	}
	id := uuid.MustParse(req.GetEventId())

	ev, err := scanEvent(s.pool.QueryRow(ctx, selectEvents+` WHERE e.id = $1 GROUP BY e.id`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "event not found")
	}
	if err != nil {
		return nil, fmt.Errorf("query event: %w", err)
	}
	return &inventoryv1.GetEventResponse{Event: ev}, nil
}

func (s *Server) ListEvents(ctx context.Context, req *inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
	rows, err := s.pool.Query(ctx, selectEvents+` GROUP BY e.id ORDER BY e.created_at DESC, e.id LIMIT $1`,
		clampEventLimit(req.GetLimit()))
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var out []*inventoryv1.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return &inventoryv1.ListEventsResponse{Events: out}, nil
}

// clampEventLimit applies the default and the ceiling, so a client cannot ask
// the database for an unbounded result.
func clampEventLimit(n int32) int32 {
	switch {
	case n <= 0:
		return defaultEventLimit
	case n > maxEventLimit:
		return maxEventLimit
	default:
		return n
	}
}

// scanner is the part of pgx.Row and pgx.Rows that scanEvent needs, so one
// function serves both the single-row and the list query.
type scanner interface {
	Scan(dest ...any) error
}

func scanEvent(row scanner) (*inventoryv1.Event, error) {
	var id uuid.UUID
	var name string
	var createdAt time.Time
	var total, available int64
	if err := row.Scan(&id, &name, &createdAt, &total, &available); err != nil {
		return nil, err
	}
	return &inventoryv1.Event{
		Id:             id.String(),
		Name:           name,
		TotalSeats:     int32(total),
		AvailableSeats: int32(available),
		CreatedAt:      timestamppb.New(createdAt),
	}, nil
}
