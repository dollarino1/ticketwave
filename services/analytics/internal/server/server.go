// Package server is analytics-svc's read API. It only reads: the numbers are
// written by the projection consuming order-events.
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultLimit = 20
	maxLimit     = 100

	selectStats = `SELECT event_id, tickets_sold, revenue_cents, orders_confirmed, orders_failed, updated_at FROM event_stats`
)

type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Server {
	return &Server{pool: pool}
}

func (s *Server) GetEventStats(ctx context.Context, req *analyticsv1.GetEventStatsRequest) (*analyticsv1.GetEventStatsResponse, error) {
	id, err := uuid.Parse(req.GetEventId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "event_id must be a UUID")
	}

	stats, err := scanStats(s.pool.QueryRow(ctx, selectStats+` WHERE event_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "no statistics for this event yet")
	}
	if err != nil {
		return nil, fmt.Errorf("query event stats: %w", err)
	}
	return &analyticsv1.GetEventStatsResponse{Stats: stats}, nil
}

func (s *Server) ListEventStats(ctx context.Context, req *analyticsv1.ListEventStatsRequest) (*analyticsv1.ListEventStatsResponse, error) {
	rows, err := s.pool.Query(ctx, selectStats+` ORDER BY revenue_cents DESC, event_id LIMIT $1`, clampLimit(req.GetLimit()))
	if err != nil {
		return nil, fmt.Errorf("query event stats: %w", err)
	}
	defer rows.Close()

	var out []*analyticsv1.EventStats
	for rows.Next() {
		stats, err := scanStats(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event stats: %w", err)
		}
		out = append(out, stats)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event stats: %w", err)
	}
	return &analyticsv1.ListEventStatsResponse{Stats: out}, nil
}

// clampLimit applies the default and the ceiling, so a client cannot ask the
// database for an unbounded result.
func clampLimit(n int32) int32 {
	switch {
	case n <= 0:
		return defaultLimit
	case n > maxLimit:
		return maxLimit
	default:
		return n
	}
}

// scanner is the part of pgx.Row and pgx.Rows that scanStats needs, so one
// function serves both the single-row and the list query.
type scanner interface {
	Scan(dest ...any) error
}

func scanStats(row scanner) (*analyticsv1.EventStats, error) {
	var id uuid.UUID
	var updatedAt time.Time
	stats := &analyticsv1.EventStats{}
	err := row.Scan(&id, &stats.TicketsSold, &stats.RevenueCents, &stats.OrdersConfirmed, &stats.OrdersFailed, &updatedAt)
	if err != nil {
		return nil, err
	}
	stats.EventId = id.String()
	stats.UpdatedAt = timestamppb.New(updatedAt)
	return stats, nil
}
