//go:build integration

package server

import (
	"context"
	"testing"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	"github.com/dollarino1/ticketwave/pkg/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Run with ANALYTICS_DATABASE_URL pointing at the analytics database.
func newServer(t *testing.T) (*Server, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.NewPool(t, "ANALYTICS_DATABASE_URL", "migrations/analytics")
	return New(pool), pool
}

func seed(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, tickets, revenue, confirmed, failed int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO event_stats (event_id, tickets_sold, revenue_cents, orders_confirmed, orders_failed) VALUES ($1, $2, $3, $4, $5)`,
		id, tickets, revenue, confirmed, failed)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetEventStats_ReturnsTheStoredNumbers(t *testing.T) {
	s, pool := newServer(t)
	id := uuid.New()
	seed(t, pool, id, 12, 60000, 7, 2)

	resp, err := s.GetEventStats(context.Background(), &analyticsv1.GetEventStatsRequest{EventId: id.String()})
	if err != nil {
		t.Fatalf("GetEventStats: %v", err)
	}

	got := resp.GetStats()
	if got.GetEventId() != id.String() || got.GetTicketsSold() != 12 || got.GetRevenueCents() != 60000 ||
		got.GetOrdersConfirmed() != 7 || got.GetOrdersFailed() != 2 {
		t.Errorf("stats = %v, want event %s with 12 tickets, 60000 cents, 7 confirmed, 2 failed", got, id)
	}
	if got.GetUpdatedAt() == nil || got.GetUpdatedAt().AsTime().IsZero() {
		t.Error("updated_at was not set")
	}
}

func TestGetEventStats_ErrorCodes(t *testing.T) {
	s, _ := newServer(t)
	cases := map[string]struct {
		eventID string
		want    codes.Code
	}{
		"unknown event": {uuid.NewString(), codes.NotFound},
		"not a UUID":    {"nope", codes.InvalidArgument},
		"empty":         {"", codes.InvalidArgument},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.GetEventStats(context.Background(), &analyticsv1.GetEventStatsRequest{EventId: tc.eventID})
			if got := status.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

func TestListEventStats_OrdersByRevenueAndHonoursTheLimit(t *testing.T) {
	s, pool := newServer(t)
	low, mid, high := uuid.New(), uuid.New(), uuid.New()
	seed(t, pool, mid, 1, 5000, 1, 0)
	seed(t, pool, low, 1, 1000, 1, 0)
	seed(t, pool, high, 1, 9000, 1, 0)

	resp, err := s.ListEventStats(context.Background(), &analyticsv1.ListEventStatsRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ListEventStats: %v", err)
	}

	got := resp.GetStats()
	if len(got) != 2 || got[0].GetEventId() != high.String() || got[1].GetEventId() != mid.String() {
		t.Errorf("got %v, want the two highest-revenue events, %s then %s", got, high, mid)
	}
}

func TestListEventStats_DefaultsToTwentyAndNeverExceedsTheCap(t *testing.T) {
	s, pool := newServer(t)
	for i := 0; i < 25; i++ {
		seed(t, pool, uuid.New(), 1, int64(1000+i), 1, 0)
	}

	resp, err := s.ListEventStats(context.Background(), &analyticsv1.ListEventStatsRequest{})
	if err != nil {
		t.Fatalf("ListEventStats: %v", err)
	}
	if n := len(resp.GetStats()); n != defaultLimit {
		t.Errorf("no limit returned %d rows, want the default %d", n, defaultLimit)
	}

	resp, err = s.ListEventStats(context.Background(), &analyticsv1.ListEventStatsRequest{Limit: 100000})
	if err != nil {
		t.Fatalf("ListEventStats: %v", err)
	}
	if n := len(resp.GetStats()); n != 25 {
		t.Errorf("huge limit returned %d rows, want all 25 that exist (and never more than %d)", n, maxLimit)
	}
}
