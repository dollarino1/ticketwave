//go:build integration

package server

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_CountReservationsByOutcome(t *testing.T) {
	s, _ := newTestServer(t)
	eventID, seats := newEvent(t, s, "Metrics", 3)
	okBefore := testutil.ToFloat64(reservations.WithLabelValues(reserved))
	conflictBefore := testutil.ToFloat64(reservations.WithLabelValues(conflict))

	if err := reserve(s, eventID, uuid.NewString(), seats[0]); err != nil {
		t.Fatal(err)
	}
	if err := reserve(s, eventID, uuid.NewString(), seats[0]); err == nil { // taken
		t.Fatal("a taken seat was reserved")
	}
	if err := reserve(s, eventID, uuid.NewString(), uuid.NewString()); err == nil { // does not exist
		t.Fatal("a missing seat was reserved")
	}

	if got := testutil.ToFloat64(reservations.WithLabelValues(reserved)) - okBefore; got != 1 {
		t.Errorf("reserved rose by %v, want 1", got)
	}
	if got := testutil.ToFloat64(reservations.WithLabelValues(conflict)) - conflictBefore; got != 2 {
		t.Errorf("conflict rose by %v, want 2 (a taken seat and a missing one)", got)
	}
}

func TestMetrics_CountExpiredHoldsTheSweeperFrees(t *testing.T) {
	s, rdb := newTestServer(t)
	ctx := context.Background()
	eventID, seats := newEvent(t, s, "Expiry", 3)
	if err := reserve(s, eventID, uuid.NewString(), seats[0], seats[1]); err != nil {
		t.Fatal(err)
	}
	for _, id := range seats[:2] {
		if err := rdb.Del(ctx, holdKey(id)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	before := testutil.ToFloat64(expiredHolds)

	s.sweepExpiredHolds(ctx)

	if got := testutil.ToFloat64(expiredHolds) - before; got != 2 {
		t.Errorf("expired-holds counter rose by %v, want 2", got)
	}
}
