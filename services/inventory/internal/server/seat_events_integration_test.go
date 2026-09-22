//go:build integration

package server

import (
	"context"
	"slices"
	"testing"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/google/uuid"
)

// seatEventsFor returns the events waiting in the outbox for one event, oldest first.
func seatEventsFor(t *testing.T, s *Server, eventID string) []*eventsv1.SeatEvent {
	t.Helper()
	rows, err := s.pool.Query(context.Background(),
		`SELECT id, event_type, payload FROM outbox WHERE event_id = $1 ORDER BY created_at, id`, eventID)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()

	var out []*eventsv1.SeatEvent
	for rows.Next() {
		var id uuid.UUID
		var eventType string
		var payload []byte
		if err := rows.Scan(&id, &eventType, &payload); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		e, err := events.DecodeSeatEvent(payload)
		if err != nil {
			t.Fatalf("payload does not decode: %v", err)
		}
		if e.MessageId != id.String() {
			t.Errorf("payload message_id %s differs from the row id %s; consumers dedupe on it", e.MessageId, id)
		}
		if want := "seat." + events.SeatStateName(e.State); eventType != want {
			t.Errorf("event_type = %q, want %q", eventType, want)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}
	return out
}

func sorted(ids []string) []string {
	c := slices.Clone(ids)
	slices.Sort(c)
	return c
}

func TestReserveConfirmRelease_EachWriteASeatEvent(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	eventID, seats := newEvent(t, s, "Events out", 4)
	order1, order2 := uuid.NewString(), uuid.NewString()

	if err := reserve(s, eventID, order1, seats[0], seats[1]); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := reserve(s, eventID, order2, seats[2]); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := s.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{EventId: eventID, SeatIds: seats[:2], OrderId: order1}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := s.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{EventId: eventID, SeatIds: seats[2:3], OrderId: order2}); err != nil {
		t.Fatalf("release: %v", err)
	}

	got := seatEventsFor(t, s, eventID)
	want := []struct {
		state eventsv1.SeatState
		seats []string
	}{
		{eventsv1.SeatState_SEAT_STATE_HELD, seats[:2]},
		{eventsv1.SeatState_SEAT_STATE_HELD, seats[2:3]},
		{eventsv1.SeatState_SEAT_STATE_SOLD, seats[:2]},
		{eventsv1.SeatState_SEAT_STATE_AVAILABLE, seats[2:3]},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d seat events, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].State != w.state || !slices.Equal(sorted(got[i].SeatIds), sorted(w.seats)) || got[i].EventId != eventID {
			t.Errorf("event %d = %v, want state %v for seats %v of event %s", i, got[i], w.state, w.seats, eventID)
		}
		if got[i].OccurredAt == nil {
			t.Errorf("event %d has no occurred_at", i)
		}
	}
}

// The outbox row is written by the same transaction as the seat change, so a
// change that does not happen must leave no event behind, or the live map would
// show seats as taken that are not.
func TestFailedSeatChanges_LeaveNoEventBehind(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	eventID, seats := newEvent(t, s, "No phantom events", 3)
	holder, other := uuid.NewString(), uuid.NewString()
	if err := reserve(s, eventID, holder, seats[0]); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	before := len(seatEventsFor(t, s, eventID))

	// Seat 0 is taken, so this whole reservation must fail...
	if err := reserve(s, eventID, other, seats[1], seats[0]); err == nil {
		t.Fatal("reserving a taken seat succeeded")
	}
	// ...someone who does not hold the seat cannot confirm or release it...
	if _, err := s.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{EventId: eventID, SeatIds: seats[:1], OrderId: other}); err == nil {
		t.Fatal("confirm by a non-holder succeeded")
	}
	if _, err := s.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{EventId: eventID, SeatIds: seats[:1], OrderId: other}); err == nil {
		t.Fatal("release by a non-holder succeeded")
	}
	// ...and a partly-valid confirm rolls back entirely.
	if _, err := s.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{EventId: eventID, SeatIds: []string{seats[0], seats[2]}, OrderId: holder}); err == nil {
		t.Fatal("confirming a seat the order does not hold succeeded")
	}

	if after := len(seatEventsFor(t, s, eventID)); after != before {
		t.Errorf("%d event(s) were written by failed operations", after-before)
	}
	if st := seatStatus(t, s, eventID, seats[0]); st != inventoryv1.SeatStatus_SEAT_STATUS_HELD {
		t.Errorf("seat 0 is %v, want it still HELD by its owner", st)
	}
}

func TestSeatsOfAnotherEventAreTreatedAsMissing(t *testing.T) {
	s, _ := newTestServer(t)
	eventA, seatsA := newEvent(t, s, "A", 1)
	eventB, _ := newEvent(t, s, "B", 1)

	if err := reserve(s, eventB, uuid.NewString(), seatsA[0]); err == nil {
		t.Fatal("reserved event A's seat under event B")
	}
	if st := seatStatus(t, s, eventA, seatsA[0]); st != inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE {
		t.Errorf("event A's seat is %v after a wrong-event attempt", st)
	}
	if n := len(seatEventsFor(t, s, eventB)) + len(seatEventsFor(t, s, eventA)); n != 0 {
		t.Errorf("%d event(s) emitted for a rejected request", n)
	}
}

func TestSweeper_EmitsOneAvailableEventPerEventForExpiredHolds(t *testing.T) {
	s, rdb := newTestServer(t)
	ctx := context.Background()
	eventA, seatsA := newEvent(t, s, "Sweep A", 3)
	eventB, seatsB := newEvent(t, s, "Sweep B", 2)
	if err := reserve(s, eventA, uuid.NewString(), seatsA[0], seatsA[1]); err != nil {
		t.Fatalf("reserve A: %v", err)
	}
	if err := reserve(s, eventB, uuid.NewString(), seatsB[0]); err != nil {
		t.Fatalf("reserve B: %v", err)
	}
	// Seat A0's hold is still alive in Redis; every other hold has expired.
	for _, id := range []string{seatsA[1], seatsB[0]} {
		if err := rdb.Del(ctx, holdKey(id)).Err(); err != nil {
			t.Fatalf("expire hold: %v", err)
		}
	}

	s.sweepExpiredHolds(ctx)

	a := seatEventsFor(t, s, eventA)
	last := a[len(a)-1]
	if last.State != eventsv1.SeatState_SEAT_STATE_AVAILABLE || !slices.Equal(last.SeatIds, []string{seatsA[1]}) {
		t.Errorf("event A's last seat event = %v, want AVAILABLE for only the expired seat %s", last, seatsA[1])
	}
	b := seatEventsFor(t, s, eventB)
	lastB := b[len(b)-1]
	if lastB.State != eventsv1.SeatState_SEAT_STATE_AVAILABLE || !slices.Equal(lastB.SeatIds, []string{seatsB[0]}) {
		t.Errorf("event B's last seat event = %v, want AVAILABLE for %s", lastB, seatsB[0])
	}
	if len(a) != 2 { // HELD, then the sweep's AVAILABLE; the live hold produced nothing more
		t.Errorf("event A has %d seat events, want 2", len(a))
	}
}
