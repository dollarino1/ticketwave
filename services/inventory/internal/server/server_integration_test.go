//go:build integration

package server

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/dollarino1/ticketwave/pkg/pgtest"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Run with INVENTORY_DATABASE_URL and REDIS_ADDR set, e.g.
//
//	INVENTORY_DATABASE_URL="postgres://ticketwave:ticketwave@localhost:5433/inventory?sslmode=disable" \
//	REDIS_ADDR=localhost:6379 go test -tags integration ./services/inventory/...
func newTestServer(t *testing.T) (*Server, *goredis.Client) {
	t.Helper()
	pool := pgtest.NewPool(t, "INVENTORY_DATABASE_URL", "migrations/inventory")

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	return New(pool, rdb), rdb
}

// newEvent creates an event and returns its ID with its seat IDs in label order.
func newEvent(t *testing.T, s *Server, name string, seats int32) (string, []string) {
	t.Helper()
	ctx := context.Background()
	created, err := s.CreateEvent(ctx, &inventoryv1.CreateEventRequest{Name: name, SeatCount: seats})
	if err != nil {
		t.Fatalf("CreateEvent: %v", err)
	}
	listed, err := s.ListSeats(ctx, &inventoryv1.ListSeatsRequest{EventId: created.EventId})
	if err != nil {
		t.Fatalf("ListSeats: %v", err)
	}
	ids := make([]string, len(listed.Seats))
	for i, seat := range listed.Seats {
		ids[i] = seat.Id
	}
	return created.EventId, ids
}

func seatStatus(t *testing.T, s *Server, eventID, seatID string) inventoryv1.SeatStatus {
	t.Helper()
	listed, err := s.ListSeats(context.Background(), &inventoryv1.ListSeatsRequest{EventId: eventID})
	if err != nil {
		t.Fatalf("ListSeats: %v", err)
	}
	for _, seat := range listed.Seats {
		if seat.Id == seatID {
			return seat.Status
		}
	}
	t.Fatalf("seat %s not found", seatID)
	return inventoryv1.SeatStatus_SEAT_STATUS_UNSPECIFIED
}

func reserve(s *Server, eventID, orderID string, seatIDs ...string) error {
	_, err := s.ReserveSeats(context.Background(), &inventoryv1.ReserveSeatsRequest{EventId: eventID, SeatIds: seatIDs, OrderId: orderID})
	return err
}

func TestCreateEvent_CreatesTheRequestedSeats(t *testing.T) {
	s, _ := newTestServer(t)

	eventID, seatIDs := newEvent(t, s, "  Jazz Night  ", 3)

	if len(seatIDs) != 3 {
		t.Fatalf("got %d seats, want 3", len(seatIDs))
	}
	got, err := s.GetEvent(context.Background(), &inventoryv1.GetEventRequest{EventId: eventID})
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if got.Event.Name != "Jazz Night" || got.Event.TotalSeats != 3 || got.Event.AvailableSeats != 3 {
		t.Errorf("event = %v, want trimmed name Jazz Night with 3 of 3 seats available", got.Event)
	}
	for _, id := range seatIDs {
		if st := seatStatus(t, s, eventID, id); st != inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE {
			t.Errorf("new seat %s is %v, want AVAILABLE", id, st)
		}
	}
}

func TestEvents_CountAvailableSeatsAndListNewestFirst(t *testing.T) {
	s, _ := newTestServer(t)
	older, olderSeats := newEvent(t, s, "Older", 4)
	time.Sleep(20 * time.Millisecond) // created_at must differ for the ordering to be meaningful
	newer, _ := newEvent(t, s, "Newer", 2)

	if err := reserve(s, older, uuid.NewString(), olderSeats[0], olderSeats[1]); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	listed, err := s.ListEvents(context.Background(), &inventoryv1.ListEventsRequest{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(listed.Events) != 2 || listed.Events[0].Id != newer || listed.Events[1].Id != older {
		t.Fatalf("events = %v, want the newer one first", listed.Events)
	}
	if got := listed.Events[1]; got.TotalSeats != 4 || got.AvailableSeats != 2 {
		t.Errorf("older event = %d of %d available, want 2 of 4 after reserving 2", got.AvailableSeats, got.TotalSeats)
	}

	one, err := s.ListEvents(context.Background(), &inventoryv1.ListEventsRequest{Limit: 1})
	if err != nil || len(one.Events) != 1 {
		t.Errorf("limit 1 returned %d events (err %v), want exactly 1", len(one.GetEvents()), err)
	}
}

func TestGetEvent_ErrorCodes(t *testing.T) {
	s, _ := newTestServer(t)
	cases := map[string]struct {
		id   string
		want codes.Code
	}{
		"unknown event": {uuid.NewString(), codes.NotFound},
		"not a UUID":    {"nope", codes.InvalidArgument},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.GetEvent(context.Background(), &inventoryv1.GetEventRequest{EventId: tc.id})
			if got := status.Code(err); got != tc.want {
				t.Errorf("code = %v (%v), want %v", got, err, tc.want)
			}
		})
	}
}

// The reason this whole service exists: many buyers, one seat, exactly one
// winner. Several seats are raced at once and every buyer is released from a
// starting gun, so buyers for the same seat genuinely overlap. Without the row
// lock in ReserveSeats this fails at once; a single seat with staggered buyers
// (an earlier version of this test) did not.
func TestReserveSeats_ExactlyOneOfManyConcurrentBuyersWins(t *testing.T) {
	s, rdb := newTestServer(t)
	const seatCount, buyersPerSeat = 10, 12
	eventID, seatIDs := newEvent(t, s, "Sold out fast", seatCount)
	pgtest.WarmPool(t, s.pool) // every buyer must really be simultaneous, or the race is hidden

	var mu sync.Mutex
	winners := make(map[string][]string) // seat -> orders that got it
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, seat := range seatIDs {
		for b := 0; b < buyersPerSeat; b++ {
			orderID := uuid.NewString()
			wg.Go(func() {
				<-start
				err := reserve(s, eventID, orderID, seat)
				switch status.Code(err) {
				case codes.OK:
					mu.Lock()
					winners[seat] = append(winners[seat], orderID)
					mu.Unlock()
				case codes.FailedPrecondition:
					// lost the race, as expected
				default:
					t.Errorf("unexpected error: %v", err)
				}
			})
		}
	}
	close(start)
	wg.Wait()

	ctx := context.Background()
	for _, seat := range seatIDs {
		t.Cleanup(func() { rdb.Del(ctx, holdKey(seat)) })

		if got := len(winners[seat]); got != 1 {
			t.Errorf("seat %s went to %d buyers, want exactly 1", seat, got)
			continue
		}
		if st := seatStatus(t, s, eventID, seat); st != inventoryv1.SeatStatus_SEAT_STATUS_HELD {
			t.Errorf("seat %s is %v, want HELD", seat, st)
		}
		// Redis holds the winner's order ID with a ~5 minute TTL.
		if got, err := rdb.Get(ctx, holdKey(seat)).Result(); err != nil || got != winners[seat][0] {
			t.Errorf("redis hold for %s = %q (err %v), want the winning order %q", seat, got, err, winners[seat][0])
		}
		if ttl, _ := rdb.TTL(ctx, holdKey(seat)).Result(); ttl < 290*time.Second || ttl > holdTTL {
			t.Errorf("hold TTL for %s = %v, want about %v", seat, ttl, holdTTL)
		}
	}
}

func TestReserveSeats_IsAllOrNothing(t *testing.T) {
	s, _ := newTestServer(t)
	eventID, seats := newEvent(t, s, "Pairs", 3)
	first, second := uuid.NewString(), uuid.NewString()

	if err := reserve(s, eventID, first, seats[0]); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// The second buyer wants seats 0 and 1, but seat 0 is taken.
	err := reserve(s, eventID, second, seats[0], seats[1])

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
	if st := seatStatus(t, s, eventID, seats[1]); st != inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE {
		t.Errorf("seat 1 is %v after a failed 2-seat reservation, want AVAILABLE: nothing may be half-reserved", st)
	}
}

func TestConfirmAndRelease_OnlyForTheOrderHoldingTheSeat(t *testing.T) {
	s, rdb := newTestServer(t)
	eventID, seats := newEvent(t, s, "Ownership", 2)
	owner, stranger := uuid.NewString(), uuid.NewString()
	ctx := context.Background()
	if err := reserve(s, eventID, owner, seats[0], seats[1]); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	t.Cleanup(func() { rdb.Del(ctx, holdKey(seats[0]), holdKey(seats[1])) })

	_, err := s.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{EventId: eventID, SeatIds: seats[:1], OrderId: stranger})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("stranger confirming got %v, want FailedPrecondition", err)
	}
	_, err = s.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{EventId: eventID, SeatIds: seats[:1], OrderId: stranger})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("stranger releasing got %v, want FailedPrecondition", err)
	}
	if st := seatStatus(t, s, eventID, seats[0]); st != inventoryv1.SeatStatus_SEAT_STATUS_HELD {
		t.Fatalf("seat is %v after the stranger's attempts, want still HELD", st)
	}

	if _, err := s.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{EventId: eventID, SeatIds: seats[:1], OrderId: owner}); err != nil {
		t.Fatalf("owner confirming: %v", err)
	}
	if st := seatStatus(t, s, eventID, seats[0]); st != inventoryv1.SeatStatus_SEAT_STATUS_SOLD {
		t.Errorf("confirmed seat is %v, want SOLD", st)
	}
	if n, _ := rdb.Exists(ctx, holdKey(seats[0])).Result(); n != 0 {
		t.Error("the redis hold survived confirmation")
	}

	if _, err := s.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{EventId: eventID, SeatIds: seats[1:], OrderId: owner}); err != nil {
		t.Fatalf("owner releasing: %v", err)
	}
	if st := seatStatus(t, s, eventID, seats[1]); st != inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE {
		t.Errorf("released seat is %v, want AVAILABLE", st)
	}
	if err := reserve(s, eventID, stranger, seats[1]); err != nil {
		t.Errorf("a released seat could not be reserved again: %v", err)
	}
	rdb.Del(ctx, holdKey(seats[1]))
}

func TestSweeper_ReleasesOnlyHoldsWhoseRedisKeyIsGone(t *testing.T) {
	s, rdb := newTestServer(t)
	eventID, seats := newEvent(t, s, "Expiry", 2)
	ctx := context.Background()
	expired, live := seats[0], seats[1]
	if err := reserve(s, eventID, uuid.NewString(), expired); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := reserve(s, eventID, uuid.NewString(), live); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	t.Cleanup(func() { rdb.Del(ctx, holdKey(expired), holdKey(live)) })

	rdb.Del(ctx, holdKey(expired)) // what Redis does when the 5 minute TTL runs out
	s.sweepExpiredHolds(ctx)

	if st := seatStatus(t, s, eventID, expired); st != inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE {
		t.Errorf("expired hold is %v, want the sweeper to have released it", st)
	}
	if st := seatStatus(t, s, eventID, live); st != inventoryv1.SeatStatus_SEAT_STATUS_HELD {
		t.Errorf("live hold is %v, want it left alone", st)
	}
}

func TestSeatRPCs_RejectMalformedInputBeforeTouchingTheDatabase(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	good := uuid.NewString()

	checks := map[string]error{
		"create with no name":      first(s.CreateEvent(ctx, &inventoryv1.CreateEventRequest{SeatCount: 5})),
		"create with 0 seats":      first(s.CreateEvent(ctx, &inventoryv1.CreateEventRequest{Name: "x"})),
		"list seats, bad event id": first(s.ListSeats(ctx, &inventoryv1.ListSeatsRequest{EventId: "nope"})),
		"reserve, bad seat id":     reserve(s, good, good, "nope"),
		"reserve, no seats":        reserve(s, good, good),
		"reserve, repeated seat":   reserve(s, good, good, good, good),
		"reserve, bad order id":    reserve(s, good, "nope", good),
		"confirm, bad seat id":     first(s.ConfirmSeats(ctx, &inventoryv1.ConfirmSeatsRequest{EventId: good, SeatIds: []string{"nope"}, OrderId: good})),
		"release, bad order id":    first(s.ReleaseSeats(ctx, &inventoryv1.ReleaseSeatsRequest{EventId: good, SeatIds: []string{good}, OrderId: "nope"})),
	}
	for name, err := range checks {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
}

// first drops a gRPC response and keeps only its error.
func first[T any](_ T, err error) error { return err }
