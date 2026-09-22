//go:build integration

package projection

import (
	"context"
	"sync"
	"testing"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/pkg/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Run with ANALYTICS_DATABASE_URL pointing at the analytics database, e.g.
//
//	ANALYTICS_DATABASE_URL="postgres://ticketwave:ticketwave@localhost:5433/analytics?sslmode=disable" \
//	  go test -tags integration ./services/analytics/...
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return pgtest.NewPool(t, "ANALYTICS_DATABASE_URL", "migrations/analytics")
}

// newMessage builds an order event with a fresh message ID, so each call is a
// distinct event. Reuse the returned Message to simulate a redelivery.
func newMessage(t *testing.T, typ eventsv1.OrderEventType, eventID uuid.UUID, seats int, amountCents int64) kafka.Message {
	t.Helper()
	seatIDs := make([]string, seats)
	for i := range seatIDs {
		seatIDs[i] = uuid.NewString()
	}
	b, err := events.EncodeOrderEvent(&eventsv1.OrderEvent{
		MessageId:   uuid.NewString(),
		Type:        typ,
		OrderId:     uuid.NewString(),
		UserId:      uuid.NewString(),
		EventId:     eventID.String(),
		SeatIds:     seatIDs,
		AmountCents: amountCents,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return kafka.Message{Value: b}
}

type stats struct {
	found                               bool
	tickets, revenue, confirmed, failed int64
}

func statsFor(t *testing.T, pool *pgxpool.Pool, eventID uuid.UUID) stats {
	t.Helper()
	var s stats
	err := pool.QueryRow(context.Background(),
		`SELECT tickets_sold, revenue_cents, orders_confirmed, orders_failed FROM event_stats WHERE event_id = $1`,
		eventID).Scan(&s.tickets, &s.revenue, &s.confirmed, &s.failed)
	if err == nil {
		s.found = true
	} else if err != pgx.ErrNoRows {
		t.Fatalf("query stats: %v", err)
	}
	return s
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestHandle_ConfirmedOrdersAccumulate(t *testing.T) {
	pool := newPool(t)
	p := New(pool)
	eventID := uuid.New()

	for _, m := range []kafka.Message{
		newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventID, 2, 5000),
		newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventID, 3, 7000),
	} {
		if err := p.Handle(context.Background(), m); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	want := stats{found: true, tickets: 5, revenue: 12000, confirmed: 2}
	if got := statsFor(t, pool, eventID); got != want {
		t.Errorf("stats = %+v, want %+v", got, want)
	}
}

func TestHandle_FailedOrderOnlyCountsAsFailed(t *testing.T) {
	pool := newPool(t)
	eventID := uuid.New()

	err := New(pool).Handle(context.Background(),
		newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED, eventID, 2, 5000))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if got, want := statsFor(t, pool, eventID), (stats{found: true, failed: 1}); got != want {
		t.Errorf("stats = %+v, want %+v", got, want)
	}
}

func TestHandle_CreatedEventsChangeNothing(t *testing.T) {
	pool := newPool(t)
	eventID := uuid.New()

	err := New(pool).Handle(context.Background(),
		newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED, eventID, 2, 5000))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if statsFor(t, pool, eventID).found || countRows(t, pool, "processed_messages") != 0 {
		t.Error("an ORDER_CREATED event left a trace; it must change nothing")
	}
}

func TestHandle_RedeliveredMessageIsCountedOnce(t *testing.T) {
	pool := newPool(t)
	p := New(pool)
	eventID := uuid.New()
	msg := newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventID, 2, 5000)

	for i := 0; i < 3; i++ {
		if err := p.Handle(context.Background(), msg); err != nil {
			t.Fatalf("Handle #%d: %v", i+1, err)
		}
	}

	if got, want := statsFor(t, pool, eventID), (stats{found: true, tickets: 2, revenue: 5000, confirmed: 1}); got != want {
		t.Errorf("stats = %+v after 3 deliveries of one message, want %+v", got, want)
	}
}

func TestHandle_ConcurrentDistinctMessagesAllCount(t *testing.T) {
	pool := newPool(t)
	p := New(pool)
	eventID := uuid.New()

	const messages, workers = 50, 8
	var msgs []kafka.Message
	for i := 0; i < messages; i++ {
		msgs = append(msgs, newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventID, 2, 100))
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := w; i < messages; i += workers {
				if err := p.Handle(context.Background(), msgs[i]); err != nil {
					t.Errorf("Handle: %v", err)
				}
			}
		})
	}
	wg.Wait()

	// Many transactions update one row at once; no increment may be lost.
	want := stats{found: true, tickets: messages * 2, revenue: messages * 100, confirmed: messages}
	if got := statsFor(t, pool, eventID); got != want {
		t.Errorf("stats = %+v, want %+v", got, want)
	}
}

func TestHandle_ConcurrentDuplicatesCountOnce(t *testing.T) {
	pool := newPool(t)
	p := New(pool)
	eventID := uuid.New()
	msg := newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventID, 2, 5000)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Go(func() {
			if err := p.Handle(context.Background(), msg); err != nil {
				t.Errorf("Handle: %v", err)
			}
		})
	}
	wg.Wait()

	if got, want := statsFor(t, pool, eventID), (stats{found: true, tickets: 2, revenue: 5000, confirmed: 1}); got != want {
		t.Errorf("stats = %+v after 8 simultaneous deliveries of one message, want %+v", got, want)
	}
}

// If updating the counters fails, the message must not stay marked as counted.
// Otherwise its redelivery would be skipped and the event silently lost forever.
func TestHandle_FailureRollsBackTheDedupMarker(t *testing.T) {
	pool := newPool(t)
	p := New(pool)
	eventID := uuid.New()
	ctx := context.Background()

	// Force the counter update to fail for a big order.
	if _, err := pool.Exec(ctx, `ALTER TABLE event_stats ADD CONSTRAINT test_cap CHECK (tickets_sold < 1000)`); err != nil {
		t.Fatal(err)
	}
	msg := newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventID, 1000, 5000)

	if err := p.Handle(ctx, msg); err == nil {
		t.Fatal("Handle succeeded despite the failing constraint, want an error")
	}
	if n := countRows(t, pool, "processed_messages"); n != 0 {
		t.Fatalf("%d message(s) stayed marked as counted after the update failed; the redelivery would be skipped", n)
	}

	// The fault clears (say, a deploy fixed it). The redelivered message must now count.
	if _, err := pool.Exec(ctx, `ALTER TABLE event_stats DROP CONSTRAINT test_cap`); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, msg); err != nil {
		t.Fatalf("Handle on redelivery: %v", err)
	}
	if got, want := statsFor(t, pool, eventID), (stats{found: true, tickets: 1000, revenue: 5000, confirmed: 1}); got != want {
		t.Errorf("stats = %+v after the redelivery, want %+v", got, want)
	}
}

func TestHandle_ConcertsAreCountedSeparately(t *testing.T) {
	pool := newPool(t)
	p := New(pool)
	a, b := uuid.New(), uuid.New()

	for _, m := range []kafka.Message{
		newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, a, 1, 1000),
		newMessage(t, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, b, 4, 9000),
	} {
		if err := p.Handle(context.Background(), m); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	if got, want := statsFor(t, pool, a), (stats{found: true, tickets: 1, revenue: 1000, confirmed: 1}); got != want {
		t.Errorf("concert a = %+v, want %+v", got, want)
	}
	if got, want := statsFor(t, pool, b), (stats{found: true, tickets: 4, revenue: 9000, confirmed: 1}); got != want {
		t.Errorf("concert b = %+v, want %+v", got, want)
	}
}
