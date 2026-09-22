// Package projection folds order events into per-concert statistics.
package projection

import (
	"context"
	"fmt"
	"log"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// delta is what one event adds to a concert's statistics.
type delta struct {
	ticketsSold  int64
	revenueCents int64
	confirmed    int64
	failed       int64
}

type change struct {
	messageID uuid.UUID
	eventID   uuid.UUID
	delta     delta
}

// parse validates an order event and works out what it does to the statistics.
// ok is false for events that change nothing, such as ORDER_CREATED.
//
// Anything that can never be processed comes back wrapped with kafka.Permanent,
// so the consumer dead-letters it at once instead of retrying.
func parse(msg kafka.Message) (c change, ok bool, err error) {
	ev, err := events.DecodeOrderEvent(msg.Value)
	if err != nil {
		return change{}, false, kafka.Permanent(err)
	}

	var d delta
	switch ev.GetType() {
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED:
		d = delta{ticketsSold: int64(len(ev.GetSeatIds())), revenueCents: ev.GetAmountCents(), confirmed: 1}
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED:
		d = delta{failed: 1}
	default:
		return change{}, false, nil
	}

	messageID, err := uuid.Parse(ev.GetMessageId())
	if err != nil {
		return change{}, false, kafka.Permanent(fmt.Errorf("invalid message_id %q: %w", ev.GetMessageId(), err))
	}
	eventID, err := uuid.Parse(ev.GetEventId())
	if err != nil {
		return change{}, false, kafka.Permanent(fmt.Errorf("invalid event_id %q: %w", ev.GetEventId(), err))
	}
	return change{messageID: messageID, eventID: eventID, delta: d}, true, nil
}

type Projector struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Projector {
	return &Projector{pool: pool}
}

// Handle applies one order event. It is idempotent: handling the same message
// any number of times, even concurrently, counts it exactly once.
func (p *Projector) Handle(ctx context.Context, msg kafka.Message) error {
	c, ok, err := parse(msg)
	if err != nil || !ok {
		return err
	}
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		return apply(ctx, tx, c)
	})
}

// apply records the message and updates the counters in one transaction. That
// is the difference from notification-svc, whose "already sent" marker lives in
// Redis, outside the side effect, so a crash between the two can still repeat
// an email. Here the marker and the counters share a transaction, so they
// commit together or not at all: a redelivery never double-counts, and a
// failure never leaves a message marked as counted when it was not.
func apply(ctx context.Context, tx pgx.Tx, c change) error {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		c.messageID,
	)
	if err != nil {
		return fmt.Errorf("record message: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Printf("skipping already-counted message %s", c.messageID)
		return nil
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO event_stats (event_id, tickets_sold, revenue_cents, orders_confirmed, orders_failed)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (event_id) DO UPDATE SET
		     tickets_sold     = event_stats.tickets_sold     + EXCLUDED.tickets_sold,
		     revenue_cents    = event_stats.revenue_cents    + EXCLUDED.revenue_cents,
		     orders_confirmed = event_stats.orders_confirmed + EXCLUDED.orders_confirmed,
		     orders_failed    = event_stats.orders_failed    + EXCLUDED.orders_failed,
		     updated_at       = now()`,
		c.eventID, c.delta.ticketsSold, c.delta.revenueCents, c.delta.confirmed, c.delta.failed,
	)
	if err != nil {
		return fmt.Errorf("update event stats: %w", err)
	}
	return nil
}
