package server

import (
	"context"
	"fmt"
	"time"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// execer is what emitSeatEvent needs from a transaction. Taking this instead of
// pgx.Tx makes it impossible to call with the pool by accident: the whole point is
// that the event row is written by the SAME transaction as the seat change.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// emitSeatEvent records, inside tx, that seatIDs of eventID entered state. It is
// the outbox half of the pattern; pkg/outbox later relays the row to Kafka.
func emitSeatEvent(ctx context.Context, tx execer, eventID string, seatIDs []string, state eventsv1.SeatState) error {
	if len(seatIDs) == 0 {
		return nil
	}
	id := uuid.New()
	payload, err := events.EncodeSeatEvent(&eventsv1.SeatEvent{
		MessageId:  id.String(),
		OccurredAt: timestamppb.New(time.Now()),
		EventId:    eventID,
		SeatIds:    seatIDs,
		State:      state,
	})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (id, event_id, event_type, payload) VALUES ($1, $2, $3, $4)`,
		id, eventID, "seat."+events.SeatStateName(state), payload,
	); err != nil {
		return fmt.Errorf("insert seat event into outbox: %w", err)
	}
	return nil
}
