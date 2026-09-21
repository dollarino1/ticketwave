package server

import (
	"context"
	"fmt"
	"time"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// orderSnapshot is the order as it looked when a state change happened. Events
// carry it whole, so consumers never have to call order-svc back.
type orderSnapshot struct {
	ID          uuid.UUID
	UserID      string
	EventID     string
	SeatIDs     []string
	AmountCents int64
}

// buildOrderEvent returns the outbox row ID and JSON payload for one state
// change. The row ID doubles as the event's message_id, so a consumer that
// dedupes on message_id is deduping on the outbox row itself.
func buildOrderEvent(
	o orderSnapshot,
	typ eventsv1.OrderEventType,
	reason eventsv1.OrderFailureReason,
	now time.Time,
) (uuid.UUID, []byte, error) {
	id := uuid.New()
	payload, err := events.EncodeOrderEvent(&eventsv1.OrderEvent{
		MessageId:     id.String(),
		OccurredAt:    timestamppb.New(now),
		Type:          typ,
		OrderId:       o.ID.String(),
		UserId:        o.UserID,
		EventId:       o.EventID,
		SeatIds:       o.SeatIDs,
		AmountCents:   o.AmountCents,
		FailureReason: reason,
	})
	if err != nil {
		return uuid.Nil, nil, err
	}
	return id, payload, nil
}

// insertOutbox writes the event inside the caller's transaction. That is the
// whole point of the pattern: the order row and the intent to announce it commit
// or roll back together, so a crash can never leave one without the other.
func insertOutbox(
	ctx context.Context,
	tx pgx.Tx,
	o orderSnapshot,
	typ eventsv1.OrderEventType,
	reason eventsv1.OrderFailureReason,
) error {
	id, payload, err := buildOrderEvent(o, typ, reason, time.Now())
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, order_id, event_type, payload) VALUES ($1, $2, $3, $4)`,
		id, o.ID, events.EventTypeName(typ), payload,
	)
	if err != nil {
		return fmt.Errorf("insert outbox row: %w", err)
	}
	return nil
}
