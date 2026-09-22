// Package feed turns seat events from Kafka into hub updates.
package feed

import (
	"log/slog"

	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/services/realtime/internal/hub"
)

// Publisher is the part of the hub the feed needs.
type Publisher interface {
	Publish(eventID string, u hub.Update)
}

// Handler returns the function to give kafka.Tail.
//
// A message that cannot be decoded is logged and skipped. There is no dead-letter
// topic here and there should not be: a live view has no use for an old update, and
// a poison message must not stop the ones behind it.
func Handler(p Publisher, log *slog.Logger) func(kafka.Message) {
	return func(m kafka.Message) {
		e, err := events.DecodeSeatEvent(m.Value)
		if err != nil {
			log.Warn("skipping an undecodable seat event",
				slog.Int64("offset", m.Offset), slog.Any("error", err))
			return
		}
		if e.GetEventId() == "" || len(e.GetSeatIds()) == 0 {
			log.Warn("skipping a seat event with no event or no seats", slog.Int64("offset", m.Offset))
			return
		}
		p.Publish(e.GetEventId(), hub.Update{
			SeatIDs: e.GetSeatIds(),
			State:   events.SeatStateName(e.GetState()),
		})
	}
}
