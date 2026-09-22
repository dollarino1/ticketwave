package feed

import (
	"log/slog"
	"testing"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/services/realtime/internal/hub"
)

type published struct {
	eventID string
	update  hub.Update
}

type recorder struct{ got []published }

func (r *recorder) Publish(eventID string, u hub.Update) {
	r.got = append(r.got, published{eventID, u})
}

func encode(t *testing.T, e *eventsv1.SeatEvent) kafka.Message {
	t.Helper()
	b, err := events.EncodeSeatEvent(e)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: b}
}

func TestHandler_TurnsASeatEventIntoAnUpdateForItsEvent(t *testing.T) {
	cases := map[eventsv1.SeatState]string{
		eventsv1.SeatState_SEAT_STATE_AVAILABLE: "available",
		eventsv1.SeatState_SEAT_STATE_HELD:      "held",
		eventsv1.SeatState_SEAT_STATE_SOLD:      "sold",
	}
	for state, want := range cases {
		t.Run(want, func(t *testing.T) {
			rec := &recorder{}
			Handler(rec, slog.New(slog.DiscardHandler))(encode(t, &eventsv1.SeatEvent{
				EventId: "e1", SeatIds: []string{"s1", "s2"}, State: state,
			}))

			if len(rec.got) != 1 {
				t.Fatalf("published %d updates, want 1", len(rec.got))
			}
			g := rec.got[0]
			if g.eventID != "e1" || g.update.State != want || len(g.update.SeatIDs) != 2 {
				t.Errorf("published %+v, want event e1, state %s, 2 seats", g, want)
			}
		})
	}
}

// A live view has no use for a broken message and must not stop for one.
func TestHandler_SkipsMessagesItCannotUse(t *testing.T) {
	cases := map[string]kafka.Message{
		"not JSON":       {Value: []byte("garbage")},
		"empty":          {Value: nil},
		"no event id":    encode(t, &eventsv1.SeatEvent{SeatIds: []string{"s1"}, State: eventsv1.SeatState_SEAT_STATE_HELD}),
		"no seats":       encode(t, &eventsv1.SeatEvent{EventId: "e1", State: eventsv1.SeatState_SEAT_STATE_HELD}),
		"an order event": {Value: []byte(`{"order_id":"o1","type":"ORDER_EVENT_TYPE_CONFIRMED"}`)},
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			rec := &recorder{}
			Handler(rec, slog.New(slog.DiscardHandler))(msg)
			if len(rec.got) != 0 {
				t.Errorf("published %+v for an unusable message", rec.got)
			}
		})
	}
}

func TestHandler_ToleratesFieldsAddedLater(t *testing.T) {
	rec := &recorder{}
	msg := kafka.Message{Value: []byte(`{"event_id":"e1","seat_ids":["s1"],"state":"SEAT_STATE_SOLD","some_future_field":42}`)}

	Handler(rec, slog.New(slog.DiscardHandler))(msg)

	if len(rec.got) != 1 || rec.got[0].update.State != "sold" {
		t.Errorf("published %+v, want the update despite the unknown field", rec.got)
	}
}
