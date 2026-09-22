package projection

import (
	"testing"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/google/uuid"
)

const (
	testMessageID = "8f14e45f-ceea-467a-9575-2d2f1e0c1a11"
	testEventID   = "1c6e1f2a-0000-4000-8000-000000000003"
)

func message(t *testing.T, ev *eventsv1.OrderEvent) kafka.Message {
	t.Helper()
	b, err := events.EncodeOrderEvent(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return kafka.Message{Value: b}
}

func orderEvent(typ eventsv1.OrderEventType) *eventsv1.OrderEvent {
	return &eventsv1.OrderEvent{
		MessageId:   testMessageID,
		Type:        typ,
		OrderId:     "order-1",
		UserId:      "user-1",
		EventId:     testEventID,
		SeatIds:     []string{"a", "b", "c"},
		AmountCents: 7500,
	}
}

func TestParse_ConfirmedCountsSeatsAndRevenue(t *testing.T) {
	c, ok, err := parse(message(t, orderEvent(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)))

	if err != nil || !ok {
		t.Fatalf("parse returned ok=%v err=%v, want ok and no error", ok, err)
	}
	if c.messageID != uuid.MustParse(testMessageID) || c.eventID != uuid.MustParse(testEventID) {
		t.Errorf("ids = %s/%s, want the event's own", c.messageID, c.eventID)
	}
	want := delta{ticketsSold: 3, revenueCents: 7500, confirmed: 1}
	if c.delta != want {
		t.Errorf("delta = %+v, want %+v", c.delta, want)
	}
}

func TestParse_FailedOnlyCountsAsFailed(t *testing.T) {
	c, ok, err := parse(message(t, orderEvent(eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED)))

	if err != nil || !ok {
		t.Fatalf("parse returned ok=%v err=%v, want ok and no error", ok, err)
	}
	if want := (delta{failed: 1}); c.delta != want {
		t.Errorf("delta = %+v, want %+v: a failed order sells no tickets and earns nothing", c.delta, want)
	}
}

func TestParse_EventsThatChangeNothingAreSkipped(t *testing.T) {
	for _, typ := range []eventsv1.OrderEventType{
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED,
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_UNSPECIFIED,
	} {
		_, ok, err := parse(message(t, orderEvent(typ)))
		if ok || err != nil {
			t.Errorf("%v: parse returned ok=%v err=%v, want not ok and no error", typ, ok, err)
		}
	}
}

func TestParse_UnprocessableMessagesArePermanent(t *testing.T) {
	badMessageID := orderEvent(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)
	badMessageID.MessageId = "not-a-uuid"
	badEventID := orderEvent(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)
	badEventID.EventId = ""

	cases := map[string]kafka.Message{
		"malformed JSON":        {Value: []byte("not json")},
		"message_id not a UUID": message(t, badMessageID),
		"event_id missing":      message(t, badEventID),
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			_, ok, err := parse(msg)
			if ok || err == nil || !kafka.IsPermanent(err) {
				t.Fatalf("parse returned ok=%v err=%v, want a permanent error: retrying can never fix this", ok, err)
			}
		})
	}
}
