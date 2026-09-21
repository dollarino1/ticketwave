package events

import (
	"testing"
	"time"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOrderEvent_RoundTrips(t *testing.T) {
	want := &eventsv1.OrderEvent{
		MessageId:     "8f14e45f-ceea-467a-9575-2d2f1e0c1a11",
		OccurredAt:    timestamppb.New(time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)),
		Type:          eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED,
		OrderId:       "1c6e1f2a-0000-4000-8000-000000000001",
		UserId:        "1c6e1f2a-0000-4000-8000-000000000002",
		EventId:       "1c6e1f2a-0000-4000-8000-000000000003",
		SeatIds:       []string{"seat-a", "seat-b"},
		AmountCents:   2100,
		FailureReason: eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED,
	}

	b, err := EncodeOrderEvent(want)
	if err != nil {
		t.Fatalf("EncodeOrderEvent: %v", err)
	}
	got, err := DecodeOrderEvent(b)
	if err != nil {
		t.Fatalf("DecodeOrderEvent: %v", err)
	}

	if !proto.Equal(want, got) {
		t.Errorf("round trip changed the event:\n got  %v\n want %v", got, want)
	}
}

func TestDecodeOrderEvent_IgnoresUnknownFields(t *testing.T) {
	// A newer producer added "coupon"; an older consumer must still read the event.
	in := []byte(`{"messageId":"m1","orderId":"o1","coupon":"SPRING","type":"ORDER_EVENT_TYPE_CREATED"}`)

	got, err := DecodeOrderEvent(in)
	if err != nil {
		t.Fatalf("DecodeOrderEvent: %v", err)
	}
	if got.GetMessageId() != "m1" || got.GetOrderId() != "o1" {
		t.Errorf("decoded %v, want message m1 / order o1", got)
	}
	if got.GetType() != eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED {
		t.Errorf("type = %v, want CREATED", got.GetType())
	}
}

func TestDecodeOrderEvent_RejectsGarbage(t *testing.T) {
	if _, err := DecodeOrderEvent([]byte("not json")); err == nil {
		t.Fatal("DecodeOrderEvent accepted garbage, want an error")
	}
}

func TestEventTypeName(t *testing.T) {
	cases := map[eventsv1.OrderEventType]string{
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED:     "order.created",
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED:   "order.confirmed",
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED:      "order.failed",
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_UNSPECIFIED: "order.unknown",
	}
	for in, want := range cases {
		if got := EventTypeName(in); got != want {
			t.Errorf("EventTypeName(%v) = %q, want %q", in, got, want)
		}
	}
}
