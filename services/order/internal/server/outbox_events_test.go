package server

import (
	"testing"
	"time"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/google/uuid"
)

func testOrder() orderSnapshot {
	return orderSnapshot{
		ID:          uuid.MustParse("1c6e1f2a-0000-4000-8000-000000000001"),
		UserID:      "1c6e1f2a-0000-4000-8000-000000000002",
		EventID:     "1c6e1f2a-0000-4000-8000-000000000003",
		SeatIDs:     []string{"seat-a", "seat-b"},
		AmountCents: 5000,
	}
}

func TestBuildOrderEvent_CarriesTheWholeOrder(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	o := testOrder()

	id, payload, err := buildOrderEvent(o, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED,
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED, now)
	if err != nil {
		t.Fatalf("buildOrderEvent: %v", err)
	}

	got, err := events.DecodeOrderEvent(payload)
	if err != nil {
		t.Fatalf("payload does not decode: %v", err)
	}
	if got.GetMessageId() != id.String() {
		t.Errorf("message_id = %q, want the outbox row ID %q", got.GetMessageId(), id)
	}
	if got.GetType() != eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED {
		t.Errorf("type = %v, want CONFIRMED", got.GetType())
	}
	if got.GetOrderId() != o.ID.String() || got.GetUserId() != o.UserID || got.GetEventId() != o.EventID {
		t.Errorf("identifiers = %v/%v/%v, want the order's own", got.GetOrderId(), got.GetUserId(), got.GetEventId())
	}
	if len(got.GetSeatIds()) != 2 || got.GetAmountCents() != 5000 {
		t.Errorf("seats/amount = %v/%d, want 2 seats and 5000", got.GetSeatIds(), got.GetAmountCents())
	}
	if !got.GetOccurredAt().AsTime().Equal(now) {
		t.Errorf("occurred_at = %v, want %v", got.GetOccurredAt().AsTime(), now)
	}
	if got.GetFailureReason() != eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED {
		t.Errorf("failure_reason = %v, want unset on a non-failure event", got.GetFailureReason())
	}
}

func TestBuildOrderEvent_FailureCarriesReason(t *testing.T) {
	_, payload, err := buildOrderEvent(testOrder(), eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED,
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED, time.Now())
	if err != nil {
		t.Fatalf("buildOrderEvent: %v", err)
	}

	got, err := events.DecodeOrderEvent(payload)
	if err != nil {
		t.Fatalf("payload does not decode: %v", err)
	}
	if got.GetFailureReason() != eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED {
		t.Errorf("failure_reason = %v, want PAYMENT_DECLINED", got.GetFailureReason())
	}
}

func TestBuildOrderEvent_EveryCallGetsAFreshMessageID(t *testing.T) {
	a, _, _ := buildOrderEvent(testOrder(), eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED,
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED, time.Now())
	b, _, _ := buildOrderEvent(testOrder(), eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED,
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED, time.Now())
	if a == b {
		t.Fatal("two events got the same message ID; consumers would drop the second as a duplicate")
	}
}
