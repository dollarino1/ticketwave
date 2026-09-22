// Package events holds what every producer and consumer of order events must
// agree on: topic names, header names, and how an event is encoded on the wire.
package events

import (
	"fmt"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	TopicOrderEvents    = "order-events"
	TopicOrderEventsDLQ = "order-events.dlq"

	HeaderMessageID = "message-id"
	HeaderEventType = "event-type"
)

// EncodeOrderEvent writes the event as protobuf-JSON. JSON rather than binary
// protobuf so the outbox's JSONB column can hold it and kafka-ui can show it.
func EncodeOrderEvent(e *eventsv1.OrderEvent) ([]byte, error) {
	b, err := protojson.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode order event: %w", err)
	}
	return b, nil
}

// DecodeOrderEvent discards unknown fields so a producer that adds a field does
// not break consumers that have not been redeployed yet.
func DecodeOrderEvent(b []byte) (*eventsv1.OrderEvent, error) {
	var e eventsv1.OrderEvent
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("decode order event: %w", err)
	}
	return &e, nil
}

// EventTypeName is the value stored in outbox.event_type and sent in the
// event-type header, so a consumer can filter without parsing the body.
func EventTypeName(t eventsv1.OrderEventType) string {
	switch t {
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED:
		return "order.created"
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED:
		return "order.confirmed"
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED:
		return "order.failed"
	default:
		return "order.unknown"
	}
}

const TopicSeatEvents = "seat-events"

// EncodeSeatEvent writes the event as protobuf-JSON, for the same reasons as
// EncodeOrderEvent: the outbox column is JSONB and kafka-ui can show it.
func EncodeSeatEvent(e *eventsv1.SeatEvent) ([]byte, error) {
	b, err := protojson.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode seat event: %w", err)
	}
	return b, nil
}

// DecodeSeatEvent discards unknown fields so that adding a field to the event
// does not break consumers that have not been redeployed yet.
func DecodeSeatEvent(b []byte) (*eventsv1.SeatEvent, error) {
	var e eventsv1.SeatEvent
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("decode seat event: %w", err)
	}
	return &e, nil
}

// SeatStateName is the plain word used in the event-type header and, later, on the
// wire to browsers: "available", "held" or "sold".
func SeatStateName(s eventsv1.SeatState) string {
	switch s {
	case eventsv1.SeatState_SEAT_STATE_AVAILABLE:
		return "available"
	case eventsv1.SeatState_SEAT_STATE_HELD:
		return "held"
	case eventsv1.SeatState_SEAT_STATE_SOLD:
		return "sold"
	default:
		return "unknown"
	}
}
