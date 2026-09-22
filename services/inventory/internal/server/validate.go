package server

import (
	"strings"
	"unicode/utf8"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxEventNameLen  = 100
	maxSeatsPerEvent = 500
	maxSeatsPerOrder = 10
)

// Validation lives here, at the edge of the service, so a malformed request is
// rejected with InvalidArgument before it reaches Postgres. Without it a value
// that is not a UUID surfaces as a database error and, further up, as a 500.

func invalid(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

func validateCreateEvent(req *inventoryv1.CreateEventRequest) error {
	name := strings.TrimSpace(req.GetName())
	if name == "" || utf8.RuneCountInString(name) > maxEventNameLen {
		return invalid("name must be 1 to %d characters", maxEventNameLen)
	}
	if req.GetSeatCount() < 1 || req.GetSeatCount() > maxSeatsPerEvent {
		return invalid("seat_count must be between 1 and %d", maxSeatsPerEvent)
	}
	return nil
}

func validateEventID(id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return invalid("event_id must be a UUID")
	}
	return nil
}

func validateSeatRequest(eventID string, seatIDs []string, orderID string) error {
	if err := validateEventID(eventID); err != nil {
		return err
	}
	if _, err := uuid.Parse(orderID); err != nil {
		return invalid("order_id must be a UUID")
	}
	if len(seatIDs) == 0 || len(seatIDs) > maxSeatsPerOrder {
		return invalid("seat_ids must contain 1 to %d seats", maxSeatsPerOrder)
	}
	// Keyed by the parsed value, so "AAAA…" and "aaaa…" count as the same seat.
	seen := make(map[uuid.UUID]struct{}, len(seatIDs))
	for _, raw := range seatIDs {
		id, err := uuid.Parse(raw)
		if err != nil {
			return invalid("seat_ids must all be UUIDs")
		}
		if _, dup := seen[id]; dup {
			return invalid("seat_ids must not repeat a seat")
		}
		seen[id] = struct{}{}
	}
	return nil
}
