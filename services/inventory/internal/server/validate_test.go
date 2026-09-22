package server

import (
	"strings"
	"testing"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateCreateEvent(t *testing.T) {
	cases := map[string]struct {
		name  string
		seats int32
		ok    bool
	}{
		"normal":                  {"Jazz Night", 100, true},
		"name is trimmed first":   {"  Jazz  ", 10, true},
		"one seat is enough":      {"Solo", 1, true},
		"largest allowed venue":   {"Arena", maxSeatsPerEvent, true},
		"empty name":              {"", 10, false},
		"blank name":              {"   ", 10, false},
		"name too long":           {strings.Repeat("x", maxEventNameLen+1), 10, false},
		"longest allowed name":    {strings.Repeat("x", maxEventNameLen), 10, true},
		"multi-byte counts runes": {strings.Repeat("é", maxEventNameLen), 10, true},
		"zero seats":              {"Jazz", 0, false},
		"negative seats":          {"Jazz", -3, false},
		"too many seats":          {"Jazz", maxSeatsPerEvent + 1, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateCreateEvent(&inventoryv1.CreateEventRequest{Name: tc.name, SeatCount: tc.seats})
			if tc.ok && err != nil {
				t.Errorf("got %v, want no error", err)
			}
			if !tc.ok && status.Code(err) != codes.InvalidArgument {
				t.Errorf("got %v, want InvalidArgument", err)
			}
		})
	}
}

func TestValidateSeatRequest(t *testing.T) {
	event, order := uuid.NewString(), uuid.NewString()
	seatA, seatB := uuid.NewString(), uuid.NewString()

	tooMany := make([]string, maxSeatsPerOrder+1)
	for i := range tooMany {
		tooMany[i] = uuid.NewString()
	}

	cases := map[string]struct {
		event, order string
		seats        []string
		ok           bool
	}{
		"valid":                 {event, order, []string{seatA, seatB}, true},
		"bad event id":          {"nope", order, []string{seatA}, false},
		"bad order id":          {event, "nope", []string{seatA}, false},
		"empty order id":        {event, "", []string{seatA}, false},
		"no seats":              {event, order, nil, false},
		"too many seats":        {event, order, tooMany, false},
		"a seat that is no id":  {event, order, []string{seatA, "nope"}, false},
		"the same seat twice":   {event, order, []string{seatA, seatA}, false},
		"same seat, other case": {event, order, []string{seatA, strings.ToUpper(seatA)}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateSeatRequest(tc.event, tc.seats, tc.order)
			if tc.ok && err != nil {
				t.Errorf("got %v, want no error", err)
			}
			if !tc.ok && status.Code(err) != codes.InvalidArgument {
				t.Errorf("got %v, want InvalidArgument", err)
			}
		})
	}
}

func TestClampEventLimit(t *testing.T) {
	cases := map[int32]int32{-1: defaultEventLimit, 0: defaultEventLimit, 5: 5, 200: 200, 201: maxEventLimit, 99999: maxEventLimit}
	for in, want := range cases {
		if got := clampEventLimit(in); got != want {
			t.Errorf("clampEventLimit(%d) = %d, want %d", in, got, want)
		}
	}
}
