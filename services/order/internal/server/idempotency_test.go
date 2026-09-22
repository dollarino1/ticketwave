package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dollarino1/ticketwave/pkg/apierr"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidIdempotencyKey(t *testing.T) {
	cases := map[string]bool{
		"":                                     true, // no key: not idempotent, allowed
		"a":                                    true,
		"3f2b8c9e-1d4a-4c6b-9e7f-0a1b2c3d4e5f": true,
		"abc_DEF-123":                          true,
		strings.Repeat("a", 64):                true,
		strings.Repeat("a", 65):                false,
		"has space":                            false,
		"semi;colon":                           false,
		"new\nline":                            false,
		"quote'":                               false,
		"unicode-é":                            false,
	}
	for key, want := range cases {
		if got := validIdempotencyKey(key); got != want {
			t.Errorf("validIdempotencyKey(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestValidateCreateOrder_RejectsABadIdempotencyKey(t *testing.T) {
	req := validRequest()
	req.IdempotencyKey = "not valid!"

	err := validateCreateOrder(req)

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestSameRequest(t *testing.T) {
	req := validRequest()
	stored := storedOrder{EventID: req.EventId, SeatIDs: []string{req.SeatIds[1], req.SeatIds[0]}, AmountCents: 10000}

	cases := []struct {
		name   string
		mutate func(*storedOrder)
		want   bool
	}{
		{"identical, seats in another order", func(*storedOrder) {}, true},
		{"another event", func(o *storedOrder) { o.EventID = uuid.NewString() }, false},
		{"another price", func(o *storedOrder) { o.AmountCents = 5000 }, false},
		{"one seat swapped", func(o *storedOrder) { o.SeatIDs = []string{req.SeatIds[0], uuid.NewString()} }, false},
		{"one seat fewer", func(o *storedOrder) { o.SeatIDs = req.SeatIds[:1] }, false},
		{"one seat more", func(o *storedOrder) { o.SeatIDs = append([]string{uuid.NewString()}, o.SeatIDs...) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := stored
			o.SeatIDs = append([]string(nil), stored.SeatIDs...)
			tc.mutate(&o)
			if got := sameRequest(o, req); got != tc.want {
				t.Errorf("sameRequest = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSameRequest_DoesNotReorderTheCallersSeats(t *testing.T) {
	req := validRequest()
	before := append([]string(nil), req.SeatIds...)

	sameRequest(storedOrder{EventID: req.EventId, SeatIDs: req.SeatIds, AmountCents: 10000}, req)

	if fmt.Sprint(req.SeatIds) != fmt.Sprint(before) {
		t.Error("sameRequest sorted the request's seat list in place")
	}
}

func TestIsDuplicateKey(t *testing.T) {
	pgErr := func(code, constraint string) error {
		return fmt.Errorf("insert order: %w", &pgconn.PgError{Code: code, ConstraintName: constraint})
	}
	cases := map[string]struct {
		err  error
		want bool
	}{
		"the idempotency index":       {pgErr("23505", idempotencyIndex), true},
		"another unique constraint":   {pgErr("23505", "orders_pkey"), false},
		"another kind of violation":   {pgErr("23503", idempotencyIndex), false},
		"not a database error at all": {errors.New("boom"), false},
		"nil":                         {nil, false},
	}
	for name, tc := range cases {
		if got := isDuplicateKey(tc.err); got != tc.want {
			t.Errorf("%s: isDuplicateKey = %v, want %v", name, got, tc.want)
		}
	}
}

func TestFailureFor_RebuildsTheOriginalError(t *testing.T) {
	str := func(s string) *string { return &s }
	cases := []struct {
		name   string
		reason *string
		want   error
	}{
		{"seats", str("seats_unavailable"), errSeatsUnavailable()},
		{"declined", str("payment_declined"), errPaymentDeclined()},
		{"outage", str("service_unavailable"), errServiceUnavailable()},
		{"unspecified", str("unspecified"), errServiceUnavailable()},
		{"never recorded", nil, errServiceUnavailable()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := failureFor(tc.reason)
			if status.Code(got) != status.Code(tc.want) || apierr.Reason(got) != apierr.Reason(tc.want) || got.Error() != tc.want.Error() {
				t.Errorf("failureFor = %v (%s), want %v (%s)", got, apierr.Reason(got), tc.want, apierr.Reason(tc.want))
			}
		})
	}
}

func TestNullIfEmpty(t *testing.T) {
	if nullIfEmpty("") != nil {
		t.Error(`"" must be stored as NULL so the partial unique index ignores it`)
	}
	if got := nullIfEmpty("k"); got == nil || *got != "k" {
		t.Errorf("nullIfEmpty(k) = %v", got)
	}
}
