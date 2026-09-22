package server

import (
	"testing"

	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validRequest() *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{
		UserId:      uuid.NewString(),
		EventId:     uuid.NewString(),
		SeatIds:     []string{uuid.NewString(), uuid.NewString()},
		AmountCents: 10000,
	}
}

func TestValidateCreateOrder_AcceptsAWellFormedOrder(t *testing.T) {
	if err := validateCreateOrder(validRequest()); err != nil {
		t.Errorf("a valid order was rejected: %v", err)
	}
}

func TestValidateCreateOrder_RejectsMalformedOrders(t *testing.T) {
	tooMany := make([]string, maxSeatsPerOrder+1)
	for i := range tooMany {
		tooMany[i] = uuid.NewString()
	}
	seat := uuid.NewString()

	cases := map[string]func(*orderv1.CreateOrderRequest){
		"user is not a UUID":    func(r *orderv1.CreateOrderRequest) { r.UserId = "nope" },
		"user is empty":         func(r *orderv1.CreateOrderRequest) { r.UserId = "" },
		"event is not a UUID":   func(r *orderv1.CreateOrderRequest) { r.EventId = "nope" },
		"no seats":              func(r *orderv1.CreateOrderRequest) { r.SeatIds = nil },
		"too many seats":        func(r *orderv1.CreateOrderRequest) { r.SeatIds = tooMany },
		"a seat is not a UUID":  func(r *orderv1.CreateOrderRequest) { r.SeatIds = []string{seat, "nope"} },
		"the same seat twice":   func(r *orderv1.CreateOrderRequest) { r.SeatIds = []string{seat, seat} },
		"zero amount":           func(r *orderv1.CreateOrderRequest) { r.AmountCents = 0 },
		"negative amount":       func(r *orderv1.CreateOrderRequest) { r.AmountCents = -1 },
		"absurdly large amount": func(r *orderv1.CreateOrderRequest) { r.AmountCents = maxAmountCents + 1 },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			req := validRequest()
			breakIt(req)
			if err := validateCreateOrder(req); status.Code(err) != codes.InvalidArgument {
				t.Errorf("got %v, want InvalidArgument", err)
			}
		})
	}
}

// The saga must tell "a service said no" from "a service broke". Only the first
// is a decision about the order.
func TestIsRefusal(t *testing.T) {
	cases := map[codes.Code]bool{
		codes.FailedPrecondition: true,
		codes.Unavailable:        false,
		codes.DeadlineExceeded:   false,
		codes.Internal:           false,
		codes.Unknown:            false,
		codes.InvalidArgument:    false,
		codes.NotFound:           false,
	}
	for code, want := range cases {
		if got := isRefusal(status.Error(code, "x")); got != want {
			t.Errorf("isRefusal(%v) = %v, want %v", code, got, want)
		}
	}
}
