//go:build integration

package server

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	paymentv1 "github.com/dollarino1/ticketwave/gen/ticketwave/payment/v1"
	"github.com/dollarino1/ticketwave/pkg/apierr"
	"github.com/dollarino1/ticketwave/pkg/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callLog records which downstream calls the saga made, and in what order,
// across both fake services.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (l *callLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

// The fakes embed the generated client interface so only the methods the saga
// uses need writing; calling any other would panic on the nil embedded value.
type fakeInventory struct {
	inventoryv1.InventoryServiceClient
	log                                *callLog
	reserveErr, confirmErr, releaseErr error
}

func (f *fakeInventory) ReserveSeats(context.Context, *inventoryv1.ReserveSeatsRequest, ...grpc.CallOption) (*inventoryv1.ReserveSeatsResponse, error) {
	f.log.add("reserve")
	return &inventoryv1.ReserveSeatsResponse{}, f.reserveErr
}

func (f *fakeInventory) ConfirmSeats(context.Context, *inventoryv1.ConfirmSeatsRequest, ...grpc.CallOption) (*inventoryv1.ConfirmSeatsResponse, error) {
	f.log.add("confirm")
	return &inventoryv1.ConfirmSeatsResponse{}, f.confirmErr
}

func (f *fakeInventory) ReleaseSeats(context.Context, *inventoryv1.ReleaseSeatsRequest, ...grpc.CallOption) (*inventoryv1.ReleaseSeatsResponse, error) {
	f.log.add("release")
	return &inventoryv1.ReleaseSeatsResponse{}, f.releaseErr
}

type fakePayment struct {
	paymentv1.PaymentServiceClient
	log       *callLog
	chargeErr error
	delay     time.Duration // how long a charge takes, to keep concurrent requests overlapping
}

func (f *fakePayment) Charge(context.Context, *paymentv1.ChargeRequest, ...grpc.CallOption) (*paymentv1.ChargeResponse, error) {
	f.log.add("charge")
	time.Sleep(f.delay)
	return &paymentv1.ChargeResponse{PaymentId: uuid.NewString()}, f.chargeErr
}

// Run with ORDER_DATABASE_URL pointing at the orders database.
func newSaga(t *testing.T) (*Server, *fakeInventory, *fakePayment, *pgxpool.Pool, *callLog) {
	t.Helper()
	pool := pgtest.NewPool(t, "ORDER_DATABASE_URL", "migrations/order")
	log := &callLog{}
	inv := &fakeInventory{log: log}
	pay := &fakePayment{log: log}
	return New(pool, inv, pay), inv, pay, pool, log
}

func refuse(msg string) error { return status.Error(codes.FailedPrecondition, msg) }
func breakDown() error        { return status.Error(codes.Unavailable, "connection refused") }

func onlyOrderStatus(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var st string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM orders`).Scan(&st); err != nil {
		t.Fatalf("expected exactly one order: %v", err)
	}
	return st
}

func outboxTypes(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT event_type FROM outbox ORDER BY created_at, id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var types []string
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatal(err)
		}
		types = append(types, typ)
	}
	return types
}

func failureReasonInOutbox(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var reason string
	err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(payload->>'failureReason', '') FROM outbox WHERE event_type = 'order.failed'`).Scan(&reason)
	if err != nil {
		t.Fatalf("no order.failed event in the outbox: %v", err)
	}
	return reason
}

func TestSaga_HappyPathConfirmsAndAnnouncesTheOrder(t *testing.T) {
	s, _, _, pool, calls := newSaga(t)

	resp, err := s.CreateOrder(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	if resp.Status != orderv1.OrderStatus_ORDER_STATUS_CONFIRMED {
		t.Errorf("status = %v, want CONFIRMED", resp.Status)
	}
	if got := calls.all(); !slices.Equal(got, []string{"reserve", "charge", "confirm"}) {
		t.Errorf("downstream calls = %v, want reserve, charge, confirm in that order", got)
	}
	if got := onlyOrderStatus(t, pool); got != "CONFIRMED" {
		t.Errorf("stored status = %s, want CONFIRMED", got)
	}
	if got := outboxTypes(t, pool); !slices.Equal(got, []string{"order.created", "order.confirmed"}) {
		t.Errorf("outbox = %v, want created then confirmed", got)
	}
}

func TestSaga_SeatsRefusedFailsBeforeAnyMoneyMoves(t *testing.T) {
	s, inv, _, pool, calls := newSaga(t)
	inv.reserveErr = refuse("one or more seats are not available")

	_, err := s.CreateOrder(context.Background(), validRequest())

	if status.Code(err) != codes.FailedPrecondition || !apierr.Is(err, apierr.ReasonSeatsUnavailable) {
		t.Errorf("got %v (reason %q), want FailedPrecondition with SEATS_UNAVAILABLE", err, apierr.Reason(err))
	}
	if got := calls.all(); !slices.Equal(got, []string{"reserve"}) {
		t.Errorf("downstream calls = %v, want only reserve: nobody may be charged for seats they did not get", got)
	}
	if got := onlyOrderStatus(t, pool); got != "FAILED" {
		t.Errorf("stored status = %s, want FAILED", got)
	}
	if got := failureReasonInOutbox(t, pool); got != "ORDER_FAILURE_REASON_SEATS_UNAVAILABLE" {
		t.Errorf("announced failure reason = %s, want SEATS_UNAVAILABLE", got)
	}
}

func TestSaga_DeclinedPaymentReleasesTheSeats(t *testing.T) {
	s, _, pay, pool, calls := newSaga(t)
	pay.chargeErr = refuse("payment declined")

	_, err := s.CreateOrder(context.Background(), validRequest())

	if status.Code(err) != codes.FailedPrecondition || !apierr.Is(err, apierr.ReasonPaymentDeclined) {
		t.Errorf("got %v (reason %q), want FailedPrecondition with PAYMENT_DECLINED", err, apierr.Reason(err))
	}
	if got := calls.all(); !slices.Equal(got, []string{"reserve", "charge", "release"}) {
		t.Errorf("downstream calls = %v, want the compensation (release) after the declined charge", got)
	}
	if got := onlyOrderStatus(t, pool); got != "FAILED" {
		t.Errorf("stored status = %s, want FAILED", got)
	}
	if got := failureReasonInOutbox(t, pool); got != "ORDER_FAILURE_REASON_PAYMENT_DECLINED" {
		t.Errorf("announced failure reason = %s, want PAYMENT_DECLINED", got)
	}
}

// A service that is down has not declined anything. The customer must be told to
// retry, not that their card was refused.
func TestSaga_InventoryBreakdownIsNotReportedAsUnavailableSeats(t *testing.T) {
	s, inv, _, pool, calls := newSaga(t)
	inv.reserveErr = breakDown()

	_, err := s.CreateOrder(context.Background(), validRequest())

	if status.Code(err) != codes.Unavailable || !apierr.Is(err, apierr.ReasonServiceUnavailable) {
		t.Errorf("got %v (reason %q), want Unavailable with SERVICE_UNAVAILABLE", err, apierr.Reason(err))
	}
	// The reservation may have succeeded with only the reply lost, so the saga
	// releases defensively and never charges.
	if got := calls.all(); !slices.Equal(got, []string{"reserve", "release"}) {
		t.Errorf("downstream calls = %v, want reserve then a defensive release, and no charge", got)
	}
	if got := onlyOrderStatus(t, pool); got != "FAILED" {
		t.Errorf("stored status = %s, want FAILED", got)
	}
	if got := failureReasonInOutbox(t, pool); got != "ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE" {
		t.Errorf("announced failure reason = %s, want SERVICE_UNAVAILABLE", got)
	}
}

func TestSaga_PaymentBreakdownIsNotReportedAsADeclinedCard(t *testing.T) {
	s, _, pay, pool, calls := newSaga(t)
	pay.chargeErr = status.Error(codes.DeadlineExceeded, "timeout")

	_, err := s.CreateOrder(context.Background(), validRequest())

	if status.Code(err) != codes.Unavailable || !apierr.Is(err, apierr.ReasonServiceUnavailable) {
		t.Errorf("got %v (reason %q), want Unavailable with SERVICE_UNAVAILABLE, not PAYMENT_DECLINED", err, apierr.Reason(err))
	}
	if got := calls.all(); !slices.Equal(got, []string{"reserve", "charge", "release"}) {
		t.Errorf("downstream calls = %v, want the seats released after the failed charge", got)
	}
	if got := failureReasonInOutbox(t, pool); got != "ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE" {
		t.Errorf("announced failure reason = %s, want SERVICE_UNAVAILABLE", got)
	}
}

// The worst case: money was taken but the seats could not be confirmed. Releasing
// the seats now would resell seats that were paid for, so the saga must not, and
// must leave the order unresolved for a human or a reconciliation job.
func TestSaga_ConfirmFailingAfterAChargeLeavesTheOrderPendingAndDoesNotRelease(t *testing.T) {
	s, inv, _, pool, calls := newSaga(t)
	inv.confirmErr = breakDown()

	_, err := s.CreateOrder(context.Background(), validRequest())

	if status.Code(err) != codes.Internal {
		t.Errorf("got %v, want Internal", err)
	}
	if got := calls.all(); !slices.Equal(got, []string{"reserve", "charge", "confirm"}) {
		t.Errorf("downstream calls = %v; there must be no release after a successful charge", got)
	}
	if got := onlyOrderStatus(t, pool); got != "PENDING" {
		t.Errorf("stored status = %s, want PENDING: there is no honest final state to record", got)
	}
	if got := outboxTypes(t, pool); !slices.Equal(got, []string{"order.created"}) {
		t.Errorf("outbox = %v, want only order.created: announcing a confirmation or failure would be a lie", got)
	}
}

func TestSaga_FailedCompensationDoesNotChangeWhatTheCustomerIsTold(t *testing.T) {
	s, inv, pay, pool, _ := newSaga(t)
	pay.chargeErr = refuse("payment declined")
	inv.releaseErr = breakDown()

	_, err := s.CreateOrder(context.Background(), validRequest())

	if !apierr.Is(err, apierr.ReasonPaymentDeclined) {
		t.Errorf("got %v, want the original PAYMENT_DECLINED: a failed release must not mask why the order failed", err)
	}
	if got := onlyOrderStatus(t, pool); got != "FAILED" {
		t.Errorf("stored status = %s, want FAILED", got)
	}
}

func TestSaga_InvalidOrdersTouchNothing(t *testing.T) {
	s, _, _, pool, calls := newSaga(t)
	bad := validRequest()
	bad.SeatIds = []string{"nope"}

	_, err := s.CreateOrder(context.Background(), bad)

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got %v, want InvalidArgument", err)
	}
	if got := calls.all(); len(got) != 0 {
		t.Errorf("downstream calls = %v, want none for an invalid order", got)
	}
	var orders, outbox int
	if err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM orders), (SELECT count(*) FROM outbox)`).Scan(&orders, &outbox); err != nil {
		t.Fatal(err)
	}
	if orders != 0 || outbox != 0 {
		t.Errorf("%d order(s) and %d outbox row(s) were written for an invalid request, want none", orders, outbox)
	}
}

func TestGetOrder_ReturnsTheStoredOrderAndDistinguishesErrors(t *testing.T) {
	s, _, _, pool, _ := newSaga(t)
	req := validRequest()
	created, err := s.CreateOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	got, err := s.GetOrder(context.Background(), &orderv1.GetOrderRequest{OrderId: created.OrderId})
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if got.UserId != req.UserId || got.EventId != req.EventId || got.AmountCents != req.AmountCents ||
		len(got.SeatIds) != len(req.SeatIds) || got.Status != orderv1.OrderStatus_ORDER_STATUS_CONFIRMED {
		t.Errorf("GetOrder = %v, want the order that was placed", got)
	}

	if _, err := s.GetOrder(context.Background(), &orderv1.GetOrderRequest{OrderId: uuid.NewString()}); status.Code(err) != codes.NotFound {
		t.Errorf("unknown order got %v, want NotFound", err)
	}
	if _, err := s.GetOrder(context.Background(), &orderv1.GetOrderRequest{OrderId: "nope"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("malformed id got %v, want InvalidArgument", err)
	}

	// A database failure must not masquerade as "not found".
	pool.Close()
	_, err = s.GetOrder(context.Background(), &orderv1.GetOrderRequest{OrderId: created.OrderId})
	if err == nil || status.Code(err) == codes.NotFound {
		t.Errorf("with the database closed GetOrder returned %v; it must be an internal error, never NotFound", err)
	}
}
