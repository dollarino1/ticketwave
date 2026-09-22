//go:build integration

package server

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/dollarino1/ticketwave/pkg/apierr"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func keyed(key string) *orderv1.CreateOrderRequest {
	r := validRequest()
	r.IdempotencyKey = key
	return r
}

func countOrders(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM orders`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func count(calls []string, name string) int {
	n := 0
	for _, c := range calls {
		if c == name {
			n++
		}
	}
	return n
}

func TestIdempotency_ARetryOfAConfirmedOrderReturnsTheSameOrderAndChargesOnce(t *testing.T) {
	s, _, _, pool, calls := newSaga(t)
	req := keyed("retry-after-timeout")

	first, err := s.CreateOrder(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("the retry failed: %v", err)
	}

	if second.OrderId != first.OrderId || second.Status != orderv1.OrderStatus_ORDER_STATUS_CONFIRMED {
		t.Errorf("retry = %v, want the first order %s CONFIRMED", second, first.OrderId)
	}
	if got := calls.all(); !slices.Equal(got, []string{"reserve", "charge", "confirm"}) {
		t.Errorf("downstream calls = %v, want the saga to have run exactly once", got)
	}
	if n := countOrders(t, pool); n != 1 {
		t.Errorf("%d orders exist, want 1", n)
	}
	if got := outboxTypes(t, pool); !slices.Equal(got, []string{"order.created", "order.confirmed"}) {
		t.Errorf("outbox = %v: a retry must not announce anything twice", got)
	}
}

// The customer who was told "declined" and retries must be told "declined" again,
// not charged a second time, and not given a vaguer answer.
func TestIdempotency_ARetryOfAFailedOrderReplaysTheSameFailure(t *testing.T) {
	cases := []struct {
		name       string
		arrange    func(*fakeInventory, *fakePayment)
		wantCode   codes.Code
		wantReason string
		wantCalls  []string
	}{
		{"card declined", func(_ *fakeInventory, p *fakePayment) { p.chargeErr = refuse("declined") },
			codes.FailedPrecondition, apierr.ReasonPaymentDeclined, []string{"reserve", "charge", "release"}},
		{"seats taken", func(i *fakeInventory, _ *fakePayment) { i.reserveErr = refuse("taken") },
			codes.FailedPrecondition, apierr.ReasonSeatsUnavailable, []string{"reserve"}},
		{"a service was down", func(i *fakeInventory, _ *fakePayment) { i.reserveErr = breakDown() },
			codes.Unavailable, apierr.ReasonServiceUnavailable, []string{"reserve", "release"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, inv, pay, pool, calls := newSaga(t)
			tc.arrange(inv, pay)
			req := keyed("fails-" + strings.ReplaceAll(tc.name, " ", "-"))

			_, firstErr := s.CreateOrder(context.Background(), req)
			_, retryErr := s.CreateOrder(context.Background(), req)

			for label, err := range map[string]error{"first": firstErr, "retry": retryErr} {
				if status.Code(err) != tc.wantCode || apierr.Reason(err) != tc.wantReason {
					t.Errorf("%s attempt = %v (%s), want %v %s", label, err, apierr.Reason(err), tc.wantCode, tc.wantReason)
				}
			}
			if firstErr.Error() != retryErr.Error() {
				t.Errorf("the retry's message %q differs from the first's %q", retryErr, firstErr)
			}
			if got := calls.all(); !slices.Equal(got, tc.wantCalls) {
				t.Errorf("downstream calls = %v, want %v: the retry must not touch any service", got, tc.wantCalls)
			}
			if n := countOrders(t, pool); n != 1 {
				t.Errorf("%d orders exist, want 1", n)
			}
		})
	}
}

func TestIdempotency_TheSameKeyForADifferentRequestIsRefused(t *testing.T) {
	s, _, _, pool, calls := newSaga(t)
	first := keyed("one-key")
	if _, err := s.CreateOrder(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	callsAfterFirst := len(calls.all())

	other := validRequest() // different seats and event
	other.UserId = first.UserId
	other.IdempotencyKey = first.IdempotencyKey
	_, err := s.CreateOrder(context.Background(), other)

	if status.Code(err) != codes.InvalidArgument || apierr.Reason(err) != apierr.ReasonIdempotencyKeyReused {
		t.Errorf("got %v (%s), want InvalidArgument IDEMPOTENCY_KEY_REUSED: returning the first result would hide a client bug", err, apierr.Reason(err))
	}
	if len(calls.all()) != callsAfterFirst {
		t.Error("a refused request still called a downstream service")
	}
	if n := countOrders(t, pool); n != 1 {
		t.Errorf("%d orders exist, want 1", n)
	}
}

func TestIdempotency_AnotherPriceForTheSameSeatsIsAlsoADifferentRequest(t *testing.T) {
	s, _, _, _, _ := newSaga(t)
	first := keyed("price-check")
	if _, err := s.CreateOrder(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	again := &orderv1.CreateOrderRequest{
		UserId: first.UserId, EventId: first.EventId, SeatIds: first.SeatIds,
		AmountCents: first.AmountCents + 1, IdempotencyKey: first.IdempotencyKey,
	}

	_, err := s.CreateOrder(context.Background(), again)

	if apierr.Reason(err) != apierr.ReasonIdempotencyKeyReused {
		t.Errorf("got %v, want IDEMPOTENCY_KEY_REUSED", err)
	}
}

func TestIdempotency_SeatOrderDoesNotMakeItADifferentRequest(t *testing.T) {
	s, _, _, pool, _ := newSaga(t)
	first := keyed("seat-order")
	firstResp, err := s.CreateOrder(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	reversed := &orderv1.CreateOrderRequest{
		UserId: first.UserId, EventId: first.EventId, AmountCents: first.AmountCents,
		SeatIds: []string{first.SeatIds[1], first.SeatIds[0]}, IdempotencyKey: first.IdempotencyKey,
	}

	resp, err := s.CreateOrder(context.Background(), reversed)

	if err != nil || resp.OrderId != firstResp.OrderId {
		t.Errorf("got %v, %v, want the first order back", resp, err)
	}
	if n := countOrders(t, pool); n != 1 {
		t.Errorf("%d orders exist, want 1", n)
	}
}

// A key belongs to one user. Sharing (or probing) another person's key must be impossible.
func TestIdempotency_KeysAreScopedToTheUser(t *testing.T) {
	s, _, _, pool, calls := newSaga(t)
	a, b := keyed("shared-word"), keyed("shared-word") // different users, same key text

	if _, err := s.CreateOrder(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateOrder(context.Background(), b); err != nil {
		t.Fatalf("a second user's order was blocked by the first user's key: %v", err)
	}

	if n := countOrders(t, pool); n != 2 {
		t.Errorf("%d orders exist, want 2", n)
	}
	if got := count(calls.all(), "charge"); got != 2 {
		t.Errorf("%d charges, want 2: two different users must both be charged", got)
	}
}

func TestIdempotency_WithoutAKeyEveryRequestIsANewOrder(t *testing.T) {
	s, _, _, pool, _ := newSaga(t)
	req := validRequest() // no key

	if _, err := s.CreateOrder(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateOrder(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	if n := countOrders(t, pool); n != 2 {
		t.Errorf("%d orders exist, want 2: no key means no deduplication", n)
	}
}

// An order that exists but has not finished must not be started a second time.
func TestIdempotency_ARequestStillInProgressIsReportedNotRestarted(t *testing.T) {
	s, _, _, pool, calls := newSaga(t)
	req := keyed("slow-one")
	_, err := pool.Exec(context.Background(),
		`INSERT INTO orders (id, user_id, event_id, seat_ids, amount_cents, status, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)`,
		uuid.New(), req.UserId, req.EventId, req.SeatIds, req.AmountCents, req.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.CreateOrder(context.Background(), req)

	if status.Code(err) != codes.Aborted || apierr.Reason(err) != apierr.ReasonRequestInProgress {
		t.Errorf("got %v (%s), want Aborted REQUEST_IN_PROGRESS", err, apierr.Reason(err))
	}
	if len(calls.all()) != 0 {
		t.Errorf("downstream calls = %v, want none", calls.all())
	}
}

// The property the whole feature exists for: a burst of identical requests, all in
// flight at once, buys the seats and charges the card ONCE.
func TestIdempotency_ConcurrentIdenticalRequestsChargeExactlyOnce(t *testing.T) {
	s, _, pay, pool, calls := newSaga(t)
	pay.delay = 150 * time.Millisecond // long enough that every request overlaps the first
	req := keyed("double-click")

	const clients = 12
	var (
		start     = make(chan struct{})
		wg        sync.WaitGroup
		mu        sync.Mutex
		confirmed int
		orderIDs  = map[string]bool{}
		other     []error
	)
	for i := 0; i < clients; i++ {
		wg.Go(func() {
			<-start // release everyone at the same instant
			resp, err := s.CreateOrder(context.Background(), req)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				confirmed++
				orderIDs[resp.OrderId] = true
			case apierr.Is(err, apierr.ReasonRequestInProgress):
				// the correct answer to a duplicate that arrived while the first was running
			default:
				other = append(other, err)
			}
		})
	}
	close(start)
	wg.Wait()

	if len(other) != 0 {
		t.Errorf("unexpected errors: %v", other)
	}
	if confirmed < 1 {
		t.Error("nobody's order was confirmed")
	}
	if len(orderIDs) != 1 {
		t.Errorf("%d different orders were confirmed, want 1", len(orderIDs))
	}
	if got := count(calls.all(), "charge"); got != 1 {
		t.Errorf("the card was charged %d times, want exactly 1", got)
	}
	if got := count(calls.all(), "reserve"); got != 1 {
		t.Errorf("seats were reserved %d times, want exactly 1", got)
	}
	if n := countOrders(t, pool); n != 1 {
		t.Errorf("%d orders exist, want 1", n)
	}
}

func TestIdempotency_TheFailureReasonIsStoredWithTheOrder(t *testing.T) {
	s, _, pay, pool, _ := newSaga(t)
	pay.chargeErr = refuse("declined")

	_, _ = s.CreateOrder(context.Background(), keyed("store-reason"))

	var reason *string
	if err := pool.QueryRow(context.Background(), `SELECT failure_reason FROM orders`).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == nil || *reason != "payment_declined" {
		t.Errorf("failure_reason = %v, want payment_declined", reason)
	}
}

func TestIdempotency_AConfirmedOrderStoresNoFailureReason(t *testing.T) {
	s, _, _, pool, _ := newSaga(t)

	if _, err := s.CreateOrder(context.Background(), keyed("confirmed")); err != nil {
		t.Fatal(err)
	}

	var reason *string
	if err := pool.QueryRow(context.Background(), `SELECT failure_reason FROM orders`).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason != nil {
		t.Errorf("failure_reason = %q on a confirmed order, want NULL", *reason)
	}
}
