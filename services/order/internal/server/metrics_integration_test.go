//go:build integration

package server

import (
	"context"
	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_SagaOutcomesAreCountedByResultAndReason(t *testing.T) {
	confirmed := ordersTotal.WithLabelValues("confirmed", "none")
	declined := ordersTotal.WithLabelValues("failed", "payment_declined")
	seatsGone := ordersTotal.WithLabelValues("failed", "seats_unavailable")
	cBefore, dBefore, sBefore := testutil.ToFloat64(confirmed), testutil.ToFloat64(declined), testutil.ToFloat64(seatsGone)

	s, inv, pay, _, _ := newSaga(t)
	if _, err := s.CreateOrder(context.Background(), validRequest()); err != nil {
		t.Fatal(err)
	}
	pay.chargeErr = refuse("declined")
	_, _ = s.CreateOrder(context.Background(), validRequest())
	pay.chargeErr = nil
	inv.reserveErr = refuse("taken")
	_, _ = s.CreateOrder(context.Background(), validRequest())

	if got := testutil.ToFloat64(confirmed) - cBefore; got != 1 {
		t.Errorf("confirmed rose by %v, want 1", got)
	}
	if got := testutil.ToFloat64(declined) - dBefore; got != 1 {
		t.Errorf("payment_declined rose by %v, want 1", got)
	}
	if got := testutil.ToFloat64(seatsGone) - sBefore; got != 1 {
		t.Errorf("seats_unavailable rose by %v, want 1", got)
	}
}

// A sale the database refused to record must not be reported as a sale.
func TestMetrics_ATransitionThatFailedToCommitIsNotCounted(t *testing.T) {
	s, _, _, _, _ := newSaga(t)
	before := testutil.ToFloat64(ordersTotal.WithLabelValues("confirmed", "none"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // every database call now fails

	err := s.transition(ctx, orderSnapshot{}, "CONFIRMED",
		eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED)

	if err == nil {
		t.Fatal("transition succeeded on a cancelled context")
	}
	if got := testutil.ToFloat64(ordersTotal.WithLabelValues("confirmed", "none")) - before; got != 0 {
		t.Errorf("confirmed counter rose by %v for a transition that failed", got)
	}
}
