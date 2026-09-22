package server

import (
	"testing"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordOutcome(t *testing.T) {
	cases := []struct {
		name   string
		typ    eventsv1.OrderEventType
		reason eventsv1.OrderFailureReason
		result string
		label  string
		counts bool
	}{
		{"confirmed", eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED, "confirmed", "none", true},
		{"seats gone", eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE, "failed", "seats_unavailable", true},
		{"declined", eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED, "failed", "payment_declined", true},
		{"outage", eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE, "failed", "service_unavailable", true},
		{"unspecified reason", eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED, "failed", "unspecified", true},
		// A brand-new order is not an outcome yet.
		{"created", eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED, eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var before float64
			if tc.counts {
				before = testutil.ToFloat64(ordersTotal.WithLabelValues(tc.result, tc.label))
			}
			total := testutil.CollectAndCount(ordersTotal)

			recordOutcome(tc.typ, tc.reason)

			if tc.counts {
				if got := testutil.ToFloat64(ordersTotal.WithLabelValues(tc.result, tc.label)) - before; got != 1 {
					t.Errorf("%s/%s rose by %v, want 1", tc.result, tc.label, got)
				}
			} else if got := testutil.CollectAndCount(ordersTotal); got != total {
				t.Errorf("a non-final event created %d new series", got-total)
			}
		})
	}
}

func TestMetrics_AllOutcomesExistAtZeroFromStartup(t *testing.T) {
	// 1 confirmed + 4 failure reasons. init() creates them; recordOutcome adds none.
	if got := testutil.CollectAndCount(ordersTotal); got < 5 {
		t.Errorf("only %d series exist at startup, want at least 5: a first failure would be invisible to rate()", got)
	}
}
