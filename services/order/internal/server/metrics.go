package server

import (
	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ordersTotal is the business heartbeat: how many orders ended, and how. The gRPC
// metrics say whether calls succeeded; this says whether the SALES did. A payment
// provider outage shows up here as a wall of "service_unavailable" failures while
// every RPC still returns cleanly.
var ordersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ticketwave_orders_total",
	Help: "Orders that reached a final state, by result and failure reason.",
}, []string{"result", "reason"})

// recordOutcome counts an order once its final state is safely committed. Counting
// before the commit would report a sale that the database then rolled back.
func recordOutcome(typ eventsv1.OrderEventType, reason eventsv1.OrderFailureReason) {
	switch typ {
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED:
		ordersTotal.WithLabelValues("confirmed", "none").Inc()
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED:
		ordersTotal.WithLabelValues("failed", reasonLabel(reason)).Inc()
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_UNSPECIFIED, eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED:
		// Not final: the order is still in progress.
	}
}

// reasonLabel is a fixed, small set of words, so the label cannot grow unbounded.
func reasonLabel(r eventsv1.OrderFailureReason) string {
	switch r {
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE:
		return "seats_unavailable"
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED:
		return "payment_declined"
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE:
		return "service_unavailable"
	default:
		return "unspecified"
	}
}

// Create every series at zero so the first sale or failure registers as a rise from
// 0 rather than as a series that appeared already at 1, which rate() cannot see.
func init() {
	ordersTotal.WithLabelValues("confirmed", "none")
	for _, reason := range []string{"seats_unavailable", "payment_declined", "service_unavailable", "unspecified"} {
		ordersTotal.WithLabelValues("failed", reason)
	}
}
