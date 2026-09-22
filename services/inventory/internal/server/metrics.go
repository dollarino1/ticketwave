package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// reservations counts reservation attempts by outcome. "conflict" is the interesting
// one: someone lost a race for a seat. A high ratio during an on-sale means demand is
// concentrated on a few seats, which is normal, and a rising ratio on a quiet event
// means stale seat maps are being shown to buyers.
var reservations = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ticketwave_inventory_reservations_total",
	Help: "Seat reservation attempts, by outcome.",
}, []string{"result"})

const (
	reserved = "reserved"
	conflict = "conflict"
)

// expiredHolds counts seats the sweeper freed because their hold ran out: buyers who
// picked seats and never finished paying.
var expiredHolds = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ticketwave_inventory_expired_holds_total",
	Help: "Seats released because their 5-minute hold expired.",
})

// Create the series at zero: see initMetrics in pkg/kafka for why.
func init() {
	reservations.WithLabelValues(reserved)
	reservations.WithLabelValues(conflict)
}
