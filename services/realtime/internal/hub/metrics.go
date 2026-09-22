package hub

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// subscribersGauge is the number of open seat-map streams, the service's load. It
	// approaches the configured limit before new viewers start being turned away.
	subscribersGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ticketwave_realtime_subscribers",
		Help: "Browsers currently watching a seat map.",
	})

	updatesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticketwave_realtime_updates_sent_total",
		Help: "Seat updates queued to browsers (one per browser per update).",
	})

	// droppedSlow counts viewers cut off for not reading fast enough. It should stay
	// near zero: a steady rate means events are arriving faster than clients can take
	// them, or that the buffer is too small for real network conditions.
	droppedSlow = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticketwave_realtime_dropped_slow_subscribers_total",
		Help: "Subscribers disconnected because their buffer filled.",
	})

	refusedFull = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticketwave_realtime_refused_subscribers_total",
		Help: "Connections refused because the hub was at its limit.",
	})
)
