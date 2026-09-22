package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// These live on the default registry, which the /metrics endpoint serves. They are
// package-level because a metric may be registered only once per process, and one
// gateway process serves every request.
var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ticketwave_http_requests_total",
		Help: "HTTP requests handled by the gateway, by route, method and status code.",
	}, []string{"route", "method", "code"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ticketwave_http_request_duration_seconds",
		Help:    "Time the gateway took to answer, by route.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"route", "method"})

	httpInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ticketwave_http_requests_in_flight",
		Help: "Requests the gateway is handling right now.",
	})

	rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ticketwave_ratelimit_rejections_total",
		Help: "Requests refused with 429, by which limit was hit. A spike in login is an attack.",
	}, []string{"limit"})
)

// unmatched labels requests no route claimed. It is a constant, never the URL: a
// label whose value comes from the client is a way to fill Prometheus's memory, since
// every distinct value becomes a new time series.
const unmatched = "unmatched"

// instrument records the route's request count, latency and in-flight gauge.
//
// The label is the route PATTERN ("GET /api/orders/{id}"), not the path, for the same
// cardinality reason: one series per route, not one per order ID. The pattern is known
// where the route is registered, so no lookup after the fact is needed.
func instrument(route string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			httpInFlight.Inc()
			defer httpInFlight.Dec()

			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			httpRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
			httpDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
		})
	}
}

// limitName turns a rate-limit key such as "login-email:ab12cd" or "orders:user:1234"
// into its bounded name ("login-email", "orders"): everything before the first colon.
func limitName(key string) string {
	name, _, _ := strings.Cut(key, ":")
	return name
}
