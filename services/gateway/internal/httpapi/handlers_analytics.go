package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
)

type statsView struct {
	EventID         string    `json:"event_id"`
	EventName       string    `json:"event_name"`
	TicketsSold     int64     `json:"tickets_sold"`
	RevenueCents    int64     `json:"revenue_cents"`
	OrdersConfirmed int64     `json:"orders_confirmed"`
	OrdersFailed    int64     `json:"orders_failed"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// listAnalytics joins two services: analytics-svc knows the numbers but only by
// event ID, inventory-svc knows the names. Composing them here, rather than
// making analytics-svc depend on inventory, keeps each service ignorant of the
// other. That is the gateway acting as a backend-for-frontend.
func (a *API) listAnalytics(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryLimit(w, r)
	if !ok {
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	stats, err := a.analytics.ListEventStats(ctx, &analyticsv1.ListEventStatsRequest{Limit: limit})
	if err != nil {
		a.fail(w, r, err)
		return
	}

	// Names are a nicety. If inventory is having a bad moment the numbers are
	// still worth showing, so this failure is logged rather than returned.
	names := map[string]string{}
	events, err := a.inventory.ListEvents(ctx, &inventoryv1.ListEventsRequest{Limit: 200})
	if err != nil {
		a.log.Warn("could not look up event names for the analytics view",
			slog.String("request_id", requestIDFrom(r.Context())), slog.Any("error", err))
	} else {
		for _, e := range events.GetEvents() {
			names[e.GetId()] = e.GetName()
		}
	}

	out := make([]statsView, 0, len(stats.GetStats()))
	for _, s := range stats.GetStats() {
		out = append(out, statsView{
			EventID:         s.GetEventId(),
			EventName:       names[s.GetEventId()],
			TicketsSold:     s.GetTicketsSold(),
			RevenueCents:    s.GetRevenueCents(),
			OrdersConfirmed: s.GetOrdersConfirmed(),
			OrdersFailed:    s.GetOrdersFailed(),
			UpdatedAt:       s.GetUpdatedAt().AsTime(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}
