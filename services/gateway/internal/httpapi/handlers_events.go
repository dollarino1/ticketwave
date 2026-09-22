package httpapi

import (
	"net/http"
	"strconv"
	"time"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
)

type eventView struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	TotalSeats     int32  `json:"total_seats"`
	AvailableSeats int32  `json:"available_seats"`
	// The price is the gateway's to decide (see createOrder); it is shown so the
	// browser never has to hard-code a number that could drift from the real one.
	SeatPriceCents int64     `json:"seat_price_cents"`
	CreatedAt      time.Time `json:"created_at"`
}

type seatView struct {
	ID     string `json:"id"`
	Label  string `json:"label"`
	Status string `json:"status"`
}

func (a *API) eventFrom(e *inventoryv1.Event) eventView {
	return eventView{
		ID:             e.GetId(),
		Name:           e.GetName(),
		TotalSeats:     e.GetTotalSeats(),
		AvailableSeats: e.GetAvailableSeats(),
		SeatPriceCents: a.seatPriceCents,
		CreatedAt:      e.GetCreatedAt().AsTime(),
	}
}

// seatStatusName gives the browser plain words instead of proto enum names.
func seatStatusName(s inventoryv1.SeatStatus) string {
	switch s {
	case inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE:
		return "available"
	case inventoryv1.SeatStatus_SEAT_STATUS_HELD:
		return "held"
	case inventoryv1.SeatStatus_SEAT_STATUS_SOLD:
		return "sold"
	default:
		return "unknown"
	}
}

// queryLimit reads the optional ?limit= parameter. Zero means "not given", and
// the service applies its own default and ceiling.
func queryLimit(w http.ResponseWriter, r *http.Request) (int32, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, true
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n < 1 {
		writeError(w, http.StatusBadRequest, "invalid_argument", "limit must be a positive whole number")
		return 0, false
	}
	return int32(n), true
}

func (a *API) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryLimit(w, r)
	if !ok {
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.inventory.ListEvents(ctx, &inventoryv1.ListEventsRequest{Limit: limit})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	events := make([]eventView, 0, len(resp.GetEvents()))
	for _, e := range resp.GetEvents() {
		events = append(events, a.eventFrom(e))
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (a *API) getEvent(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.inventory.GetEvent(ctx, &inventoryv1.GetEventRequest{EventId: r.PathValue("id")})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"event": a.eventFrom(resp.GetEvent())})
}

func (a *API) listSeats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.inventory.ListSeats(ctx, &inventoryv1.ListSeatsRequest{EventId: r.PathValue("id")})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	seats := make([]seatView, 0, len(resp.GetSeats()))
	for _, s := range resp.GetSeats() {
		seats = append(seats, seatView{ID: s.GetId(), Label: s.GetLabel(), Status: seatStatusName(s.GetStatus())})
	}
	writeJSON(w, http.StatusOK, map[string]any{"seats": seats})
}

type createEventBody struct {
	Name      string `json:"name"`
	SeatCount int32  `json:"seat_count"`
}

func (a *API) createEvent(w http.ResponseWriter, r *http.Request) {
	var body createEventBody
	if !decodeJSON(w, r, &body) {
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.inventory.CreateEvent(ctx, &inventoryv1.CreateEventRequest{Name: body.Name, SeatCount: body.SeatCount})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"event_id": resp.GetEventId()})
}
