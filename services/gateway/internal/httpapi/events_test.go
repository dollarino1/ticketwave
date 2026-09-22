package httpapi

import (
	"net/http"
	"testing"
	"time"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListEvents_IsPublicAndReturnsPlainJSON(t *testing.T) {
	h := newHarness(t)
	created := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		return &inventoryv1.ListEventsResponse{Events: []*inventoryv1.Event{
			{Id: eventID, Name: "Jazz", TotalSeats: 100, AvailableSeats: 40, CreatedAt: timestamppb.New(created)},
		}}, nil
	}

	rec := h.do("GET", "/api/events", "") // no token

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decode[struct{ Events []eventView }](t, rec)
	if len(body.Events) != 1 || body.Events[0].Name != "Jazz" || body.Events[0].AvailableSeats != 40 || !body.Events[0].CreatedAt.Equal(created) {
		t.Errorf("events = %+v", body.Events)
	}
	if got := body.Events[0].SeatPriceCents; got != 5000 {
		t.Errorf("seat_price_cents = %d, want the configured 5000 so the UI never hard-codes it", got)
	}
}

func TestListEvents_EmptyIsAnEmptyArrayNotNull(t *testing.T) {
	h := newHarness(t)
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		return &inventoryv1.ListEventsResponse{}, nil
	}

	rec := h.do("GET", "/api/events", "")

	if got := rec.Body.String(); got != "{\"events\":[]}\n" {
		t.Errorf("body = %q; a null would crash `events.map` in the browser", got)
	}
}

func TestListEvents_LimitParameter(t *testing.T) {
	cases := map[string]struct {
		query string
		want  int
		limit int32
	}{
		"none":         {"", 200, 0},
		"valid":        {"?limit=25", 200, 25},
		"zero":         {"?limit=0", 400, 0},
		"negative":     {"?limit=-3", 400, 0},
		"not a number": {"?limit=abc", 400, 0},
		"overflow":     {"?limit=99999999999", 400, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			var sent int32 = -1
			if tc.want == 200 {
				h.inventory.listEvents = func(in *inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
					sent = in.Limit
					return &inventoryv1.ListEventsResponse{}, nil
				}
			}

			rec := h.do("GET", "/api/events"+tc.query, "")

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want == 200 && sent != tc.limit {
				t.Errorf("inventory-svc got limit %d, want %d", sent, tc.limit)
			}
		})
	}
}

func TestGetEvent_NotFoundIsA404(t *testing.T) {
	h := newHarness(t)
	h.inventory.getEvent = func(*inventoryv1.GetEventRequest) (*inventoryv1.GetEventResponse, error) {
		return nil, status.Error(codes.NotFound, "event not found")
	}

	rec := h.do("GET", "/api/events/"+eventID, "")

	if rec.Code != http.StatusNotFound || errCode(t, rec) != "not_found" {
		t.Errorf("got %d %q, want 404 not_found", rec.Code, errCode(t, rec))
	}
}

func TestListSeats_UsesTheIDFromThePathAndNamesStatuses(t *testing.T) {
	h := newHarness(t)
	var asked string
	h.inventory.listSeats = func(in *inventoryv1.ListSeatsRequest) (*inventoryv1.ListSeatsResponse, error) {
		asked = in.EventId
		return &inventoryv1.ListSeatsResponse{Seats: []*inventoryv1.Seat{
			{Id: seatA, Label: "A1", Status: inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE},
			{Id: seatB, Label: "A2", Status: inventoryv1.SeatStatus_SEAT_STATUS_HELD},
			{Id: seatC, Label: "A3", Status: inventoryv1.SeatStatus_SEAT_STATUS_SOLD},
		}}, nil
	}

	rec := h.do("GET", "/api/events/"+eventID+"/seats", "")

	if rec.Code != http.StatusOK || asked != eventID {
		t.Fatalf("status %d, asked for %q", rec.Code, asked)
	}
	seats := decode[struct{ Seats []seatView }](t, rec).Seats
	want := []string{"available", "held", "sold"}
	for i, s := range seats {
		if s.Status != want[i] {
			t.Errorf("seat %s status = %q, want %q", s.Label, s.Status, want[i])
		}
	}
}

func TestCreateEvent_PassesTheOrganizersInputThrough(t *testing.T) {
	h := newHarness(t)
	var got *inventoryv1.CreateEventRequest
	h.inventory.createEvent = func(in *inventoryv1.CreateEventRequest) (*inventoryv1.CreateEventResponse, error) {
		got = in
		return &inventoryv1.CreateEventResponse{EventId: eventID}, nil
	}

	rec := h.do("POST", "/api/events", `{"name":"Jazz","seat_count":50}`, bearer("organizer-token")...)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got.Name != "Jazz" || got.SeatCount != 50 {
		t.Errorf("inventory-svc received %v", got)
	}
}

func TestCreateEvent_InventoryValidationReachesTheClient(t *testing.T) {
	h := newHarness(t)
	h.inventory.createEvent = func(*inventoryv1.CreateEventRequest) (*inventoryv1.CreateEventResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "seat_count must be between 1 and 10000")
	}

	rec := h.do("POST", "/api/events", `{"name":"x","seat_count":0}`, bearer("organizer-token")...)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestAnalytics_MergesEventNames(t *testing.T) {
	h := newHarness(t)
	h.analytics.listEventStats = func(*analyticsv1.ListEventStatsRequest) (*analyticsv1.ListEventStatsResponse, error) {
		return &analyticsv1.ListEventStatsResponse{Stats: []*analyticsv1.EventStats{
			{EventId: eventID, TicketsSold: 7, RevenueCents: 35000, OrdersConfirmed: 3, OrdersFailed: 1, UpdatedAt: timestamppb.Now()},
			{EventId: otherID, TicketsSold: 1},
		}}, nil
	}
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		return &inventoryv1.ListEventsResponse{Events: []*inventoryv1.Event{{Id: eventID, Name: "Jazz"}}}, nil
	}

	rec := h.do("GET", "/api/analytics/events", "", bearer("organizer-token")...)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	got := decode[struct{ Events []statsView }](t, rec).Events
	if len(got) != 2 || got[0].EventName != "Jazz" || got[0].TicketsSold != 7 || got[0].RevenueCents != 35000 {
		t.Errorf("stats = %+v", got)
	}
	if got[1].EventName != "" {
		t.Errorf("an event unknown to inventory got the name %q", got[1].EventName)
	}
}

func TestAnalytics_StillAnswersWhenInventoryIsDown(t *testing.T) {
	h := newHarness(t)
	h.analytics.listEventStats = func(*analyticsv1.ListEventStatsRequest) (*analyticsv1.ListEventStatsResponse, error) {
		return &analyticsv1.ListEventStatsResponse{Stats: []*analyticsv1.EventStats{{EventId: eventID, TicketsSold: 2}}}, nil
	}
	h.inventory.listEvents = func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}

	rec := h.do("GET", "/api/analytics/events", "", bearer("organizer-token")...)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: names are optional, the numbers are the point", rec.Code)
	}
	if got := decode[struct{ Events []statsView }](t, rec).Events; len(got) != 1 || got[0].TicketsSold != 2 {
		t.Errorf("stats = %+v", got)
	}
}

func TestAnalytics_FailsWhenAnalyticsIsDown(t *testing.T) {
	h := newHarness(t)
	h.analytics.listEventStats = func(*analyticsv1.ListEventStatsRequest) (*analyticsv1.ListEventStatsResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}

	rec := h.do("GET", "/api/analytics/events", "", bearer("organizer-token")...)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}
