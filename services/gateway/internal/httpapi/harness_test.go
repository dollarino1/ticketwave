package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/dollarino1/ticketwave/pkg/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	userID      = "11111111-1111-4111-8111-111111111111"
	otherID     = "22222222-2222-4222-8222-222222222222"
	organizerID = "33333333-3333-4333-8333-333333333333"
	adminID     = "44444444-4444-4444-8444-444444444444"
	eventID     = "55555555-5555-4555-8555-555555555555"
	seatA       = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	seatB       = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	seatC       = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

// Every fake below fails the test if the gateway calls a method the test did not
// prepare. That is how "the downstream service must NOT be called" is asserted.

func unexpected(t *testing.T, what string) error {
	t.Helper()
	t.Errorf("unexpected call to %s", what)
	return status.Error(codes.Internal, "unexpected call to "+what)
}

type fakeAuth struct {
	t        *testing.T
	register func(*authv1.RegisterRequest) (*authv1.RegisterResponse, error)
	login    func(*authv1.LoginRequest) (*authv1.LoginResponse, error)
	refresh  func(*authv1.RefreshRequest) (*authv1.RefreshResponse, error)
	logout   func(*authv1.LogoutRequest) (*authv1.LogoutResponse, error)
}

func (f *fakeAuth) Register(_ context.Context, in *authv1.RegisterRequest, _ ...grpc.CallOption) (*authv1.RegisterResponse, error) {
	if f.register == nil {
		return nil, unexpected(f.t, "Auth.Register")
	}
	return f.register(in)
}

func (f *fakeAuth) Login(_ context.Context, in *authv1.LoginRequest, _ ...grpc.CallOption) (*authv1.LoginResponse, error) {
	if f.login == nil {
		return nil, unexpected(f.t, "Auth.Login")
	}
	return f.login(in)
}

func (f *fakeAuth) Refresh(_ context.Context, in *authv1.RefreshRequest, _ ...grpc.CallOption) (*authv1.RefreshResponse, error) {
	if f.refresh == nil {
		return nil, unexpected(f.t, "Auth.Refresh")
	}
	return f.refresh(in)
}

func (f *fakeAuth) Logout(_ context.Context, in *authv1.LogoutRequest, _ ...grpc.CallOption) (*authv1.LogoutResponse, error) {
	if f.logout == nil {
		return nil, unexpected(f.t, "Auth.Logout")
	}
	return f.logout(in)
}

type fakeInventory struct {
	lastCtx     context.Context // the context of the most recent ListEvents call
	t           *testing.T
	createEvent func(*inventoryv1.CreateEventRequest) (*inventoryv1.CreateEventResponse, error)
	getEvent    func(*inventoryv1.GetEventRequest) (*inventoryv1.GetEventResponse, error)
	listEvents  func(*inventoryv1.ListEventsRequest) (*inventoryv1.ListEventsResponse, error)
	listSeats   func(*inventoryv1.ListSeatsRequest) (*inventoryv1.ListSeatsResponse, error)
}

func (f *fakeInventory) CreateEvent(_ context.Context, in *inventoryv1.CreateEventRequest, _ ...grpc.CallOption) (*inventoryv1.CreateEventResponse, error) {
	if f.createEvent == nil {
		return nil, unexpected(f.t, "Inventory.CreateEvent")
	}
	return f.createEvent(in)
}

func (f *fakeInventory) GetEvent(_ context.Context, in *inventoryv1.GetEventRequest, _ ...grpc.CallOption) (*inventoryv1.GetEventResponse, error) {
	if f.getEvent == nil {
		return nil, unexpected(f.t, "Inventory.GetEvent")
	}
	return f.getEvent(in)
}

func (f *fakeInventory) ListEvents(ctx context.Context, in *inventoryv1.ListEventsRequest, _ ...grpc.CallOption) (*inventoryv1.ListEventsResponse, error) {
	f.lastCtx = ctx
	if f.listEvents == nil {
		return nil, unexpected(f.t, "Inventory.ListEvents")
	}
	return f.listEvents(in)
}

func (f *fakeInventory) ListSeats(_ context.Context, in *inventoryv1.ListSeatsRequest, _ ...grpc.CallOption) (*inventoryv1.ListSeatsResponse, error) {
	if f.listSeats == nil {
		return nil, unexpected(f.t, "Inventory.ListSeats")
	}
	return f.listSeats(in)
}

type fakeOrders struct {
	t           *testing.T
	createOrder func(*orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error)
	getOrder    func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error)
}

func (f *fakeOrders) CreateOrder(_ context.Context, in *orderv1.CreateOrderRequest, _ ...grpc.CallOption) (*orderv1.CreateOrderResponse, error) {
	if f.createOrder == nil {
		return nil, unexpected(f.t, "Orders.CreateOrder")
	}
	return f.createOrder(in)
}

func (f *fakeOrders) GetOrder(_ context.Context, in *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	if f.getOrder == nil {
		return nil, unexpected(f.t, "Orders.GetOrder")
	}
	return f.getOrder(in)
}

type fakeAnalytics struct {
	t              *testing.T
	listEventStats func(*analyticsv1.ListEventStatsRequest) (*analyticsv1.ListEventStatsResponse, error)
}

func (f *fakeAnalytics) ListEventStats(_ context.Context, in *analyticsv1.ListEventStatsRequest, _ ...grpc.CallOption) (*analyticsv1.ListEventStatsResponse, error) {
	if f.listEventStats == nil {
		return nil, unexpected(f.t, "Analytics.ListEventStats")
	}
	return f.listEventStats(in)
}

// fakeLimiter records every key it is asked about and can be told to refuse some.
type fakeLimiter struct {
	mu   sync.Mutex
	keys []string
	deny func(key string) bool
	err  error
}

func (f *fakeLimiter) Allow(_ context.Context, key string, limit int, window time.Duration) (ratelimit.Result, error) {
	f.mu.Lock()
	f.keys = append(f.keys, key)
	f.mu.Unlock()
	if f.err != nil {
		return ratelimit.Result{}, f.err
	}
	if f.deny != nil && f.deny(key) {
		return ratelimit.Result{Allowed: false, Remaining: 0, ResetIn: 1500 * time.Millisecond}, nil
	}
	return ratelimit.Result{Allowed: true, Remaining: limit - 1, ResetIn: window}, nil
}

func (f *fakeLimiter) seen(fragment string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.keys {
		if strings.Contains(k, fragment) {
			return true
		}
	}
	return false
}

type fakeVerifier struct{ tokens map[string]*jwt.Claims }

func (f *fakeVerifier) Verify(token string) (*jwt.Claims, error) {
	if c, ok := f.tokens[token]; ok {
		return c, nil
	}
	return nil, errors.New("token expired at 2020-01-01 (this reason must never reach the client)")
}

type harness struct {
	t         *testing.T
	auth      *fakeAuth
	inventory *fakeInventory
	orders    *fakeOrders
	analytics *fakeAnalytics
	limiter   *fakeLimiter
	logs      *bytes.Buffer
	handler   http.Handler
}

func newHarness(t *testing.T, opts ...func(*Deps)) *harness {
	t.Helper()
	h := &harness{
		t:         t,
		auth:      &fakeAuth{t: t},
		inventory: &fakeInventory{t: t},
		orders:    &fakeOrders{t: t},
		analytics: &fakeAnalytics{t: t},
		limiter:   &fakeLimiter{},
		logs:      &bytes.Buffer{},
	}
	deps := Deps{
		Auth:      h.auth,
		Inventory: h.inventory,
		Orders:    h.orders,
		Analytics: h.analytics,
		Limiter:   h.limiter,
		Verifier: &fakeVerifier{tokens: map[string]*jwt.Claims{
			"user-token":      {UserID: userID, Email: "ann@example.com", Role: "user"},
			"other-token":     {UserID: otherID, Email: "bob@example.com", Role: "user"},
			"organizer-token": {UserID: organizerID, Email: "boss@example.com", Role: "organizer"},
			"admin-token":     {UserID: adminID, Email: "root@example.com", Role: "admin"},
			"forged-token":    {UserID: userID, Email: "ann@example.com", Role: "superuser"},
		}},
		SeatPriceCents: 5000,
		RequestTimeout: time.Second,
		Logger:         slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, opt := range opts {
		opt(&deps)
	}
	h.handler = New(deps)
	return h
}

// do sends a request through the whole handler. headers is a flat list of
// name, value pairs, applied after the default JSON Content-Type.
func (h *harness) do(method, path, body string, headers ...string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequestWithContext(h.t.Context(), method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func bearer(token string) []string { return []string{"Authorization", "Bearer " + token} }

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("response is not the expected JSON: %v\nbody: %s", err, rec.Body.String())
	}
	return v
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	return decode[errorBody](t, rec).Error.Code
}

func cookieNamed(rec *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func notFoundErr() error { return status.Error(codes.NotFound, "event not found") }
