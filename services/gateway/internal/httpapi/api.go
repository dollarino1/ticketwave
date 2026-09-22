// Package httpapi is the gateway's HTTP surface: it turns REST calls from the
// browser into gRPC calls to the services, and is where authentication,
// authorization, rate limiting and input hygiene are enforced.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/dollarino1/ticketwave/pkg/authz"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/dollarino1/ticketwave/pkg/ratelimit"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
)

// The interfaces below are the slice of each generated gRPC client that the
// gateway actually calls. Declaring them here, where they are used, is idiomatic
// Go ("accept interfaces"), and it lets tests substitute fakes without a network.

type AuthClient interface {
	Register(ctx context.Context, in *authv1.RegisterRequest, opts ...grpc.CallOption) (*authv1.RegisterResponse, error)
	Login(ctx context.Context, in *authv1.LoginRequest, opts ...grpc.CallOption) (*authv1.LoginResponse, error)
	Refresh(ctx context.Context, in *authv1.RefreshRequest, opts ...grpc.CallOption) (*authv1.RefreshResponse, error)
	Logout(ctx context.Context, in *authv1.LogoutRequest, opts ...grpc.CallOption) (*authv1.LogoutResponse, error)
}

type InventoryClient interface {
	CreateEvent(ctx context.Context, in *inventoryv1.CreateEventRequest, opts ...grpc.CallOption) (*inventoryv1.CreateEventResponse, error)
	GetEvent(ctx context.Context, in *inventoryv1.GetEventRequest, opts ...grpc.CallOption) (*inventoryv1.GetEventResponse, error)
	ListEvents(ctx context.Context, in *inventoryv1.ListEventsRequest, opts ...grpc.CallOption) (*inventoryv1.ListEventsResponse, error)
	ListSeats(ctx context.Context, in *inventoryv1.ListSeatsRequest, opts ...grpc.CallOption) (*inventoryv1.ListSeatsResponse, error)
}

type OrderClient interface {
	CreateOrder(ctx context.Context, in *orderv1.CreateOrderRequest, opts ...grpc.CallOption) (*orderv1.CreateOrderResponse, error)
	GetOrder(ctx context.Context, in *orderv1.GetOrderRequest, opts ...grpc.CallOption) (*orderv1.GetOrderResponse, error)
}

type AnalyticsClient interface {
	ListEventStats(ctx context.Context, in *analyticsv1.ListEventStatsRequest, opts ...grpc.CallOption) (*analyticsv1.ListEventStatsResponse, error)
}

// RateLimiter is satisfied by *ratelimit.Limiter.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (ratelimit.Result, error)
}

// TokenVerifier is satisfied by *jwt.Verifier.
type TokenVerifier interface {
	Verify(token string) (*jwt.Claims, error)
}

// Deps is everything the API needs from the outside world.
type Deps struct {
	Auth      AuthClient
	Inventory InventoryClient
	Orders    OrderClient
	Analytics AnalyticsClient
	Limiter   RateLimiter
	Verifier  TokenVerifier

	SeatPriceCents    int64
	CookieSecure      bool
	TrustProxyHeaders bool
	RequestTimeout    time.Duration
	Logger            *slog.Logger
}

// API holds the dependencies the handlers share.
type API struct {
	auth      AuthClient
	inventory InventoryClient
	orders    OrderClient
	analytics AnalyticsClient
	limiter   RateLimiter
	verifier  TokenVerifier

	seatPriceCents int64
	cookieSecure   bool
	trustProxy     bool
	timeout        time.Duration
	log            *slog.Logger
}

// New builds the handler for the whole API.
func New(d Deps) http.Handler {
	a := &API{
		auth:           d.Auth,
		inventory:      d.Inventory,
		orders:         d.Orders,
		analytics:      d.Analytics,
		limiter:        d.Limiter,
		verifier:       d.Verifier,
		seatPriceCents: d.SeatPriceCents,
		cookieSecure:   d.CookieSecure,
		trustProxy:     d.TrustProxyHeaders,
		timeout:        d.RequestTimeout,
		log:            d.Logger,
	}
	if a.log == nil {
		a.log = slog.Default()
	}
	if a.timeout <= 0 {
		a.timeout = 8 * time.Second
	}

	mux := http.NewServeMux()
	a.routes(mux)

	// Outermost first. The request ID comes first so that everything after it can
	// be tied to one request in the logs. Logging sits outside the recoverer so a
	// request that panicked is still logged, with the 500 the client received.
	handler := chain(mux,
		a.requestID,
		a.logging,
		a.recoverer,
		securityHeaders,
	)

	// otelhttp starts the trace's root span (the browser sends no traceparent of its
	// own) and puts it in the request's context, before anything else runs — that is
	// what lets a.logging read a trace ID, and every gRPC call made further in reuse
	// the same trace. With no tracer configured (SetupTracing was never called with a
	// real endpoint) this still runs, but records nothing, so it is always on.
	// spanName, set per route in routes(), gives each span a name a person can read
	// ("GET /api/orders/{id}") instead of otelhttp's generic default.
	return otelhttp.NewHandler(handler, "gateway")
}

func (a *API) routes(mux *http.ServeMux) {
	perIP := func(name string, limit int) middleware { return a.limit(name, limit, time.Minute, a.byIP) }
	perUser := func(name string, limit int) middleware { return a.limit(name, limit, time.Minute, a.byUser) }
	organizer := a.requireRole(authz.RoleOrganizer)

	// route registers a handler wrapped in metrics and named tracing. Both sit
	// OUTSIDE the route-specific middleware, so a request refused by auth or a rate
	// limit is still counted, and its span still carries the route it was refused on.
	route := func(pattern string, h http.HandlerFunc, mws ...middleware) {
		mux.Handle(pattern, instrument(pattern)(spanName(pattern)(chain(h, mws...))))
	}

	route("GET /healthz", a.healthz)

	// Account. These are the endpoints attackers hammer, so they get the tightest limits.
	route("POST /api/auth/register", a.register, perIP("register", 5))
	route("POST /api/auth/login", a.login, perIP("login", 10))
	route("POST /api/auth/refresh", a.refresh, a.csrf, perIP("refresh", 30))
	route("POST /api/auth/logout", a.logout, a.csrf, perIP("logout", 30))

	// Browsing needs no account.
	route("GET /api/events", a.listEvents, perIP("browse", 120))
	route("GET /api/events/{id}", a.getEvent, perIP("browse", 120))
	route("GET /api/events/{id}/seats", a.listSeats, perIP("browse", 120))

	// Buying needs a signed-in user.
	route("POST /api/orders", a.createOrder, a.authenticate, perUser("orders", 20))
	route("GET /api/orders/{id}", a.getOrder, a.authenticate, perUser("read", 120))

	// Running events needs an organizer.
	route("POST /api/events", a.createEvent, a.authenticate, organizer, perUser("write", 30))
	route("GET /api/analytics/events", a.listAnalytics, a.authenticate, organizer, perUser("read", 120))

	mux.Handle("/", instrument(unmatched)(spanName(unmatched)(http.HandlerFunc(a.notFound))))
}

func (a *API) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not_found", "no such endpoint")
}

// callContext bounds the downstream work done for one request, so a stuck
// service cannot hold a browser connection open indefinitely.
func (a *API) callContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), a.timeout)
}
