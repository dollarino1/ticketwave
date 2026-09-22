package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/dollarino1/ticketwave/pkg/authz"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type middleware func(http.Handler) http.Handler

// chain wraps h in the middlewares, first one outermost: chain(h, a, b) runs a,
// then b, then h.
func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type ctxKey int

const (
	claimsKey ctxKey = iota
	callerKey
)

// requestIDFrom is the observability package's request ID, so that the same value the
// gateway logs is the one forwarded to every service it calls.
func requestIDFrom(ctx context.Context) string { return observability.RequestID(ctx) }

// claimsFrom returns the verified token claims that authenticate stored.
func claimsFrom(ctx context.Context) (*jwt.Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*jwt.Claims)
	return c, ok && c != nil
}

// caller lets the logging middleware, which runs outside authenticate, learn who
// the request turned out to be once authentication has happened deeper in.
type caller struct{ userID string }

// validRequestID accepts an ID a proxy or client sent, but only if it is short
// and plain: it ends up in logs, where a crafted value could forge log lines.
var validRequestID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func (a *API) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !validRequestID.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)

		ctx := observability.WithRequestID(r.Context(), id)
		ctx = context.WithValue(ctx, callerKey, &caller{})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// spanName sets the name of the request's current span (started by otelhttp, further
// out) to something a person reading Tempo can recognise, such as "GET
// /api/orders/{id}", instead of otelhttp's generic default. It runs even when
// tracing is off: SpanFromContext then returns a no-op span, and naming it is free.
func spanName(name string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			trace.SpanFromContext(r.Context()).SetName(name)
			next.ServeHTTP(w, r)
		})
	}
}

// recoverer turns a panic in a handler into a 500 instead of a dropped
// connection. The client learns nothing about the panic; the logs get the stack.
func (a *API) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				a.log.Error("panic in handler",
					slog.String("request_id", requestIDFrom(r.Context())),
					slog.String("trace_id", observability.TraceID(r.Context())),
					slog.String("span_id", observability.SpanID(r.Context())),
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())))
				writeError(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// statusRecorder remembers the status a handler wrote so it can be logged.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the real writer, which streaming
// endpoints need to flush and to set their own deadlines.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (a *API) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		who, _ := r.Context().Value(callerKey).(*caller)
		attrs := []any{
			slog.String("request_id", requestIDFrom(r.Context())),
			slog.String("trace_id", observability.TraceID(r.Context())),
			slog.String("span_id", observability.SpanID(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		}
		if who != nil && who.userID != "" {
			attrs = append(attrs, slog.String("user_id", who.userID))
		}
		a.log.Info("request", attrs...)
	})
}

// authenticate requires a valid access token and stores its claims for the
// handlers behind it.
func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthenticated", "sign in to continue")
			return
		}
		claims, err := a.verifier.Verify(token)
		if err != nil {
			// The reason (expired, bad signature, wrong algorithm) is for the logs,
			// never the client: it would only help someone forging tokens.
			a.log.Debug("token rejected", slog.String("request_id", requestIDFrom(r.Context())), slog.Any("error", err))
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeError(w, http.StatusUnauthorized, "unauthenticated", "your session is not valid, sign in again")
			return
		}

		ctx := context.WithValue(r.Context(), claimsKey, claims)
		if who, ok := r.Context().Value(callerKey).(*caller); ok {
			who.userID = claims.UserID
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func bearerToken(r *http.Request) (string, bool) {
	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

// requireRole allows only callers whose role is at least min. It must run after
// authenticate; reaching it without claims is treated as unauthenticated.
func (a *API) requireRole(min authz.Role) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := claimsFrom(r.Context())
			if !ok {
				writeError(w, http.StatusUnauthorized, "unauthenticated", "sign in to continue")
				return
			}
			if !authz.Role(claims.Role).AtLeast(min) {
				writeError(w, http.StatusForbidden, "permission_denied", "you do not have permission to do that")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

const (
	csrfHeader = "X-Requested-With"
	csrfValue  = "ticketwave-web"
)

// csrf guards endpoints that authenticate with the refresh cookie. A browser
// attaches cookies to requests started by any site, so the cookie alone proves
// nothing about who initiated the request. A page on another origin cannot add a
// custom header without the browser first asking us for permission (CORS
// preflight), which we never grant, so requiring one shuts that door. This sits
// on top of the cookie's SameSite=Strict, which already stops most cross-site sends.
func (a *API) csrf(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(csrfHeader) != csrfValue {
			writeError(w, http.StatusForbidden, "csrf_check_failed", "missing or wrong "+csrfHeader+" header")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// byIP and byUser pick what a rate limit is counted against.
func (a *API) byIP(r *http.Request) string { return clientIP(r, a.trustProxy) }

func (a *API) byUser(r *http.Request) string {
	if claims, ok := claimsFrom(r.Context()); ok {
		return "user:" + claims.UserID
	}
	return a.byIP(r) // no token: fall back to the address
}

// limit allows at most `limit` requests per window for whatever key selects.
func (a *API) limit(name string, limit int, window time.Duration, key func(*http.Request) string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !a.allow(w, r, name+":"+key(r), limit, window) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// allow spends one attempt against key. It writes the 429 itself and returns
// false when the caller is over the limit.
//
// If Redis is unreachable it lets the request through and logs the failure.
// Failing closed would turn a Redis outage into a full site outage. The cost is
// that rate limits pause while Redis is down, which is the lesser evil here.
func (a *API) allow(w http.ResponseWriter, r *http.Request, key string, limit int, window time.Duration) bool {
	res, err := a.limiter.Allow(r.Context(), key, limit, window)
	if err != nil {
		a.log.Error("rate limiter unavailable, letting the request through",
			slog.String("request_id", requestIDFrom(r.Context())), slog.Any("error", err))
		return true
	}
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limit))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
	if res.Allowed {
		return true
	}
	rateLimited.WithLabelValues(limitName(key)).Inc()
	retry := int((res.ResetIn + time.Second - 1) / time.Second) // round up: never tell a client to come back too early
	if retry < 1 {
		retry = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests, try again shortly")
	return false
}

// clientIP is the address the request came from. Behind our own proxy it is
// X-Real-IP, which nginx overwrites with the real peer address. Trusting that
// header anywhere else would let a client name any address it likes.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); net.ParseIP(ip) != nil {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// hashKey turns an identifier such as an email address into a fixed-size token
// for use in a Redis key, so personal data never sits in Redis in the clear.
func hashKey(s string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(s))))
	return hex.EncodeToString(sum[:8])
}
