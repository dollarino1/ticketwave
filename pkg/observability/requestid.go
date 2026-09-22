package observability

import "context"

// RequestIDHeader is the HTTP header, and RequestIDMetadata the gRPC metadata key,
// that carry one request's ID from the browser edge through every service it touches.
// With the same ID in every service's log lines, "what happened to this request?" is
// a single search instead of a guess about timestamps.
const (
	RequestIDHeader   = "X-Request-ID"
	RequestIDMetadata = "x-request-id" // gRPC metadata keys are lower-case
)

type requestIDKey struct{}

// WithRequestID returns a context carrying id.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the ID in ctx, or "" if there is none.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
