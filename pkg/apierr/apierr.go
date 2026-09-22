// Package apierr lets a service say why a request failed in a way another
// service can act on, without parsing English.
//
// A gRPC status code says what kind of failure it was (FailedPrecondition), but
// not which one. order-svc uses FailedPrecondition both for "the seats are gone"
// and for "the payment was declined", and the gateway must answer those two
// differently. Matching on the human-readable message would break the moment
// someone rewords it. Instead the failure carries a machine-readable reason.
package apierr

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Domain scopes the reasons below to this system.
const Domain = "ticketwave"

// Reasons an order can fail. Consumers compare against these constants.
const (
	ReasonSeatsUnavailable = "SEATS_UNAVAILABLE"
	ReasonPaymentDeclined  = "PAYMENT_DECLINED"
	// ReasonServiceUnavailable means a dependency broke, not that the request was
	// refused. It is the one failure a client should retry.
	ReasonServiceUnavailable = "SERVICE_UNAVAILABLE"

	// ReasonRequestInProgress: an identical request (same idempotency key) is still being
	// processed. The client should wait and ask again, not start another.
	ReasonRequestInProgress = "REQUEST_IN_PROGRESS"
	// ReasonIdempotencyKeyReused: the key was already used for a DIFFERENT request. This
	// is a client bug, and silently returning the first request's result would hide it.
	ReasonIdempotencyKeyReused = "IDEMPOTENCY_KEY_REUSED"
)

// New builds a gRPC error with a status code, a message meant for people, and a
// machine-readable reason.
func New(code codes.Code, reason, message string) error {
	st := status.New(code, message)
	withReason, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: Domain})
	if err != nil {
		return st.Err() // attaching details failed; the code and message still get through
	}
	return withReason.Err()
}

// Reason returns the machine-readable reason attached to err, or "" if there is none.
func Reason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetDomain() == Domain {
			return info.GetReason()
		}
	}
	return ""
}

// Is reports whether err carries the given reason.
func Is(err error, reason string) bool {
	return err != nil && Reason(err) == reason
}
