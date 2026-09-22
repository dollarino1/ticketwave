package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"

	"github.com/dollarino1/ticketwave/pkg/apierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxBodyBytes = 64 << 10

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Every response here is personal or changes with state; none may be cached.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// The status line is already sent. If the client has gone away there is
	// nothing useful left to do with an encoding error.
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

// decodeJSON reads exactly one JSON object from the request body into dst. It is
// strict on purpose: unknown fields are rejected so a typo, or an attempt to
// smuggle in a field such as user_id, fails loudly instead of being ignored. It
// writes the error response itself and returns false when the body is unusable.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body is too large")
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, "invalid_json", "request body is empty")
		case strings.HasPrefix(err.Error(), "json: unknown field"):
			writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		default:
			writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON for this endpoint")
		}
		return false
	}
	// Anything after the first object, even another object, is a malformed request.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
		return false
	}
	return true
}

// httpError translates a failure from a downstream gRPC call into what the
// browser should see. Two rules shape it:
//
//   - A machine-readable reason (apierr) wins over the bare status code, because
//     several different failures share FailedPrecondition and the client must
//     react differently to each.
//   - Only messages the services wrote for callers are passed through. Anything
//     that could carry an internal detail, such as a SQL error or a stack, is
//     replaced by a fixed sentence.
func httpError(err error) (httpStatus int, code, message string) {
	switch apierr.Reason(err) {
	case apierr.ReasonSeatsUnavailable:
		return http.StatusConflict, "seats_unavailable", "one or more of those seats are no longer available"
	case apierr.ReasonPaymentDeclined:
		return http.StatusPaymentRequired, "payment_declined", "your payment was declined"
	case apierr.ReasonServiceUnavailable:
		return http.StatusServiceUnavailable, "service_unavailable", "the service is temporarily unavailable, please try again"
	case apierr.ReasonRequestInProgress:
		return http.StatusConflict, "request_in_progress", "a request with this Idempotency-Key is still being processed; wait a moment and retry it"
	case apierr.ReasonIdempotencyKeyReused:
		return http.StatusUnprocessableEntity, "idempotency_key_reused", "this Idempotency-Key was already used for a different request"
	}

	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, "internal", "internal error"
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, "invalid_argument", st.Message()
	case codes.Unauthenticated:
		return http.StatusUnauthorized, "unauthenticated", st.Message()
	case codes.PermissionDenied:
		return http.StatusForbidden, "permission_denied", st.Message()
	case codes.NotFound:
		return http.StatusNotFound, "not_found", st.Message()
	case codes.AlreadyExists:
		return http.StatusConflict, "already_exists", st.Message()
	case codes.FailedPrecondition:
		return http.StatusConflict, "conflict", st.Message()
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, "rate_limited", st.Message()
	case codes.Unavailable:
		return http.StatusServiceUnavailable, "service_unavailable", "the service is temporarily unavailable, please try again"
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, "timeout", "the request took too long, please try again"
	case codes.Canceled:
		return 499, "canceled", "request canceled" // nginx's convention for "the client hung up"
	default:
		return http.StatusInternalServerError, "internal", "internal error"
	}
}

// fail answers a request whose downstream call failed. Server-side failures are
// logged with the real error, since the client is deliberately told nothing.
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	httpStatus, code, message := httpError(err)
	if httpStatus >= 500 {
		a.log.Error("downstream call failed",
			slog.String("request_id", requestIDFrom(r.Context())),
			slog.String("path", r.URL.Path),
			slog.Any("error", err))
	}
	writeError(w, httpStatus, code, message)
}
