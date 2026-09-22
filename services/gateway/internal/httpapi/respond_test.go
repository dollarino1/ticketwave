package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/dollarino1/ticketwave/pkg/apierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantStatus  int
		wantCode    string
		wantMessage string // "" means: only check the message is not empty
	}{
		{"seats gone", apierr.New(codes.FailedPrecondition, apierr.ReasonSeatsUnavailable, "x"), 409, "seats_unavailable", ""},
		{"card declined", apierr.New(codes.FailedPrecondition, apierr.ReasonPaymentDeclined, "x"), 402, "payment_declined", ""},
		{"a service is down", apierr.New(codes.Unavailable, apierr.ReasonServiceUnavailable, "x"), 503, "service_unavailable", ""},
		{"invalid argument keeps its message", status.Error(codes.InvalidArgument, "email is not valid"), 400, "invalid_argument", "email is not valid"},
		{"unauthenticated keeps its message", status.Error(codes.Unauthenticated, "Invalid email or password"), 401, "unauthenticated", "Invalid email or password"},
		{"permission denied", status.Error(codes.PermissionDenied, "no"), 403, "permission_denied", "no"},
		{"not found", status.Error(codes.NotFound, "event not found"), 404, "not_found", "event not found"},
		{"already exists", status.Error(codes.AlreadyExists, "email already registered"), 409, "already_exists", "email already registered"},
		{"failed precondition without a reason", status.Error(codes.FailedPrecondition, "nope"), 409, "conflict", "nope"},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "slow down"), 429, "rate_limited", "slow down"},
		{"unavailable", status.Error(codes.Unavailable, "connection refused 10.0.0.5:5432"), 503, "service_unavailable", ""},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "ctx"), 504, "timeout", ""},
		{"canceled", status.Error(codes.Canceled, "ctx"), 499, "canceled", ""},
		{"internal", status.Error(codes.Internal, "pq: syntax error at or near SELECT"), 500, "internal", "internal error"},
		{"unknown", status.Error(codes.Unknown, "secret detail"), 500, "internal", "internal error"},
		{"not even a gRPC error", errors.New("dial tcp 10.0.0.5:5432: refused"), 500, "internal", "internal error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotCode, gotMessage := httpError(tc.err)
			if gotStatus != tc.wantStatus || gotCode != tc.wantCode {
				t.Errorf("got %d %q, want %d %q", gotStatus, gotCode, tc.wantStatus, tc.wantCode)
			}
			if tc.wantMessage != "" && gotMessage != tc.wantMessage {
				t.Errorf("message = %q, want %q", gotMessage, tc.wantMessage)
			}
			if gotMessage == "" {
				t.Error("the client got an empty message")
			}
		})
	}
}

// Internal details must never reach the browser, whatever the status.
func TestHTTPError_NeverLeaksInternalDetails(t *testing.T) {
	secrets := []string{"pq:", "10.0.0.5", "SELECT", "secret detail", "connection refused"}
	for _, code := range []codes.Code{codes.Internal, codes.Unknown, codes.Unavailable, codes.DataLoss, codes.Unimplemented} {
		_, _, message := httpError(status.Error(code, "pq: failure at 10.0.0.5 while running SELECT: secret detail: connection refused"))
		for _, secret := range secrets {
			if strings.Contains(message, secret) {
				t.Errorf("%v leaked %q to the client in %q", code, secret, message)
			}
		}
	}
}

// A machine-readable reason beats the coarse status code, because several
// different failures share FailedPrecondition.
func TestHTTPError_ReasonWinsOverTheStatusCode(t *testing.T) {
	err := apierr.New(codes.FailedPrecondition, apierr.ReasonPaymentDeclined, "payment failed")

	if got, _, _ := httpError(err); got != http.StatusPaymentRequired {
		t.Errorf("status = %d, want 402: a declined card is not a generic conflict", got)
	}
}

func TestDecodeJSON_IsStrict(t *testing.T) {
	const maxOK = maxBodyBytes - 200
	cases := []struct {
		name        string
		body        string
		contentType string // "" leaves the harness default (application/json)
		want        int
		wantCode    string
	}{
		{"valid", `{"email":"a@b.co","password":"x"}`, "", 201, ""},
		{"valid with a charset parameter", `{"email":"a@b.co","password":"x"}`, "application/json; charset=utf-8", 201, ""},
		{"unknown field", `{"email":"a@b.co","password":"x","role":"admin"}`, "", 400, "invalid_json"},
		{"second object after the first", `{"email":"a@b.co","password":"x"}{"email":"e"}`, "", 400, "invalid_json"},
		{"trailing garbage", `{"email":"a@b.co","password":"x"} nonsense`, "", 400, "invalid_json"},
		{"not JSON", `not json`, "", 400, "invalid_json"},
		{"wrong field type", `{"email":123}`, "", 400, "invalid_json"},
		{"an array instead of an object", `[1,2]`, "", 400, "invalid_json"},
		{"wrong content type", `{"email":"a"}`, "text/plain", 415, "unsupported_media_type"},
		{"form content type", `email=a`, "application/x-www-form-urlencoded", 415, "unsupported_media_type"},
		{"body too large", `{"email":"` + strings.Repeat("a", maxBodyBytes+10) + `"}`, "", 413, "body_too_large"},
		{"large but allowed", `{"email":"` + strings.Repeat("a", maxOK) + `","password":"x"}`, "", 201, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			if tc.want == 201 {
				h.auth.register = func(*authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
					return &authv1.RegisterResponse{UserId: userID}, nil
				}
			} // otherwise Auth.Register must not be called; the fake fails the test if it is

			var headers []string
			if tc.contentType != "" {
				headers = []string{"Content-Type", tc.contentType}
			}
			rec := h.do("POST", "/api/auth/register", tc.body, headers...)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %.200s)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.wantCode != "" && errCode(t, rec) != tc.wantCode {
				t.Errorf("error code = %q, want %q", errCode(t, rec), tc.wantCode)
			}
		})
	}
}

func TestDecodeJSON_EmptyBodyIsRejected(t *testing.T) {
	h := newHarness(t)

	rec := h.do("POST", "/api/auth/register", "", "Content-Type", "application/json")

	if rec.Code != http.StatusBadRequest || errCode(t, rec) != "invalid_json" {
		t.Errorf("got %d %q, want 400 invalid_json", rec.Code, errCode(t, rec))
	}
}
