package apierr

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNew_KeepsCodeMessageAndReason(t *testing.T) {
	err := New(codes.FailedPrecondition, ReasonPaymentDeclined, "payment failed")

	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition || st.Message() != "payment failed" {
		t.Errorf("status = %v %q, want FailedPrecondition %q", st.Code(), st.Message(), "payment failed")
	}
	if got := Reason(err); got != ReasonPaymentDeclined {
		t.Errorf("Reason = %q, want %q", got, ReasonPaymentDeclined)
	}
}

// The whole point: the two order failures share a gRPC code but must be told apart.
func TestReason_DistinguishesFailuresThatShareACode(t *testing.T) {
	seats := New(codes.FailedPrecondition, ReasonSeatsUnavailable, "unable to reserve seats")
	payment := New(codes.FailedPrecondition, ReasonPaymentDeclined, "payment failed")

	if status.Code(seats) != status.Code(payment) {
		t.Fatal("test setup: the two errors should share a status code")
	}
	if Reason(seats) == Reason(payment) {
		t.Error("two different failures carry the same reason, so a caller cannot react differently")
	}
	if !Is(seats, ReasonSeatsUnavailable) || Is(seats, ReasonPaymentDeclined) {
		t.Error("Is matched the wrong reason")
	}
}

func TestReason_IsEmptyWhenThereIsNone(t *testing.T) {
	for name, err := range map[string]error{
		"nil":                 nil,
		"plain error":         errors.New("boom"),
		"status without info": status.Error(codes.Internal, "boom"),
	} {
		if got := Reason(err); got != "" {
			t.Errorf("%s: Reason = %q, want empty", name, got)
		}
		if Is(err, ReasonPaymentDeclined) {
			t.Errorf("%s: Is reported a reason that is not there", name)
		}
	}
}

// A detail from some other system must not be mistaken for ours.
func TestReason_IgnoresOtherDomains(t *testing.T) {
	st, err := status.New(codes.FailedPrecondition, "x").WithDetails(&errdetails.ErrorInfo{Reason: ReasonPaymentDeclined, Domain: "someone-else"})
	if err != nil {
		t.Fatal(err)
	}

	if got := Reason(st.Err()); got != "" {
		t.Errorf("Reason = %q for a foreign domain, want empty", got)
	}
}

func TestReason_SurvivesWrapping(t *testing.T) {
	// A gRPC client returns the status error as is; wrapping it with %w must not hide it.
	wrapped := fmt.Errorf("charge: %w", New(codes.FailedPrecondition, ReasonPaymentDeclined, "payment failed"))

	if got := Reason(wrapped); got != ReasonPaymentDeclined {
		t.Errorf("Reason through a wrapper = %q, want %q", got, ReasonPaymentDeclined)
	}
}
