package server

import (
	"net/mail"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxEmailLen    = 254 // the longest address SMTP allows
	minPasswordLen = 8
	// bcrypt only reads the first 72 bytes of a password and, in current
	// versions, refuses longer input outright. Rejecting it here turns what
	// would be a confusing 500 into a clear message.
	maxPasswordLen = 72
)

// normalizeEmail returns the canonical form of an address: trimmed and
// lower-cased. Without that, "Ann@x.com" and "ann@x.com" would be two accounts
// that the unique constraint on the column treats as different.
func normalizeEmail(raw string) (string, error) {
	email := strings.ToLower(strings.TrimSpace(raw))
	if email == "" || len(email) > maxEmailLen {
		return "", status.Error(codes.InvalidArgument, "email is not valid")
	}
	// ParseAddress also accepts "Ann <ann@x.com>"; requiring the parsed address
	// to equal the input rejects that display-name form.
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email {
		return "", status.Error(codes.InvalidArgument, "email is not valid")
	}
	return email, nil
}

func validatePassword(password string) error {
	if len(password) < minPasswordLen || len(password) > maxPasswordLen {
		return status.Errorf(codes.InvalidArgument, "password must be %d to %d bytes", minPasswordLen, maxPasswordLen)
	}
	return nil
}
