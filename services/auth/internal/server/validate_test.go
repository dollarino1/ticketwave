package server

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeEmail(t *testing.T) {
	cases := map[string]struct {
		in   string
		want string // empty means "must be rejected"
	}{
		"plain":                   {"ann@example.com", "ann@example.com"},
		"upper case is folded":    {"ANN@Example.COM", "ann@example.com"},
		"whitespace is trimmed":   {"  ann@example.com \n", "ann@example.com"},
		"plus addressing is kept": {"ann+tickets@example.com", "ann+tickets@example.com"},
		"empty":                   {"", ""},
		"only spaces":             {"   ", ""},
		"no at sign":              {"annexample.com", ""},
		"display name form":       {"Ann <ann@example.com>", ""},
		"two addresses":           {"a@example.com, b@example.com", ""},
		"space inside":            {"an n@example.com", ""},
		"too long":                {strings.Repeat("a", maxEmailLen) + "@example.com", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := normalizeEmail(tc.in)
			if tc.want == "" {
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("got %q, %v; want InvalidArgument", got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Errorf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestValidatePassword(t *testing.T) {
	cases := map[string]struct {
		password string
		ok       bool
	}{
		"shortest allowed":             {strings.Repeat("x", minPasswordLen), true},
		"one byte too short":           {strings.Repeat("x", minPasswordLen-1), false},
		"longest bcrypt reads":         {strings.Repeat("x", maxPasswordLen), true},
		"one byte too long":            {strings.Repeat("x", maxPasswordLen+1), false},
		"empty":                        {"", false},
		"multi-byte counts bytes (72)": {strings.Repeat("é", 36), true},
		"multi-byte counts bytes (74)": {strings.Repeat("é", 37), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validatePassword(tc.password)
			if tc.ok && err != nil {
				t.Errorf("got %v, want no error", err)
			}
			if !tc.ok && status.Code(err) != codes.InvalidArgument {
				t.Errorf("got %v, want InvalidArgument", err)
			}
		})
	}
}

func TestHashToken_IsDeterministicAndDoesNotEchoTheToken(t *testing.T) {
	a, b := hashToken("token-one"), hashToken("token-one")
	if a != b {
		t.Error("the same token hashed to two different values, so it could never be looked up")
	}
	if a == hashToken("token-two") {
		t.Error("two different tokens hashed to the same value")
	}
	if strings.Contains(a, "token-one") || len(a) != 64 {
		t.Errorf("hash %q should be 64 hex characters and not contain the token", a)
	}
}

func TestRoleFor_OnlyConfiguredEmailsBecomeOrganizers(t *testing.T) {
	s := New(nil, nil, []string{" Boss@Venue.com ", "", "owner@venue.com"})

	cases := map[string]string{
		"boss@venue.com":  "organizer", // configured with different case and spaces
		"owner@venue.com": "organizer",
		"fan@example.com": "user",
		"":                "user",
	}
	for email, want := range cases {
		if got := string(s.roleFor(email)); got != want {
			t.Errorf("roleFor(%q) = %q, want %q", email, got, want)
		}
	}
}
