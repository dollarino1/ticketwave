//go:build integration

package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"sync"
	"testing"
	"time"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/dollarino1/ticketwave/pkg/pgtest"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testPassword = "correct horse battery"

// Generating an RSA key is slow, so every test shares one.
var testKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

// Run with AUTH_DATABASE_URL pointing at the auth database, e.g.
//
//	AUTH_DATABASE_URL="postgres://ticketwave:ticketwave@localhost:5433/auth?sslmode=disable" \
//	  go test -tags integration ./services/auth/...
func newTestServer(t *testing.T, organizers ...string) (*Server, *jwt.Verifier, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.NewPool(t, "AUTH_DATABASE_URL", "migrations/auth")
	key := testKey()
	return New(pool, jwt.NewSigner(key), organizers), jwt.NewVerifier(&key.PublicKey), pool
}

func register(t *testing.T, s *Server, email string) string {
	t.Helper()
	resp, err := s.Register(context.Background(), &authv1.RegisterRequest{Email: email, Password: testPassword})
	if err != nil {
		t.Fatalf("Register(%s): %v", email, err)
	}
	return resp.UserId
}

func login(t *testing.T, s *Server, email string) *authv1.LoginResponse {
	t.Helper()
	resp, err := s.Login(context.Background(), &authv1.LoginRequest{Email: email, Password: testPassword})
	if err != nil {
		t.Fatalf("Login(%s): %v", email, err)
	}
	return resp
}

func refresh(s *Server, token string) (*authv1.RefreshResponse, error) {
	return s.Refresh(context.Background(), &authv1.RefreshRequest{RefreshToken: token})
}

func TestRegisterThenLogin_IssuesVerifiableTokens(t *testing.T) {
	s, verifier, pool := newTestServer(t)
	userID := register(t, s, "Ann@Example.com")

	resp := login(t, s, "  ann@EXAMPLE.com ") // different case and stray spaces: still the same account

	claims, err := verifier.Verify(resp.AccessToken)
	if err != nil {
		t.Fatalf("the access token does not verify: %v", err)
	}
	if claims.UserID != userID || claims.Email != "ann@example.com" || claims.Role != "user" {
		t.Errorf("claims = %+v, want user %s, ann@example.com, role user", claims, userID)
	}
	if resp.UserId != userID || resp.Email != "ann@example.com" || resp.Role != "user" {
		t.Errorf("response user info = %s/%s/%s, want %s/ann@example.com/user", resp.UserId, resp.Email, resp.Role, userID)
	}
	if resp.ExpiresIn != int64(accessTokenTTL.Seconds()) || resp.RefreshToken == "" || resp.RefreshToken == resp.AccessToken {
		t.Errorf("expires_in=%d refresh=%q, want %ds and a distinct refresh token", resp.ExpiresIn, resp.RefreshToken, int(accessTokenTTL.Seconds()))
	}

	// Only the hash of the refresh token may be stored, never the token itself.
	var plain, hashed int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FILTER (WHERE token_hash = $1), count(*) FILTER (WHERE token_hash = $2) FROM refresh_tokens`,
		resp.RefreshToken, hashToken(resp.RefreshToken)).Scan(&plain, &hashed)
	if err != nil {
		t.Fatal(err)
	}
	if plain != 0 || hashed != 1 {
		t.Errorf("the raw token is stored %d times and its hash %d times, want 0 and 1", plain, hashed)
	}
}

func TestRegister_OrganizerEmailsGetTheOrganizerRole(t *testing.T) {
	s, verifier, _ := newTestServer(t, "Boss@Venue.com")
	register(t, s, "boss@venue.com")
	register(t, s, "fan@example.com")

	boss := login(t, s, "boss@venue.com")
	fan := login(t, s, "fan@example.com")

	if boss.Role != "organizer" {
		t.Errorf("configured organizer got role %q", boss.Role)
	}
	if fan.Role != "user" {
		t.Errorf("an ordinary registrant got role %q, want user", fan.Role)
	}
	if claims, err := verifier.Verify(boss.AccessToken); err != nil || claims.Role != "organizer" {
		t.Errorf("the organizer's access token carries role %q (err %v), want organizer", claims.Role, err)
	}
}

func TestRegister_EmailsAreUniqueRegardlessOfCase(t *testing.T) {
	s, _, _ := newTestServer(t)
	register(t, s, "ann@example.com")

	_, err := s.Register(context.Background(), &authv1.RegisterRequest{Email: "ANN@example.com", Password: testPassword})

	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("got %v, want AlreadyExists: a differently-cased address must not be a second account", err)
	}
}

func TestRegister_RejectsBadInputAndCreatesNothing(t *testing.T) {
	s, _, pool := newTestServer(t)
	cases := map[string]struct{ email, password string }{
		"not an email":       {"nope", testPassword},
		"empty email":        {"", testPassword},
		"password too short": {"a@example.com", "short"},
		"password too long":  {"a@example.com", strings.Repeat("x", maxPasswordLen+1)},
		"empty password":     {"a@example.com", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.Register(context.Background(), &authv1.RegisterRequest{Email: tc.email, Password: tc.password})
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("got %v, want InvalidArgument", err)
			}
		})
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM users`).Scan(&n); err != nil || n != 0 {
		t.Errorf("%d user(s) exist after only invalid registrations (err %v), want 0", n, err)
	}
}

func TestLogin_UnknownEmailAndWrongPasswordAreIndistinguishable(t *testing.T) {
	s, _, _ := newTestServer(t)
	register(t, s, "ann@example.com")
	ctx := context.Background()

	_, wrongPassword := s.Login(ctx, &authv1.LoginRequest{Email: "ann@example.com", Password: "not the password"})
	_, unknownEmail := s.Login(ctx, &authv1.LoginRequest{Email: "nobody@example.com", Password: testPassword})
	_, malformedEmail := s.Login(ctx, &authv1.LoginRequest{Email: "nope", Password: testPassword})

	for name, err := range map[string]error{"wrong password": wrongPassword, "unknown email": unknownEmail, "malformed email": malformedEmail} {
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: got %v, want Unauthenticated", name, err)
		}
		if status.Convert(err).Message() != status.Convert(wrongPassword).Message() {
			t.Errorf("%s answers %q but a wrong password answers %q; the difference reveals which emails exist",
				name, status.Convert(err).Message(), status.Convert(wrongPassword).Message())
		}
	}
}

// Identical messages are not enough if one path answers in 1ms and the other in
// 50ms. The unknown-email path must still pay for a bcrypt comparison.
func TestLogin_UnknownEmailStillPaysForAPasswordCheck(t *testing.T) {
	s, _, _ := newTestServer(t)
	ctx := context.Background()
	req := &authv1.LoginRequest{Email: "nobody@example.com", Password: testPassword}
	_, _ = s.Login(ctx, req) // the first call builds the dummy hash; time the second

	start := time.Now()
	_, err := s.Login(ctx, req)
	elapsed := time.Since(start)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v, want Unauthenticated", err)
	}
	// A database lookup alone is a millisecond or two; bcrypt at the default cost
	// is tens of milliseconds. 10ms cleanly separates them.
	if elapsed < 10*time.Millisecond {
		t.Errorf("an unknown-email login took %v: too fast to have run a bcrypt comparison, so timing leaks which emails exist", elapsed)
	}
}

func TestRefresh_RotatesTheTokenAndTreatsReuseAsTheft(t *testing.T) {
	s, _, _ := newTestServer(t)
	register(t, s, "ann@example.com")
	first := login(t, s, "ann@example.com").RefreshToken

	second, err := refresh(s, first)
	if err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if second.RefreshToken == first {
		t.Fatal("refresh returned the same refresh token; rotation did not happen")
	}

	// Presenting the already-used token again means someone else has a copy.
	if _, err := refresh(s, first); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("reusing a rotated token got %v, want Unauthenticated", err)
	}
	// Consequence: every session of that user is now revoked, even the newest.
	if _, err := refresh(s, second.RefreshToken); status.Code(err) != codes.Unauthenticated {
		t.Errorf("after reuse was detected, the newest token still worked (%v); all sessions should be revoked", err)
	}
}

func TestRefresh_SimultaneousUseOfOneTokenIssuesOnlyOneSession(t *testing.T) {
	s, _, pool := newTestServer(t)
	register(t, s, "ann@example.com")
	token := login(t, s, "ann@example.com").RefreshToken
	pgtest.WarmPool(t, pool) // the refreshes must really be simultaneous, or the race is hidden

	const attempts = 8
	var mu sync.Mutex
	successes := 0
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Go(func() {
			<-start
			if _, err := refresh(s, token); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if status.Code(err) != codes.Unauthenticated {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	if successes != 1 {
		t.Errorf("%d simultaneous refreshes of one token succeeded, want exactly 1: the rest must be seen as reuse", successes)
	}
}

func TestRefresh_RejectsExpiredUnknownAndEmptyTokens(t *testing.T) {
	s, _, pool := newTestServer(t)
	register(t, s, "ann@example.com")
	expired := login(t, s, "ann@example.com").RefreshToken
	if _, err := pool.Exec(context.Background(), `UPDATE refresh_tokens SET expires_at = now() - interval '1 minute'`); err != nil {
		t.Fatal(err)
	}

	for name, token := range map[string]string{"expired": expired, "unknown": "not-a-real-token", "empty": ""} {
		if _, err := refresh(s, token); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s token got %v, want Unauthenticated", name, err)
		}
	}
}

func TestRefresh_PicksUpARoleChange(t *testing.T) {
	s, verifier, pool := newTestServer(t)
	register(t, s, "ann@example.com")
	token := login(t, s, "ann@example.com").RefreshToken
	if _, err := pool.Exec(context.Background(), `UPDATE users SET role = 'organizer' WHERE email = 'ann@example.com'`); err != nil {
		t.Fatal(err)
	}

	resp, err := refresh(s, token)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	claims, err := verifier.Verify(resp.AccessToken)
	if err != nil || claims.Role != "organizer" || resp.Role != "organizer" {
		t.Errorf("after promotion the token's role is %q and the response's %q (err %v), want organizer in both", claims.Role, resp.Role, err)
	}
}

func TestLogout_RevokesTheTokenAndIsIdempotent(t *testing.T) {
	s, _, _ := newTestServer(t)
	register(t, s, "ann@example.com")
	token := login(t, s, "ann@example.com").RefreshToken
	ctx := context.Background()

	for i := 0; i < 2; i++ { // twice: logging out again must not fail
		if _, err := s.Logout(ctx, &authv1.LogoutRequest{RefreshToken: token}); err != nil {
			t.Fatalf("Logout #%d: %v", i+1, err)
		}
	}
	if _, err := s.Logout(ctx, &authv1.LogoutRequest{RefreshToken: "never issued"}); err != nil {
		t.Errorf("logging out an unknown token returned %v; it must succeed silently so tokens cannot be probed", err)
	}

	if _, err := refresh(s, token); status.Code(err) != codes.Unauthenticated {
		t.Errorf("a logged-out token could still be refreshed: %v", err)
	}
}
