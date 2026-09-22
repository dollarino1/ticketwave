package httpapi

import (
	"net/http"
	"strings"
	"testing"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	loginBody     = `{"email":"Ann@Example.com","password":"correct horse"}`
	refreshSecret = "refresh-secret-value"
)

var csrf = []string{csrfHeader, csrfValue}

func loggedIn(*authv1.LoginRequest) (*authv1.LoginResponse, error) {
	return &authv1.LoginResponse{
		AccessToken: "access-token", RefreshToken: refreshSecret, ExpiresIn: 900,
		UserId: userID, Email: "ann@example.com", Role: "user",
	}, nil
}

func TestRegister_CreatesTheAccount(t *testing.T) {
	h := newHarness(t)
	var got *authv1.RegisterRequest
	h.auth.register = func(in *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
		got = in
		return &authv1.RegisterResponse{UserId: userID}, nil
	}

	rec := h.do("POST", "/api/auth/register", `{"email":"ann@example.com","password":"correct horse"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	if got.Email != "ann@example.com" || got.Password != "correct horse" {
		t.Errorf("auth-svc received %v", got)
	}
	if body := decode[map[string]string](t, rec); body["user_id"] != userID {
		t.Errorf("body = %v, want user_id %s", body, userID)
	}
}

func TestRegister_MapsServiceErrors(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
		code string
	}{
		"email taken":  {status.Error(codes.AlreadyExists, "email already registered"), 409, "already_exists"},
		"bad password": {status.Error(codes.InvalidArgument, "password must be 8 to 72 bytes"), 400, "invalid_argument"},
		"auth is down": {status.Error(codes.Unavailable, "connection refused"), 503, "service_unavailable"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.auth.register = func(*authv1.RegisterRequest) (*authv1.RegisterResponse, error) { return nil, tc.err }

			rec := h.do("POST", "/api/auth/register", `{"email":"a@b.co","password":"x"}`)

			if rec.Code != tc.want || errCode(t, rec) != tc.code {
				t.Errorf("got %d %q, want %d %q", rec.Code, errCode(t, rec), tc.want, tc.code)
			}
		})
	}
}

func TestLogin_PutsTheRefreshTokenInAnHttpOnlyCookieAndNotInTheBody(t *testing.T) {
	h := newHarness(t)
	h.auth.login = loggedIn

	rec := h.do("POST", "/api/auth/login", loginBody)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	body := decode[sessionResponse](t, rec)
	if body.AccessToken != "access-token" || body.ExpiresIn != 900 || body.User.ID != userID || body.User.Role != "user" {
		t.Errorf("body = %+v", body)
	}
	if strings.Contains(rec.Body.String(), refreshSecret) {
		t.Fatal("the refresh token is in the JSON body, where a script in the page could read it")
	}

	c := cookieNamed(rec, refreshCookieName)
	if c == nil {
		t.Fatal("no refresh cookie was set")
	}
	if c.Value != refreshSecret {
		t.Errorf("cookie value = %q, want the refresh token", c.Value)
	}
	if !c.HttpOnly {
		t.Error("cookie is readable from JavaScript (HttpOnly is off)")
	}
	if c.SameSite != http.SameSiteStrictMode {
		t.Errorf("SameSite = %v, want Strict", c.SameSite)
	}
	if c.Path != "/api/auth" {
		t.Errorf("Path = %q, want /api/auth so it is not sent with ordinary API calls", c.Path)
	}
	if c.MaxAge != 7*24*3600 {
		t.Errorf("MaxAge = %d, want 7 days", c.MaxAge)
	}
	if c.Secure {
		t.Error("Secure is on although CookieSecure was not enabled")
	}
}

func TestLogin_CookieIsSecureWhenConfigured(t *testing.T) {
	h := newHarness(t, func(d *Deps) { d.CookieSecure = true })
	h.auth.login = loggedIn

	rec := h.do("POST", "/api/auth/login", loginBody)

	if c := cookieNamed(rec, refreshCookieName); c == nil || !c.Secure {
		t.Errorf("cookie = %+v, want Secure when the deployment uses TLS", c)
	}
}

func TestLogin_FailureSetsNoCookie(t *testing.T) {
	h := newHarness(t)
	h.auth.login = func(*authv1.LoginRequest) (*authv1.LoginResponse, error) {
		return nil, status.Error(codes.Unauthenticated, "Invalid email or password")
	}

	rec := h.do("POST", "/api/auth/login", loginBody)

	if rec.Code != http.StatusUnauthorized || errCode(t, rec) != "unauthenticated" {
		t.Errorf("got %d %q, want 401 unauthenticated", rec.Code, errCode(t, rec))
	}
	if cookieNamed(rec, refreshCookieName) != nil {
		t.Error("a failed login still set a cookie")
	}
}

// Someone guessing one account's password from many addresses slips under the
// per-address limit, so guesses are also counted per account.
func TestLogin_LimitsGuessesAgainstOneAccount(t *testing.T) {
	h := newHarness(t)
	h.limiter.deny = func(key string) bool { return strings.HasPrefix(key, "login-email:") }

	rec := h.do("POST", "/api/auth/login", loginBody) // auth-svc must not be reached

	if rec.Code != http.StatusTooManyRequests || errCode(t, rec) != "rate_limited" {
		t.Fatalf("got %d %q, want 429 rate_limited", rec.Code, errCode(t, rec))
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want 2 (1.5s rounded up: never tell a client to return too early)", got)
	}
	if h.limiter.seen("ann@example.com") || h.limiter.seen("Ann@Example.com") {
		t.Error("the email address was used in a Redis key in the clear")
	}
}

func TestLogin_TheSameAccountShareOneCounterWhateverTheCase(t *testing.T) {
	h := newHarness(t)
	h.auth.login = loggedIn

	h.do("POST", "/api/auth/login", `{"email":"Ann@Example.com","password":"x"}`)
	h.do("POST", "/api/auth/login", `{"email":"  ann@example.COM ","password":"x"}`)

	var emailKeys []string
	for _, k := range h.limiter.keys {
		if strings.HasPrefix(k, "login-email:") {
			emailKeys = append(emailKeys, k)
		}
	}
	if len(emailKeys) != 2 || emailKeys[0] != emailKeys[1] {
		t.Errorf("per-account keys = %v, want the same key twice: changing letter case must not reset the limit", emailKeys)
	}
}

func TestRefresh_RequiresTheCSRFHeader(t *testing.T) {
	h := newHarness(t) // Auth.Refresh must not be called

	rec := h.do("POST", "/api/auth/refresh", "", "Cookie", refreshCookieName+"="+refreshSecret)

	if rec.Code != http.StatusForbidden || errCode(t, rec) != "csrf_check_failed" {
		t.Errorf("got %d %q, want 403 csrf_check_failed", rec.Code, errCode(t, rec))
	}
}

func TestRefresh_RejectsAWrongCSRFValue(t *testing.T) {
	h := newHarness(t)

	rec := h.do("POST", "/api/auth/refresh", "", csrfHeader, "anything", "Cookie", refreshCookieName+"="+refreshSecret)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

func TestRefresh_NeedsTheCookie(t *testing.T) {
	h := newHarness(t)

	rec := h.do("POST", "/api/auth/refresh", "", csrf...)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 without a session cookie", rec.Code)
	}
}

func TestRefresh_RotatesTheCookieAndReturnsANewAccessToken(t *testing.T) {
	h := newHarness(t)
	var sent string
	h.auth.refresh = func(in *authv1.RefreshRequest) (*authv1.RefreshResponse, error) {
		sent = in.RefreshToken
		return &authv1.RefreshResponse{AccessToken: "new-access", RefreshToken: "rotated-secret", ExpiresIn: 900, UserId: userID, Email: "ann@example.com", Role: "user"}, nil
	}

	rec := h.do("POST", "/api/auth/refresh", "", append(csrf, "Cookie", refreshCookieName+"="+refreshSecret)...)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if sent != refreshSecret {
		t.Errorf("auth-svc was sent %q, want the token from the cookie", sent)
	}
	if got := decode[sessionResponse](t, rec).AccessToken; got != "new-access" {
		t.Errorf("access token = %q, want the new one", got)
	}
	if c := cookieNamed(rec, refreshCookieName); c == nil || c.Value != "rotated-secret" {
		t.Errorf("cookie = %+v, want it replaced with the rotated token", c)
	}
	if strings.Contains(rec.Body.String(), "rotated-secret") {
		t.Error("the rotated refresh token appears in the body")
	}
}

func TestRefresh_ADeadTokenClearsTheCookie(t *testing.T) {
	h := newHarness(t)
	h.auth.refresh = func(*authv1.RefreshRequest) (*authv1.RefreshResponse, error) {
		return nil, status.Error(codes.Unauthenticated, "Invalid refresh token")
	}

	rec := h.do("POST", "/api/auth/refresh", "", append(csrf, "Cookie", refreshCookieName+"="+refreshSecret)...)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if c := cookieNamed(rec, refreshCookieName); c == nil || c.MaxAge >= 0 {
		t.Errorf("cookie = %+v, want it deleted so the browser stops sending a dead token", c)
	}
}

func TestLogout_RevokesTheTokenAndClearsTheCookie(t *testing.T) {
	h := newHarness(t)
	var revoked string
	h.auth.logout = func(in *authv1.LogoutRequest) (*authv1.LogoutResponse, error) {
		revoked = in.RefreshToken
		return &authv1.LogoutResponse{}, nil
	}

	rec := h.do("POST", "/api/auth/logout", "", append(csrf, "Cookie", refreshCookieName+"="+refreshSecret)...)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if revoked != refreshSecret {
		t.Errorf("revoked %q, want the cookie's token", revoked)
	}
	if c := cookieNamed(rec, refreshCookieName); c == nil || c.MaxAge >= 0 || c.Value != "" {
		t.Errorf("cookie = %+v, want it cleared", c)
	}
}

func TestLogout_StillSignsTheBrowserOutWhenRevocationFails(t *testing.T) {
	h := newHarness(t)
	h.auth.logout = func(*authv1.LogoutRequest) (*authv1.LogoutResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}

	rec := h.do("POST", "/api/auth/logout", "", append(csrf, "Cookie", refreshCookieName+"="+refreshSecret)...)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204: the user must be signed out of this browser regardless", rec.Code)
	}
	if c := cookieNamed(rec, refreshCookieName); c == nil || c.MaxAge >= 0 {
		t.Error("the cookie was not cleared")
	}
	if !strings.Contains(h.logs.String(), "could not revoke the refresh token") {
		t.Error("a failed revocation was not logged; it deserves an alert")
	}
}

func TestLogout_WithoutACookieCallsNothing(t *testing.T) {
	h := newHarness(t) // Auth.Logout must not be called

	rec := h.do("POST", "/api/auth/logout", "", csrf...)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
}

func TestLogout_RequiresTheCSRFHeader(t *testing.T) {
	h := newHarness(t)

	rec := h.do("POST", "/api/auth/logout", "", "Cookie", refreshCookieName+"="+refreshSecret)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403: any site could otherwise sign a user out", rec.Code)
	}
}
