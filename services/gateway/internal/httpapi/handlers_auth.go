package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	refreshCookieName = "refresh_token"
	// The cookie lives as long as the refresh token it carries (auth-svc issues 7 days).
	refreshCookieMaxAge = 7 * 24 * time.Hour
	// The cookie is only sent to the auth endpoints, never with ordinary API calls.
	refreshCookiePath = "/api/auth"
)

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type userView struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

// sessionResponse deliberately has no refresh token. That travels only in an
// httpOnly cookie, out of reach of any script running in the page, so a cross
// site scripting bug cannot steal the long-lived credential.
type sessionResponse struct {
	AccessToken string   `json:"access_token"`
	ExpiresIn   int64    `json:"expires_in"`
	User        userView `json:"user"`
}

func (a *API) register(w http.ResponseWriter, r *http.Request) {
	var body credentials
	if !decodeJSON(w, r, &body) {
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.auth.Register(ctx, &authv1.RegisterRequest{Email: body.Email, Password: body.Password})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"user_id": resp.GetUserId()})
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	var body credentials
	if !decodeJSON(w, r, &body) {
		return
	}
	// On top of the per-address limit, limit guesses against one account. Someone
	// spreading a password-guessing attack across many addresses would slip past
	// the per-address limit but not this one.
	if !a.allow(w, r, "login-email:"+hashKey(body.Email), 5, time.Minute) {
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.auth.Login(ctx, &authv1.LoginRequest{Email: body.Email, Password: body.Password})
	if err != nil {
		a.fail(w, r, err)
		return
	}

	a.setRefreshCookie(w, resp.GetRefreshToken())
	writeJSON(w, http.StatusOK, sessionResponse{
		AccessToken: resp.GetAccessToken(),
		ExpiresIn:   resp.GetExpiresIn(),
		User:        userView{ID: resp.GetUserId(), Email: resp.GetEmail(), Role: resp.GetRole()},
	})
}

func (a *API) refresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(refreshCookieName)
	if err != nil || cookie.Value == "" {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no session, sign in")
		return
	}
	ctx, cancel := a.callContext(r)
	defer cancel()

	resp, err := a.auth.Refresh(ctx, &authv1.RefreshRequest{RefreshToken: cookie.Value})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			// The token is dead (expired, revoked, or reused). Tell the browser to
			// throw the cookie away instead of sending it again on every page load.
			a.clearRefreshCookie(w)
		}
		a.fail(w, r, err)
		return
	}

	// Rotation: the response carries a NEW refresh token, and the old one is now revoked.
	a.setRefreshCookie(w, resp.GetRefreshToken())
	writeJSON(w, http.StatusOK, sessionResponse{
		AccessToken: resp.GetAccessToken(),
		ExpiresIn:   resp.GetExpiresIn(),
		User:        userView{ID: resp.GetUserId(), Email: resp.GetEmail(), Role: resp.GetRole()},
	})
}

func (a *API) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(refreshCookieName); err == nil && cookie.Value != "" {
		ctx, cancel := a.callContext(r)
		defer cancel()
		if _, err := a.auth.Logout(ctx, &authv1.LogoutRequest{RefreshToken: cookie.Value}); err != nil {
			// The user still gets signed out of this browser below; only the
			// server-side revocation failed, which is worth an alert.
			a.log.Error("could not revoke the refresh token on logout",
				slog.String("request_id", requestIDFrom(r.Context())), slog.Any("error", err))
		}
	}
	a.clearRefreshCookie(w)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) setRefreshCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     refreshCookieName,
		Value:    token,
		Path:     refreshCookiePath,
		MaxAge:   int(refreshCookieMaxAge.Seconds()),
		HttpOnly: true,                    // invisible to page scripts
		Secure:   a.cookieSecure,          // HTTPS only wherever TLS is in use
		SameSite: http.SameSiteStrictMode, // never sent on requests started by another site
	})
}

func (a *API) clearRefreshCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     refreshCookieName,
		Value:    "",
		Path:     refreshCookiePath,
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   a.cookieSecure,
		SameSite: http.SameSiteStrictMode,
	})
}
