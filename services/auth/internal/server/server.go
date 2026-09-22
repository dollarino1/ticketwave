package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/dollarino1/ticketwave/pkg/authz"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	accessTokenTTL  = 15 * time.Minute
	refreshTokenTTL = 7 * 24 * time.Hour
)

// One error value for "unknown email" and "wrong password", so a caller cannot
// tell the two apart and use login to discover which emails are registered.
var (
	errBadCredentials = status.Error(codes.Unauthenticated, "Invalid email or password")
	errInvalidRefresh = status.Error(codes.Unauthenticated, "Invalid refresh token")
	errExpiredRefresh = status.Error(codes.Unauthenticated, "Refresh token expired")
)

// dummyHash is compared against when a login names an unknown email, so that
// path costs the same CPU as a real password check. Without it, "no such user"
// answers in about a millisecond and "wrong password" in about fifty, and that
// difference leaks which emails have accounts. Built once, on first use.
var dummyHash = sync.OnceValue(func() []byte {
	h, err := bcrypt.GenerateFromPassword([]byte("timing-equalizer"), bcrypt.DefaultCost)
	if err != nil {
		panic(fmt.Sprintf("generate the dummy bcrypt hash: %v", err)) // cannot happen for a short fixed input
	}
	return h
})

type Server struct {
	authv1.UnimplementedAuthServiceServer
	pool       *pgxpool.Pool
	signer     *jwt.Signer
	organizers map[string]struct{}
}

// New builds the service. organizerEmails are the addresses that get the
// organizer role on registration.
func New(pool *pgxpool.Pool, signer *jwt.Signer, organizerEmails []string) *Server {
	organizers := make(map[string]struct{}, len(organizerEmails))
	for _, e := range organizerEmails {
		if e = strings.ToLower(strings.TrimSpace(e)); e != "" {
			organizers[e] = struct{}{}
		}
	}
	return &Server{pool: pool, signer: signer, organizers: organizers}
}

func (s *Server) roleFor(email string) authz.Role {
	if _, ok := s.organizers[email]; ok {
		return authz.RoleOrganizer
	}
	return authz.RoleUser
}

type user struct {
	id           uuid.UUID
	email        string
	passwordHash string
	role         authz.Role
}

func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	email, err := normalizeEmail(req.GetEmail())
	if err != nil {
		return nil, err
	}
	if err := validatePassword(req.GetPassword()); err != nil {
		return nil, err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.GetPassword()), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	id := uuid.New()
	_, err = s.pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, role) VALUES ($1, $2, $3, $4)`,
		id, email, string(hash), string(s.roleFor(email)),
	)
	if err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
			return nil, status.Error(codes.AlreadyExists, "email already registered")
		}
		return nil, fmt.Errorf("insert user: %w", err)
	}

	return &authv1.RegisterResponse{UserId: id.String()}, nil
}

func (s *Server) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	u, found, err := s.findUser(ctx, req.GetEmail())
	if err != nil {
		return nil, err
	}
	if !found {
		_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(req.GetPassword()))
		return nil, errBadCredentials
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.passwordHash), []byte(req.GetPassword())); err != nil {
		return nil, errBadCredentials
	}

	accessToken, refreshToken, err := s.issueTokens(ctx, s.pool, u)
	if err != nil {
		return nil, err
	}
	return &authv1.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(accessTokenTTL.Seconds()),
		UserId:       u.id.String(),
		Email:        u.email,
		Role:         string(u.role),
	}, nil
}

// Refresh trades a valid refresh token for a new access token and a NEW refresh
// token, revoking the one presented (rotation). Presenting a token that was
// already used is treated as theft: every session of that user is revoked.
func (s *Server) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.RefreshResponse, error) {
	tokenHash := hashToken(req.GetRefreshToken())

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// FOR UPDATE makes two simultaneous refreshes of one token take turns. The
	// second then sees it already revoked and trips reuse detection. Without the
	// lock both would read "not revoked" and both would be issued a live session.
	var tokenID, userID uuid.UUID
	var revoked bool
	var expiresAt time.Time
	err = tx.QueryRow(ctx,
		`SELECT id, user_id, revoked, expires_at FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE`,
		tokenHash).Scan(&tokenID, &userID, &revoked, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errInvalidRefresh
	}
	if err != nil {
		return nil, fmt.Errorf("look up refresh token: %w", err)
	}

	if revoked {
		if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked = true WHERE user_id = $1`, userID); err != nil {
			return nil, fmt.Errorf("revoke all tokens: %w", err)
		}
		// Commit before failing: the revocation must survive this request's error.
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit revocation: %w", err)
		}
		return nil, errInvalidRefresh
	}
	if time.Now().After(expiresAt) {
		return nil, errExpiredRefresh
	}

	if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked = true WHERE id = $1`, tokenID); err != nil {
		return nil, fmt.Errorf("revoke token: %w", err)
	}

	// Read the user fresh, so a role change takes effect at the next refresh.
	u := user{id: userID}
	var role string
	err = tx.QueryRow(ctx, `SELECT email, role FROM users WHERE id = $1`, userID).Scan(&u.email, &role)
	if err != nil {
		return nil, fmt.Errorf("look up user: %w", err)
	}
	u.role = authz.Role(role)

	accessToken, refreshToken, err := s.issueTokens(ctx, tx, u)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return &authv1.RefreshResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(accessTokenTTL.Seconds()),
		UserId:       u.id.String(),
		Email:        u.email,
		Role:         string(u.role),
	}, nil
}

// Logout revokes the presented refresh token. It always succeeds and reveals
// nothing about whether the token existed, so it cannot be used to probe tokens.
func (s *Server) Logout(ctx context.Context, req *authv1.LogoutRequest) (*authv1.LogoutResponse, error) {
	_, err := s.pool.Exec(ctx,
		`UPDATE refresh_tokens SET revoked = true WHERE token_hash = $1`,
		hashToken(req.GetRefreshToken()))
	if err != nil {
		return nil, fmt.Errorf("revoke refresh token: %w", err)
	}
	return &authv1.LogoutResponse{}, nil
}

// findUser looks a user up by email. A malformed email cannot belong to any
// account, so it is reported as simply not found.
func (s *Server) findUser(ctx context.Context, rawEmail string) (user, bool, error) {
	email, err := normalizeEmail(rawEmail)
	if err != nil {
		return user{}, false, nil
	}

	var u user
	var role string
	err = s.pool.QueryRow(ctx,
		`SELECT id, email, password_hash, role FROM users WHERE email = $1`, email,
	).Scan(&u.id, &u.email, &u.passwordHash, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return user{}, false, nil
	}
	if err != nil {
		return user{}, false, fmt.Errorf("look up user: %w", err)
	}
	u.role = authz.Role(role)
	return u, true, nil
}

// execer is what both a connection pool and a transaction can do, so tokens can
// be issued either on their own (login) or inside a larger transaction (refresh).
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// issueTokens signs an access token and creates a refresh token, storing only
// the refresh token's hash: a leaked table then cannot be replayed as sessions.
func (s *Server) issueTokens(ctx context.Context, db execer, u user) (accessToken, refreshToken string, err error) {
	accessToken, err = s.signer.Sign(u.id.String(), u.email, string(u.role), accessTokenTTL)
	if err != nil {
		return "", "", fmt.Errorf("sign access token: %w", err)
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("generate refresh token: %w", err)
	}
	refreshToken = base64.RawURLEncoding.EncodeToString(raw)

	_, err = db.Exec(ctx,
		`INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) VALUES ($1, $2, $3, $4)`,
		uuid.New(), u.id, hashToken(refreshToken), time.Now().Add(refreshTokenTTL),
	)
	if err != nil {
		return "", "", fmt.Errorf("insert refresh token: %w", err)
	}
	return accessToken, refreshToken, nil
}

// hashToken is SHA-256, not bcrypt: a refresh token is 256 random bits, so there
// is nothing to brute-force and a slow hash would only add latency.
func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
