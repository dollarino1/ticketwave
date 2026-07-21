package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	authv1.UnimplementedAuthServiceServer
	pool   *pgxpool.Pool
	signer *jwt.Signer
}

func New(pool *pgxpool.Pool, signer *jwt.Signer) *Server {
	return &Server{pool: pool, signer: signer}
}

const (
	accessTokenTTL  = 15 * time.Minute
	refreshTokenTTL = 7 * 24 * time.Hour
)

func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("bcrypt.GenerateFromPassword: %w", err)
	}

	id := uuid.New()
	_, err = s.pool.Exec(ctx, `INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)`, id, req.Email, hash)
	if err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
			return nil, status.Error(codes.AlreadyExists, "email already registered")
		}
		return nil, fmt.Errorf("insert user: %w", err)
	}

	return &authv1.RegisterResponse{UserId: id.String()}, nil
}

func (s *Server) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	var id uuid.UUID
	var passwordHash string
	err := s.pool.QueryRow(ctx, `SELECT id, password_hash FROM users WHERE email = $1`, req.Email).Scan(&id, &passwordHash)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "Invalid email or password")
	}

	err = bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "Invalid email or password")
	}

	accessToken, err := s.signer.Sign(id.String(), req.Email, accessTokenTTL)
	if err != nil {
		return nil, fmt.Errorf("sign access token: %w", err)
	}

	rawBytes := make([]byte, 32)
	if _, err := rand.Read(rawBytes); err != nil {
		return nil, fmt.Errorf("generate refresh token: %w", err)
	}
	rawToken := base64.RawURLEncoding.EncodeToString(rawBytes)
	hash := sha256.Sum256([]byte(rawToken))
	tokenHash := hex.EncodeToString(hash[:])
	refreshID := uuid.New()
	_, err = s.pool.Exec(ctx,
		`INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) VALUES ($1, $2, $3, $4)`,
		refreshID, id, tokenHash, time.Now().Add(refreshTokenTTL),
	)
	if err != nil {
		return nil, fmt.Errorf("insert refresh token: %w", err)
	}

	return &authv1.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: rawToken,
		ExpiresIn:    int64(accessTokenTTL.Seconds()),
	}, nil
}

func (s *Server) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.RefreshResponse, error) {
	return &authv1.RefreshResponse{
		AccessToken:  "ACc test",
		RefreshToken: "ref test",
		ExpiresIn:    0,
	}, nil
}
