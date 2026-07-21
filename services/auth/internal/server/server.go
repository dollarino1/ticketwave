package server

import (
	"context"
	"errors"
	"fmt"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	authv1.UnimplementedAuthServiceServer
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Server {
	return &Server{pool: pool}
}

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
	return &authv1.LoginResponse{
		AccessToken:  "ACc test",
		RefreshToken: "ref test",
		ExpiresIn:    0,
	}, nil
}

func (s *Server) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.RefreshResponse, error) {
	return &authv1.RefreshResponse{
		AccessToken:  "ACc test",
		RefreshToken: "ref test",
		ExpiresIn:    0,
	}, nil
}
