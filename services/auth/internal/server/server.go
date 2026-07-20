package server

import (
	"context"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
)

type Server struct {
	authv1.UnimplementedAuthServiceServer
}

func New() *Server {
	return &Server{}
}

func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	return &authv1.RegisterResponse{UserId: "USER TEST"}, nil
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
