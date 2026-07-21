package main

import (
	"context"
	"log"
	"net"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/dollarino1/ticketwave/pkg/postgres"
	appconfig "github.com/dollarino1/ticketwave/services/auth/internal/config"
	"github.com/dollarino1/ticketwave/services/auth/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg, err := config.Load[appconfig.Config]()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	ctx := context.Background()

	pool, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	log.Println("Connected to database")

	privKey, err := jwt.LoadPrivateKey("certs/private.pem")
	if err != nil {
		log.Fatalf("failed to load private key: %v", err)
	}
	signer := jwt.NewSigner(privKey)

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	authv1.RegisterAuthServiceServer(grpcServer, server.New(pool, signer))
	reflection.Register(grpcServer)
	log.Printf("server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
