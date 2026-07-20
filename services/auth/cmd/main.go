package main

import (
	"context"
	"log"
	"net"

	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	"github.com/dollarino1/ticketwave/services/auth/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	authv1.RegisterAuthServiceServer(grpcServer, server.New())
	reflection.Register(grpcServer)
	log.Printf("server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
