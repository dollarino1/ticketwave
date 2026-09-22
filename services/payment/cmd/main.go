package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	paymentv1 "github.com/dollarino1/ticketwave/gen/ticketwave/payment/v1"
	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/dollarino1/ticketwave/pkg/postgres"
	appconfig "github.com/dollarino1/ticketwave/services/payment/internal/config"
	"github.com/dollarino1/ticketwave/services/payment/internal/server"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("payment-svc: %v", err)
	}
}

// run holds the whole lifecycle so its deferred cleanups always execute.
func run() error {
	cfg, err := config.Load[appconfig.Config](".env.payment")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ctx is cancelled by Ctrl+C or SIGTERM and triggers the graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := observability.SetupLogging("payment")
	observability.StartMetrics(ctx, cfg.MetricsAddr, logger)

	shutdownTracing, err := observability.SetupTracing(ctx, "payment", cfg.TracingEndpoint)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	pool, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	grpcServer := observability.NewGRPCServer(logger)
	paymentv1.RegisterPaymentServiceServer(grpcServer, server.New(pool))
	reflection.Register(grpcServer)
	observability.InitGRPCMetrics(grpcServer)

	go func() {
		<-ctx.Done()
		log.Println("shutting down: finishing in-flight requests")
		grpcServer.GracefulStop()
	}()

	log.Printf("server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	log.Println("shutdown complete")
	return nil
}
