package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/dollarino1/ticketwave/pkg/outbox"
	"github.com/dollarino1/ticketwave/pkg/postgres"
	"github.com/dollarino1/ticketwave/pkg/redis"
	appconfig "github.com/dollarino1/ticketwave/services/inventory/internal/config"
	"github.com/dollarino1/ticketwave/services/inventory/internal/server"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg, err := config.Load[appconfig.Config](".env.inventory")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// ctx is cancelled by Ctrl+C or SIGTERM and drives the graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := observability.SetupLogging("inventory")
	observability.StartMetrics(ctx, cfg.MetricsAddr, logger)

	shutdownTracing, err := observability.SetupTracing(ctx, "inventory", cfg.TracingEndpoint)
	if err != nil {
		log.Fatalf("failed to set up tracing: %v", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	pool, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	log.Println("Connected to database")

	redisClient, err := redis.New(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer func() { _ = redisClient.Close() }()
	log.Println("Connected to redis")

	producer := kafka.NewProducer(cfg.KafkaBrokers, events.TopicSeatEvents)
	defer func() { _ = producer.Close() }()

	outboxTable := outbox.Table{Name: "outbox", KeyColumn: "event_id"}
	// Exposes how far behind the outbox is; alert on its oldest-row age.
	if err := outbox.RegisterMetrics(pool, outboxTable); err != nil {
		logger.Error("outbox metrics not registered", slog.Any("error", err))
	}

	var background sync.WaitGroup
	background.Go(func() { outbox.NewPublisher(pool, producer, outboxTable).Run(ctx) })

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	inventorySrv := server.New(pool, redisClient)
	inventorySrv.StartHoldSweeper(ctx)

	grpcServer := observability.NewGRPCServer(logger)
	inventoryv1.RegisterInventoryServiceServer(grpcServer, inventorySrv)
	reflection.Register(grpcServer)
	observability.InitGRPCMetrics(grpcServer)

	go func() {
		<-ctx.Done()
		log.Println("shutting down: finishing in-flight requests")
		grpcServer.GracefulStop()
	}()

	log.Printf("server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	background.Wait() // the publisher exits once ctx is cancelled
	log.Println("shutdown complete")
}
