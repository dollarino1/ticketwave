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
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	paymentv1 "github.com/dollarino1/ticketwave/gen/ticketwave/payment/v1"
	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/dollarino1/ticketwave/pkg/outbox"
	"github.com/dollarino1/ticketwave/pkg/postgres"
	appconfig "github.com/dollarino1/ticketwave/services/order/internal/config"
	"github.com/dollarino1/ticketwave/services/order/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg, err := config.Load[appconfig.Config](".env.order")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// ctx is cancelled by Ctrl+C or SIGTERM and drives the graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := observability.SetupLogging("order")
	observability.StartMetrics(ctx, cfg.MetricsAddr, logger)

	shutdownTracing, err := observability.SetupTracing(ctx, "order", cfg.TracingEndpoint)
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

	inventoryConn, err := grpc.NewClient(cfg.InventoryAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.PropagateRequestID()),
		observability.ClientTracing())
	if err != nil {
		log.Fatalf("failed to dial inventory-svc: %v", err)
	}
	defer func() { _ = inventoryConn.Close() }()
	inventoryClient := inventoryv1.NewInventoryServiceClient(inventoryConn)

	paymentConn, err := grpc.NewClient(cfg.PaymentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.PropagateRequestID()),
		observability.ClientTracing())
	if err != nil {
		log.Fatalf("failed to dial payment-svc: %v", err)
	}
	defer func() { _ = paymentConn.Close() }()
	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)

	producer := kafka.NewProducer(cfg.KafkaBrokers, events.TopicOrderEvents)
	defer func() { _ = producer.Close() }()

	outboxTable := outbox.Table{Name: "outbox", KeyColumn: "order_id"}
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

	grpcServer := observability.NewGRPCServer(logger)
	orderv1.RegisterOrderServiceServer(grpcServer, server.New(pool, inventoryClient, paymentClient))
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
