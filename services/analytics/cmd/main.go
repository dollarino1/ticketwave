package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/dollarino1/ticketwave/pkg/postgres"
	appconfig "github.com/dollarino1/ticketwave/services/analytics/internal/config"
	"github.com/dollarino1/ticketwave/services/analytics/internal/projection"
	"github.com/dollarino1/ticketwave/services/analytics/internal/server"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("analytics-svc: %v", err)
	}
}

// run holds the whole lifecycle so its deferred cleanups always execute.
func run() error {
	cfg, err := config.Load[appconfig.Config](".env.analytics")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ctx is cancelled by Ctrl+C or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := observability.SetupLogging("analytics")
	observability.StartMetrics(ctx, cfg.MetricsAddr, logger)

	shutdownTracing, err := observability.SetupTracing(ctx, "analytics", cfg.TracingEndpoint)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	pool, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	dlq := kafka.NewProducer(cfg.KafkaBrokers, events.TopicOrderEventsDLQ)
	defer func() { _ = dlq.Close() }()

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       events.TopicOrderEvents,
		GroupID:     cfg.ConsumerGroup,
		DLQ:         dlq,
		Concurrency: cfg.Concurrency,
	}, projection.New(pool).Handle)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	grpcServer := observability.NewGRPCServer(logger)
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, server.New(pool))
	reflection.Register(grpcServer)
	observability.InitGRPCMetrics(grpcServer)

	// The consumer (write side) and the gRPC server (read side) live and die
	// together: if either fails, the group's context is cancelled and the other
	// one stops too.
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Printf("analytics-svc projecting %s as group %q", events.TopicOrderEvents, cfg.ConsumerGroup)
		if err := consumer.Run(ctx); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
		}()
		log.Printf("analytics-svc gRPC listening at %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	log.Println("shutdown complete")
	return nil
}
