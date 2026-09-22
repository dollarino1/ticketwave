package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/dollarino1/ticketwave/pkg/redis"
	appconfig "github.com/dollarino1/ticketwave/services/notification/internal/config"
	"github.com/dollarino1/ticketwave/services/notification/internal/dedup"
	"github.com/dollarino1/ticketwave/services/notification/internal/email"
	"github.com/dollarino1/ticketwave/services/notification/internal/handler"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("notification-svc: %v", err)
	}
}

// run holds the whole lifecycle so its deferred cleanups always execute. A
// log.Fatal inside main would skip them.
func run() error {
	cfg, err := config.Load[appconfig.Config](".env.notification")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ctx is cancelled by Ctrl+C or SIGTERM, which stops the consumer cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := observability.SetupLogging("notification")
	observability.StartMetrics(ctx, cfg.MetricsAddr, logger)

	shutdownTracing, err := observability.SetupTracing(ctx, "notification", cfg.TracingEndpoint)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	redisClient, err := redis.New(ctx, cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer func() { _ = redisClient.Close() }()

	dlq := kafka.NewProducer(cfg.KafkaBrokers, events.TopicOrderEventsDLQ)
	defer func() { _ = dlq.Close() }()

	h := handler.New(email.LogSender{}, dedup.NewRedis(redisClient))
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       events.TopicOrderEvents,
		GroupID:     cfg.ConsumerGroup,
		DLQ:         dlq,
		Concurrency: cfg.Concurrency,
	}, h.Handle)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	log.Printf("notification-svc consuming %s as group %q (concurrency %d)",
		events.TopicOrderEvents, cfg.ConsumerGroup, cfg.Concurrency)
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	log.Println("shutdown complete")
	return nil
}
