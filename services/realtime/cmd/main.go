package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/pkg/observability"
	appconfig "github.com/dollarino1/ticketwave/services/realtime/internal/config"
	"github.com/dollarino1/ticketwave/services/realtime/internal/feed"
	"github.com/dollarino1/ticketwave/services/realtime/internal/hub"
	"github.com/dollarino1/ticketwave/services/realtime/internal/stream"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		slog.Error("realtime-svc failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load[appconfig.Config](".env.realtime")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := observability.SetupLogging("realtime")
	observability.StartMetrics(ctx, cfg.MetricsAddr, log)

	shutdownTracing, err := observability.SetupTracing(ctx, "realtime", cfg.TracingEndpoint)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	seats := hub.New(cfg.MaxSubscribers)

	mux := http.NewServeMux()
	mux.Handle("GET /api/events/{id}/stream", stream.New(seats, log, 0))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: streams are open for as long as the page is. The stream
		// handler manages its own write deadlines. IdleTimeout applies between
		// requests on a kept-alive connection, never during a stream.
		IdleTimeout: 60 * time.Second,
		BaseContext: func(_ net.Listener) context.Context { return ctx },
	}
	// An HTTP server's Shutdown waits for open requests, and a stream never ends on
	// its own, so closing the hub is what lets them finish.
	srv.RegisterOnShutdown(seats.Close)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("following seat events", slog.String("topic", events.TopicSeatEvents))
		return kafka.Tail(gctx, kafka.TailConfig{Brokers: cfg.KafkaBrokers, Topic: events.TopicSeatEvents},
			feed.Handler(seats, log))
	})
	g.Go(func() error {
		log.Info("realtime-svc listening", slog.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
