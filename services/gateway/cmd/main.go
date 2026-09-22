package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	analyticsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/analytics/v1"
	authv1 "github.com/dollarino1/ticketwave/gen/ticketwave/auth/v1"
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	orderv1 "github.com/dollarino1/ticketwave/gen/ticketwave/order/v1"
	"github.com/dollarino1/ticketwave/pkg/config"
	"github.com/dollarino1/ticketwave/pkg/jwt"
	"github.com/dollarino1/ticketwave/pkg/observability"
	"github.com/dollarino1/ticketwave/pkg/ratelimit"
	"github.com/dollarino1/ticketwave/pkg/redis"
	appconfig "github.com/dollarino1/ticketwave/services/gateway/internal/config"
	"github.com/dollarino1/ticketwave/services/gateway/internal/httpapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}

// run holds the whole lifecycle so its deferred cleanups always execute.
func run() error {
	cfg, err := config.Load[appconfig.Config](".env.gateway")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ctx is cancelled by Ctrl+C or SIGTERM and starts the graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// JSON logs, one object per line, and a metrics listener on its own port.
	logger := observability.SetupLogging("gateway")
	observability.StartMetrics(ctx, cfg.MetricsAddr, logger)

	shutdownTracing, err := observability.SetupTracing(ctx, "gateway", cfg.TracingEndpoint)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	publicKey, err := jwt.LoadPublicKey(cfg.JWTPublicKeyPath)
	if err != nil {
		return fmt.Errorf("load public key: %w", err)
	}

	redisClient, err := redis.New(ctx, cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer func() { _ = redisClient.Close() }()

	// grpc.NewClient does not connect yet; each connection is opened on first use
	// and re-established on its own, so the gateway can start before the services.
	authConn, err := dial(cfg.AuthAddr)
	if err != nil {
		return err
	}
	defer func() { _ = authConn.Close() }()
	inventoryConn, err := dial(cfg.InventoryAddr)
	if err != nil {
		return err
	}
	defer func() { _ = inventoryConn.Close() }()
	orderConn, err := dial(cfg.OrderAddr)
	if err != nil {
		return err
	}
	defer func() { _ = orderConn.Close() }()
	analyticsConn, err := dial(cfg.AnalyticsAddr)
	if err != nil {
		return err
	}
	defer func() { _ = analyticsConn.Close() }()

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: httpapi.New(httpapi.Deps{
			Auth:              authv1.NewAuthServiceClient(authConn),
			Inventory:         inventoryv1.NewInventoryServiceClient(inventoryConn),
			Orders:            orderv1.NewOrderServiceClient(orderConn),
			Analytics:         analyticsv1.NewAnalyticsServiceClient(analyticsConn),
			Limiter:           ratelimit.New(redisClient),
			Verifier:          jwt.NewVerifier(publicKey),
			SeatPriceCents:    cfg.SeatPriceCents,
			CookieSecure:      cfg.CookieSecure,
			TrustProxyHeaders: cfg.TrustProxyHeaders,
			RequestTimeout:    cfg.RequestTimeout,
			Logger:            logger,
		}),
		// Timeouts stop a slow or malicious client from holding connections open forever.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("gateway listening", slog.String("addr", cfg.HTTPAddr))
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
	case <-ctx.Done():
		logger.Info("shutting down: finishing in-flight requests")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
	}
	logger.Info("shutdown complete")
	return nil
}

func dial(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Forward the request ID so the services log the same one the gateway did.
		grpc.WithChainUnaryInterceptor(observability.PropagateRequestID()),
		// Continue the trace this request started with (or opened here) into the
		// service being called, and record a client span for the call.
		observability.ClientTracing(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return conn, nil
}
