package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServeMetrics serves Prometheus metrics on addr until ctx is cancelled.
//
// It is a SEPARATE listener from the service's own API on purpose. Metrics describe
// internals (route names, error rates, queue depths), which are useful to an
// attacker and to nobody outside the cluster. The public port never routes to this
// one, and in production network policy limits it to Prometheus.
//
// Everything registered on the default Prometheus registry is served. That includes
// the Go runtime (goroutines, GC, heap) and process (CPU, file descriptors)
// collectors, which client_golang registers by itself.
func ServeMetrics(ctx context.Context, addr string, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() {
		log.Info("metrics listening", slog.String("addr", addr))
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("metrics server: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// Disabled is the METRICS_ADDR value that turns the metrics listener off. It has to
// be a word: the config library treats a set-but-empty variable as unset and applies
// the default, so an empty string could never mean "off".
const Disabled = "off"

// StartMetrics runs ServeMetrics in the background for the life of the process. A
// service that cannot expose metrics is still a working service, so a failure is
// logged loudly rather than taking the service down.
func StartMetrics(ctx context.Context, addr string, log *slog.Logger) {
	if addr == "" || addr == Disabled {
		return
	}
	go func() {
		if err := ServeMetrics(ctx, addr, log); err != nil {
			log.Error("metrics server stopped", slog.Any("error", err))
		}
	}()
}
