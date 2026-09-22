// Package observability holds what every service needs to be operable: structured
// logs, metrics, and the gRPC interceptors that produce them.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// SetupLogging makes JSON on stdout the process's logger and returns it.
//
// JSON because logs are read by machines first (Loki, CloudWatch, Datadog all index
// fields, and cannot do that to free text). Every line carries the service name, so
// one search across all services can still tell them apart.
//
// It also becomes slog's default, and slog.SetDefault redirects the standard `log`
// package into it. That means every existing log.Printf in the codebase is now a
// well-formed JSON line too, and can be turned into a structured one gradually.
func SetupLogging(service string) *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: levelFromEnv()})).
		With(slog.String("service", service))
	slog.SetDefault(logger)
	return logger
}

// levelFromEnv reads LOG_LEVEL (debug, info, warn, error). Anything else, or nothing,
// means info: an unrecognised value should not silence a service.
func levelFromEnv() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
