package config

type Config struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
	GRPCPort    string `env:"GRPC_PORT" envDefault:":50053"`

	// Where Prometheus scrapes this service. A separate port from the API, never
	// published to the outside. Set to "off" to disable.
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9103"`

	// Where spans go via OTLP/gRPC (Tempo). "off" (the default) disables tracing.
	TracingEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"off"`
}
