package config

type Config struct {
	DatabaseURL   string   `env:"DATABASE_URL,required"`
	GRPCPort      string   `env:"GRPC_PORT" envDefault:":50054"`
	InventoryAddr string   `env:"INVENTORY_ADDR,required"`
	PaymentAddr   string   `env:"PAYMENT_ADDR,required"`
	KafkaBrokers  []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`

	// Where Prometheus scrapes this service. A separate port from the API, never
	// published to the outside. Set to "off" to disable.
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9104"`

	// Where spans go via OTLP/gRPC (Tempo). "off" (the default) disables tracing.
	TracingEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"off"`
}
