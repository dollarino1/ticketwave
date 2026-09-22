package config

type Config struct {
	DatabaseURL   string   `env:"DATABASE_URL,required"`
	GRPCPort      string   `env:"GRPC_PORT" envDefault:":50056"`
	KafkaBrokers  []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	ConsumerGroup string   `env:"CONSUMER_GROUP" envDefault:"analytics"`
	// Concurrency is how many messages of a batch are handled in parallel. The
	// updates are additive, so any order is correct; 1 keeps the load on the
	// database gentle.
	Concurrency int `env:"CONCURRENCY" envDefault:"1"`

	// Where Prometheus scrapes this service. A separate port from the API, never
	// published to the outside. Set to "off" to disable.
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9106"`

	// Where spans go via OTLP/gRPC (Tempo). "off" (the default) disables tracing.
	TracingEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"off"`
}
