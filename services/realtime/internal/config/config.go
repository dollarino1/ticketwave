package config

type Config struct {
	HTTPAddr       string   `env:"HTTP_ADDR" envDefault:":8082"`
	KafkaBrokers   []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	MaxSubscribers int      `env:"MAX_SUBSCRIBERS" envDefault:"10000"`

	// Where Prometheus scrapes this service. A separate port from the API, never
	// published to the outside. Set to "off" to disable.
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9108"`

	// Where spans go via OTLP/gRPC (Tempo). "off" (the default) disables tracing.
	TracingEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"off"`
}
