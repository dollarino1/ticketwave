package config

type Config struct {
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	RedisAddr    string   `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	// ConsumerGroup is shared by every replica of this service. Kafka splits the
	// topic's partitions between the members of one group, so starting more
	// replicas spreads the work and stopping one hands its partitions to the rest.
	ConsumerGroup string `env:"CONSUMER_GROUP" envDefault:"notification"`
	// Concurrency is how many messages of one batch are handled in parallel.
	Concurrency int `env:"CONCURRENCY" envDefault:"4"`

	// Where Prometheus scrapes this service. A separate port from the API, never
	// published to the outside. Set to "off" to disable.
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9105"`

	// Where spans go via OTLP/gRPC (Tempo). "off" (the default) disables tracing.
	TracingEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"off"`
}
