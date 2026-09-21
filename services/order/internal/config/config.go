package config

type Config struct {
	DatabaseURL   string   `env:"DATABASE_URL,required"`
	GRPCPort      string   `env:"GRPC_PORT" envDefault:":50054"`
	InventoryAddr string   `env:"INVENTORY_ADDR,required"`
	PaymentAddr   string   `env:"PAYMENT_ADDR,required"`
	KafkaBrokers  []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
}
