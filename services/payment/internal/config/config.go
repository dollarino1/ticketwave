package config

type Config struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
	GRPCPort    string `env:"GRPC_PORT" envDefault:":50053"`
}
