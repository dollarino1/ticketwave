package config

import "time"

type Config struct {
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8081"`

	AuthAddr      string `env:"AUTH_ADDR" envDefault:"localhost:50051"`
	InventoryAddr string `env:"INVENTORY_ADDR" envDefault:"localhost:50052"`
	OrderAddr     string `env:"ORDER_ADDR" envDefault:"localhost:50054"`
	AnalyticsAddr string `env:"ANALYTICS_ADDR" envDefault:"localhost:50056"`
	RedisAddr     string `env:"REDIS_ADDR" envDefault:"localhost:6379"`

	// JWTPublicKeyPath is the key access tokens are verified with. The gateway
	// only ever holds the public half, so it can check tokens but never mint one.
	JWTPublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH" envDefault:"certs/public.pem"`

	// SeatPriceCents is what one seat costs. The gateway computes an order's
	// amount itself; letting the client send the amount would let anyone buy
	// tickets for a cent. (Orders of exactly 7 seats decline at 35000 cents, since
	// payment-svc declines multiples of 7: that is the demo hook for the saga's
	// compensation path.)
	SeatPriceCents int64 `env:"SEAT_PRICE_CENTS" envDefault:"5000"`

	// CookieSecure marks the refresh cookie HTTPS-only. Turn it on in any
	// environment that serves over TLS; it stays off for plain-HTTP local dev,
	// where browsers would otherwise refuse to store the cookie at all.
	CookieSecure bool `env:"COOKIE_SECURE" envDefault:"false"`

	// TrustProxyHeaders makes the gateway believe X-Real-IP for the client's
	// address. Enable it only behind a proxy (nginx) that overwrites that header;
	// otherwise a client could send any value and dodge its rate limits.
	TrustProxyHeaders bool `env:"TRUST_PROXY_HEADERS" envDefault:"false"`

	// RequestTimeout bounds the downstream calls made for one request.
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"8s"`

	// Where Prometheus scrapes this service. A separate port from the API, never
	// published to the outside. Set to "off" to disable.
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9107"`

	// Where spans go via OTLP/gRPC (Tempo). "off" (the default) disables tracing.
	TracingEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"off"`
}
