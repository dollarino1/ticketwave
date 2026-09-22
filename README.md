# TicketWave

A concert-ticket platform built as Go microservices. It exists to learn how real
backends are built, so the business rules are deliberately thin and the engineering
around them is not: transactions, sagas, an outbox, Kafka, auth, rate limiting, a live
seat map, containers and CI.

Three rules force all the interesting parts:

- **A seat sells once.** Row locking in Postgres, tested with 50 concurrent buyers.
- **A seat is held for 5 minutes.** Redis TTL plus a sweeper that reconciles Postgres.
- **Payment can fail after the hold.** A saga releases the seats (compensation).

## Architecture

```mermaid
flowchart LR
  B[Browser] -->|HTTPS| N[nginx]
  N -->|/api| G[gateway]
  N -->|/api/events/id/stream, SSE| RT[realtime]
  G -->|gRPC| A[auth]
  G -->|gRPC| I[inventory]
  G -->|gRPC| O[order]
  G -->|gRPC| AN[analytics]
  O -->|gRPC| I
  O -->|gRPC| P[payment]
  O -.outbox.-> K[(Kafka)]
  I -.outbox.-> K
  K -->|order-events| NO[notification]
  K -->|order-events| AN
  K -->|seat-events| RT
```

| Service | Job | Port |
|---|---|---|
| **gateway** | REST to gRPC, JWT check, roles, rate limits, error mapping | 8081 |
| **auth** | Accounts, bcrypt, RS256 access tokens, rotating refresh tokens | 50051 |
| **inventory** | Events and seats, `SELECT FOR UPDATE`, Redis holds, seat outbox | 50052 |
| **payment** | Fake charge (declines amounts divisible by 7) | 50053 |
| **order** | Saga orchestrator with compensation, order outbox | 50054 |
| **analytics** | Kafka to a read model (CQRS), exactly-once effect | 50056 |
| **notification** | Kafka consumer, deduplicated, dead-letter topic | n/a |
| **realtime** | Kafka tail to Server-Sent Events for the live seat map | 8082 |
| **web** | React + TypeScript, served by nginx | 8080 |

**Two ways services talk.** gRPC for questions that need an answer now (reserve these
seats, charge this card). Kafka for facts other services react to later (order
confirmed, seat sold). Kafka messages are written to an outbox table in the same
transaction as the change, so a change and its event can never disagree.

## Run it

Requirements: Docker, Go (see `go.mod`), Node 24.

```bash
go run ./scripts/genkeys            # once: creates certs/private.pem and certs/public.pem
docker compose --profile apps up -d --build
```

Open <http://localhost:8080>. Sign in as an organizer with the address listed in
`ORGANIZER_EMAILS` (`organizer@ticketwave.test` in `docker-compose.yml`) to create
events and see sales. Register that address first.

Try the demo failure: buy **7 seats**. That costs 35000 cents, the fake payment service
declines it, and the seats reappear as available.

Kafka UI is at <http://localhost:8088>. Stop everything with
`docker compose --profile apps rm -sf migrate auth inventory payment order notification analytics gateway realtime web`
(this leaves Postgres, Redis and Kafka running; `docker compose down` removes them too).

## Observe it

```bash
docker compose --profile apps --profile observability up -d --build
```

- **Grafana** <http://localhost:3000>: the *TicketWave overview* dashboard, and
  **Explore → Tempo** to browse individual traces (anonymous access, local only). Run
  `scripts/smoke.sh` to put traffic on it.
- **Prometheus** <http://localhost:9090>: raw queries, and the *Alerts* page.
- **Tempo** <http://localhost:3200>: the trace store Grafana's Explore queries.

Every service exposes `/metrics` on its own internal port (`METRICS_ADDR`, 9101 to 9108),
never through nginx. Beyond the automatic Go runtime and gRPC metrics there are business
ones: orders by outcome (`ticketwave_orders_total`), the age of the oldest event stuck in
an outbox (`ticketwave_outbox_oldest_unpublished_age_seconds`, the number to alert on),
dead-lettered Kafka messages, seat-reservation conflicts, and live seat-map viewers.
The alert rules in `observability/prometheus/alerts.yml` are unit-tested with `promtool`.

Logs are JSON, one object per line, with a `service` field. Every request gets an ID at
nginx that follows it through the gateway and every gRPC call, so one order can be traced
across services with nothing but grep:

```bash
docker compose --profile apps logs | grep <request_id>
```

Successful RPCs log at `debug`; set `LOG_LEVEL=debug` on a service to see them.

### Tracing one request end to end

Metrics show that something is slow or failing in aggregate; they can't show you the
story of *one* request. For that, every service is instrumented with OpenTelemetry
(gRPC servers and clients via `otelgrpc`, the gateway's HTTP layer via `otelhttp`) and
exports spans to Tempo. The gateway's JSON logs carry the same `trace_id` its span
has, so you can go from a log line straight to the full waterfall:

1. Find a `trace_id` in the logs (or the browser's network tab — it's not currently
   returned as a response header, only logged).
2. Grafana → **Explore**, pick the **Tempo** datasource, paste the trace ID into the
   TraceQL box.
3. See every span it touched, in order, with per-hop timing — e.g. a `CreateOrder`
   call shows gateway → order → inventory (reserve) → payment (charge) → inventory
   (confirm), each with its own duration, all under one root span for the HTTP request.

Tracing degrades safely: with `OTEL_EXPORTER_OTLP_ENDPOINT=off` (or Tempo simply not
running) every service still works, it just exports nothing. **Known gap:** the
Kafka/outbox side (notification, the analytics projection, the realtime seat feed) is
not part of the trace — that would need trace context stored alongside each outbox
row, which is a real schema change, not yet done. What's covered is the whole
synchronous path: everything a browser's request waits for.

## Develop

Plain `docker compose up -d` starts only Postgres, Redis and Kafka, which is the
everyday mode: run services from your editor against them.

```bash
docker compose --profile apps run --rm migrate      # apply migrations to every database
cp .env.auth.example .env.auth                      # once per service, then edit
go run ./services/auth/cmd                          # from the repo root: paths are relative
npm --prefix web install && npm --prefix web run dev   # http://localhost:5173, proxies /api
```

## Test

```bash
go test ./...                                       # unit tests, no infrastructure needed
go test -tags integration ./...                     # also needs the env vars below
npm --prefix web test
scripts/smoke.sh                                    # end to end, against the running apps stack
```

Integration tests take one database URL per service and use an isolated schema per
test, so they never touch development data:

```bash
export AUTH_DATABASE_URL="postgres://ticketwave:ticketwave@localhost:5433/auth?sslmode=disable"
export INVENTORY_DATABASE_URL=...   # same shape, database "inventory"
export ORDER_DATABASE_URL=...       # database "orders"
export ANALYTICS_DATABASE_URL=...   # database "analytics"
export REDIS_ADDR=localhost:6379 KAFKA_BROKERS=localhost:9092
```

## CI/CD

`.github/workflows/ci.yml` runs on every pull request and push to `main`: protobuf lint,
breaking-change check and stale-generated-code check; gofmt, vet, golangci-lint, and all
Go tests with the race detector against real Postgres, Redis and Kafka; `govulncheck` and
`npm audit`; web typecheck, lint, tests and build; and the end-to-end smoke test on the
full containerised stack. `release.yml` publishes all images to GitHub Container Registry
when a `v*` tag is pushed. Dependabot keeps dependencies current.

## Security decisions worth knowing

- The refresh token lives only in an `HttpOnly`, `SameSite=Strict` cookie. The access
  token lives only in JavaScript memory, never in `localStorage`.
- Refresh tokens rotate; reusing a rotated one is treated as theft and revokes the family.
- The gateway takes the user ID from the verified token and computes the price itself.
  A request body naming its own `user_id` or `amount_cents` is rejected outright.
- Someone else's order returns 404, not 403, so order IDs cannot be probed.
- nginx overwrites `X-Real-IP`, so a client cannot choose the address it is rate-limited by.
- Only nginx is published. Each service that verifies tokens gets the public key only.

## Layout

```
proto/          API and event contracts (buf); generated Go is in gen/
pkg/            shared libraries: jwt, authz, kafka, outbox, ratelimit, apierr,
                 observability (logging, metrics, tracing), ...
services/       one directory per service: cmd/ and internal/
migrations/     one directory per database
web/            React app, Dockerfile and nginx.conf
scripts/        genkeys, smoke test, stress test, database init
observability/  Prometheus config + alert rules, Tempo config, Grafana dashboard
                 + provisioning
```
