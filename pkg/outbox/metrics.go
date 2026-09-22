package outbox

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var publishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ticketwave_outbox_published_total",
	Help: "Outbox rows relayed to Kafka.",
}, []string{"table"})

// backlogCollector reports how far behind the outbox is. It runs its query when
// Prometheus scrapes, so the numbers are always current and cost nothing between
// scrapes. The query hits the partial index on unpublished rows, so it stays cheap
// however large the table of published history becomes.
//
// Two numbers, because they answer different questions. Depth says how much is
// waiting; age of the OLDEST says how long the unluckiest event has waited. A
// publisher that has died shows a growing age immediately, even when traffic is so
// low that the depth is one. Alert on age.
type backlogCollector struct {
	pool  *pgxpool.Pool
	table Table
	depth *prometheus.Desc
	age   *prometheus.Desc
	query string
}

func newBacklogCollector(pool *pgxpool.Pool, table Table) *backlogCollector {
	labels := prometheus.Labels{"table": table.Name}
	return &backlogCollector{
		pool:  pool,
		table: table,
		depth: prometheus.NewDesc("ticketwave_outbox_unpublished",
			"Outbox rows waiting to be relayed to Kafka.", nil, labels),
		age: prometheus.NewDesc("ticketwave_outbox_oldest_unpublished_age_seconds",
			"Age of the oldest row still waiting. Grows without bound when the publisher is not running.", nil, labels),
		query: fmt.Sprintf(
			`SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8
			   FROM %s WHERE published = false`, table.Name),
	}
}

func (c *backlogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.age
}

func (c *backlogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var depth int64
	var age float64
	if err := c.pool.QueryRow(ctx, c.query).Scan(&depth, &age); err != nil {
		// NaN, not a scrape error: an invalid metric makes the whole /metrics
		// response fail, which would hide every other metric of the service exactly
		// when the database is down and they are needed most.
		ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, math.NaN())
		ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, math.NaN())
		return
	}
	ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(depth))
	ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, age)
}

// RegisterMetrics exposes the outbox backlog of table on the default registry. Call
// it once per outbox at startup. It is separate from NewPublisher so that building a
// publisher has no side effects on global state.
func RegisterMetrics(pool *pgxpool.Pool, table Table) error {
	if err := table.validate(); err != nil {
		return err
	}
	if err := prometheus.Register(newBacklogCollector(pool, table)); err != nil {
		return fmt.Errorf("register outbox metrics for %s: %w", table.Name, err)
	}
	return nil
}
