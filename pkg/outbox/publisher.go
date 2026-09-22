// Package outbox implements the publishing half of the transactional outbox
// pattern: a service writes an event row in the same transaction as the state
// change it describes, and this publisher relays those rows to Kafka afterwards.
// Any service with an outbox table of the expected shape can use it.
package outbox

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Table names the outbox table and the column whose value becomes the Kafka key.
//
// The table must have: id UUID, <KeyColumn> UUID, event_type TEXT, payload JSONB,
// published BOOLEAN, published_at TIMESTAMPTZ, created_at TIMESTAMPTZ.
type Table struct {
	Name      string
	KeyColumn string
}

// SQL identifiers cannot be passed as query parameters, so they are spliced into
// the statements. That is only safe for a fixed, known-plain shape, which is
// enforced here rather than trusted.
var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func (t Table) validate() error {
	if !identifier.MatchString(t.Name) || !identifier.MatchString(t.KeyColumn) {
		return fmt.Errorf("outbox: %q and %q must be plain lower-case SQL identifiers", t.Name, t.KeyColumn)
	}
	return nil
}

// Publisher relays unpublished outbox rows to Kafka.
//
// Delivery is at-least-once. If the process dies after Kafka accepted a batch
// but before the rows were marked published, the next run sends that batch
// again, so consumers must tolerate duplicates (the message-id header lets them
// dedupe).
type Publisher struct {
	pool     *pgxpool.Pool
	out      kafka.Publisher
	interval time.Duration
	batch    int

	table     string
	selectSQL string
	updateSQL string
}

// NewPublisher panics on an invalid table: that is a programming error made at
// startup, not a runtime condition to handle.
func NewPublisher(pool *pgxpool.Pool, out kafka.Publisher, table Table) *Publisher {
	if err := table.validate(); err != nil {
		panic(err)
	}
	return &Publisher{
		pool:     pool,
		out:      out,
		table:    table.Name,
		interval: 200 * time.Millisecond,
		batch:    100,
		selectSQL: fmt.Sprintf(
			`SELECT id, %s, event_type, payload
			   FROM %s
			  WHERE published = false
			  ORDER BY created_at, id
			  LIMIT $1
			    FOR UPDATE SKIP LOCKED`, table.KeyColumn, table.Name),
		updateSQL: fmt.Sprintf(
			`UPDATE %s SET published = true, published_at = now() WHERE id = ANY($1)`, table.Name),
	}
}

// Run publishes until ctx is cancelled. It drains back-to-back while rows are
// waiting and sleeps for interval only when the outbox is empty or a batch failed.
func (p *Publisher) Run(ctx context.Context) {
	for {
		n, err := p.publishBatch(ctx)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			log.Printf("outbox: publish batch: %v", err)
		case n > 0:
			log.Printf("outbox: published %d event(s)", n)
			continue
		}

		t := time.NewTimer(p.interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// publishBatch relays up to one batch and reports how many rows it sent.
//
// FOR UPDATE SKIP LOCKED is what lets several replicas run this at once: each
// replica locks its own rows and skips the ones another replica holds, so no row
// is sent twice concurrently and nobody blocks. The trade-off is that ordering per
// key is then only best effort, so consumers must not depend on it.
//
// The Kafka write happens inside the transaction, so the row locks are held for
// one network round trip. That is what makes "published" and "sent" agree.
func (p *Publisher) publishBatch(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, p.selectSQL, p.batch)
	if err != nil {
		return 0, fmt.Errorf("select unpublished rows: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	var msgs []kafka.Message
	for rows.Next() {
		var id, key uuid.UUID
		var eventType string
		var payload []byte
		if err := rows.Scan(&id, &key, &eventType, &payload); err != nil {
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		ids = append(ids, id)
		msgs = append(msgs, kafka.Message{
			// Keying sends every event about one aggregate to one partition.
			Key:   []byte(key.String()),
			Value: payload,
			Headers: map[string]string{
				events.HeaderMessageID: id.String(),
				events.HeaderEventType: eventType,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate outbox rows: %w", err)
	}
	rows.Close() // the connection can run the UPDATE below only once the rows are closed

	if len(msgs) == 0 {
		return 0, nil
	}

	if err := p.out.Publish(ctx, msgs...); err != nil {
		return 0, fmt.Errorf("publish %d event(s): %w", len(msgs), err)
	}

	if _, err := tx.Exec(ctx, p.updateSQL, ids); err != nil {
		return 0, fmt.Errorf("mark rows published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	publishedTotal.WithLabelValues(p.table).Add(float64(len(msgs)))
	return len(msgs), nil
}
