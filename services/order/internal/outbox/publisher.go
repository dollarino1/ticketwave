// Package outbox implements the publishing half of the transactional outbox
// pattern: order-svc writes an event row in the same transaction as the state
// change, and this publisher relays those rows to Kafka afterwards.
package outbox

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Publisher relays unpublished outbox rows to Kafka.
//
// Delivery is at-least-once. If the process dies after Kafka accepted a batch
// but before the rows were marked published, the next run sends that batch
// again, so consumers must dedupe on the message-id header.
type Publisher struct {
	pool     *pgxpool.Pool
	out      kafka.Publisher
	interval time.Duration
	batch    int
}

func NewPublisher(pool *pgxpool.Pool, out kafka.Publisher) *Publisher {
	return &Publisher{
		pool:     pool,
		out:      out,
		interval: 200 * time.Millisecond,
		batch:    100,
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
// FOR UPDATE SKIP LOCKED is what lets several order-svc replicas run this at
// once: each replica locks its own rows and skips the ones another replica
// holds, so no row is sent twice concurrently and nobody blocks. The trade-off
// is that per-order ordering is then only best effort, so consumers must not
// depend on CREATED always arriving before CONFIRMED.
//
// The Kafka write happens inside the transaction, so the row locks are held for
// one network round trip. That is what makes "published" and "sent" agree.
func (p *Publisher) publishBatch(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT id, order_id, event_type, payload
		   FROM outbox
		  WHERE published = false
		  ORDER BY created_at, id
		  LIMIT $1
		    FOR UPDATE SKIP LOCKED`,
		p.batch,
	)
	if err != nil {
		return 0, fmt.Errorf("select unpublished rows: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	var msgs []kafka.Message
	for rows.Next() {
		var id, orderID uuid.UUID
		var eventType string
		var payload []byte
		if err := rows.Scan(&id, &orderID, &eventType, &payload); err != nil {
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		ids = append(ids, id)
		msgs = append(msgs, kafka.Message{
			// Keying by order ID sends every event of one order to one partition.
			Key:   []byte(orderID.String()),
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

	if _, err := tx.Exec(ctx,
		`UPDATE outbox SET published = true, published_at = now() WHERE id = ANY($1)`, ids,
	); err != nil {
		return 0, fmt.Errorf("mark rows published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(msgs), nil
}
