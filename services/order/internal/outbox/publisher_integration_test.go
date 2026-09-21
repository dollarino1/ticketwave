//go:build integration

package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestPool returns a pool whose search_path is a private schema holding freshly
// migrated copies of the order tables. Tests therefore never read, and above all
// never mark as published, rows that belong to real development data.
//
// Run with ORDER_DATABASE_URL pointing at the orders database, e.g.
//
//	ORDER_DATABASE_URL="postgres://ticketwave:ticketwave@localhost:5433/orders?sslmode=disable" \
//	  go test -tags integration ./services/order/internal/outbox/
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("ORDER_DATABASE_URL")
	if url == "" {
		t.Skip("ORDER_DATABASE_URL not set")
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, url)
	must(t, err)
	schema := "outbox_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	must(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})

	cfg, err := pgxpool.ParseConfig(url)
	must(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	must(t, err)
	t.Cleanup(pool.Close)

	files, err := filepath.Glob("../../../../migrations/order/*.up.sql")
	must(t, err)
	if len(files) == 0 {
		t.Fatal("found no migrations under migrations/order")
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		must(t, err)
		_, err = pool.Exec(ctx, string(sql))
		must(t, err)
	}
	return pool
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// seed inserts one order and n unpublished outbox rows for it, created one
// second apart so their creation order is unambiguous. It returns the row IDs
// in that order.
func seed(t *testing.T, pool *pgxpool.Pool, n int) (orderID uuid.UUID, ids []uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	orderID = uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO orders (id, user_id, event_id, seat_ids, amount_cents) VALUES ($1, $2, $3, $4, 100)`,
		orderID, uuid.New(), uuid.New(), []string{uuid.NewString()},
	)
	must(t, err)

	base := time.Now().Add(-time.Hour)
	for i := 0; i < n; i++ {
		id := uuid.New()
		_, err := pool.Exec(ctx,
			`INSERT INTO outbox (id, order_id, event_type, payload, created_at) VALUES ($1, $2, 'order.created', $3, $4)`,
			id, orderID, []byte(fmt.Sprintf(`{"n":%d}`, i)), base.Add(time.Duration(i)*time.Second),
		)
		must(t, err)
		ids = append(ids, id)
	}
	return orderID, ids
}

func countPublished(t *testing.T, pool *pgxpool.Pool) (published, total int) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FILTER (WHERE published), count(*) FROM outbox`).Scan(&published, &total)
	must(t, err)
	return published, total
}

type recorder struct {
	mu   sync.Mutex
	msgs []kafka.Message
	err  error
}

func (r *recorder) Publish(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.msgs = append(r.msgs, msgs...)
	return nil
}

func (r *recorder) messages() []kafka.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]kafka.Message(nil), r.msgs...)
}

func (r *recorder) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func newTestPublisher(pool *pgxpool.Pool, out kafka.Publisher) *Publisher {
	return &Publisher{pool: pool, out: out, interval: 10 * time.Millisecond, batch: 100}
}

func TestPublishBatch_SendsInCreationOrderAndMarksPublished(t *testing.T) {
	pool := newTestPool(t)
	orderID, ids := seed(t, pool, 5)
	rec := &recorder{}
	p := newTestPublisher(pool, rec)

	n, err := p.publishBatch(context.Background())
	must(t, err)
	if n != 5 {
		t.Fatalf("publishBatch sent %d rows, want 5", n)
	}

	msgs := rec.messages()
	for i, m := range msgs {
		if got := m.Headers[events.HeaderMessageID]; got != ids[i].String() {
			t.Errorf("message %d has message-id %s, want %s (creation order)", i, got, ids[i])
		}
		if string(m.Key) != orderID.String() {
			t.Errorf("message %d key = %s, want the order ID %s", i, m.Key, orderID)
		}
		if m.Headers[events.HeaderEventType] != "order.created" {
			t.Errorf("message %d event-type = %s, want order.created", i, m.Headers[events.HeaderEventType])
		}
	}
	if published, total := countPublished(t, pool); published != 5 || total != 5 {
		t.Errorf("published/total = %d/%d, want 5/5", published, total)
	}

	// A second pass must find nothing: rows are sent once, not on every tick.
	n, err = p.publishBatch(context.Background())
	must(t, err)
	if n != 0 || len(rec.messages()) != 5 {
		t.Errorf("second pass sent %d rows (total %d messages), want 0 and still 5", n, len(rec.messages()))
	}
}

func TestPublishBatch_RespectsBatchSize(t *testing.T) {
	pool := newTestPool(t)
	seed(t, pool, 7)
	p := newTestPublisher(pool, &recorder{})
	p.batch = 3

	n, err := p.publishBatch(context.Background())
	must(t, err)
	if n != 3 {
		t.Fatalf("publishBatch sent %d rows, want the batch size 3", n)
	}
	if published, _ := countPublished(t, pool); published != 3 {
		t.Errorf("%d rows marked published, want 3", published)
	}
}

func TestPublishBatch_KafkaFailureLeavesRowsUnpublishedForRetry(t *testing.T) {
	pool := newTestPool(t)
	seed(t, pool, 4)
	rec := &recorder{err: errors.New("broker down")}
	p := newTestPublisher(pool, rec)

	if _, err := p.publishBatch(context.Background()); err == nil {
		t.Fatal("publishBatch returned nil while Kafka was down, want an error")
	}
	if published, total := countPublished(t, pool); published != 0 || total != 4 {
		t.Fatalf("published/total = %d/%d after a failed publish, want 0/4: nothing may be marked sent", published, total)
	}

	rec.setErr(nil)
	n, err := p.publishBatch(context.Background())
	must(t, err)
	if n != 4 {
		t.Errorf("retry sent %d rows, want all 4 that the failed attempt left behind", n)
	}
}

func TestPublishBatch_ConcurrentPublishersNeverSendARowTwice(t *testing.T) {
	pool := newTestPool(t)
	const rows = 200
	seed(t, pool, rows)
	rec := &recorder{}

	// A deadline turns "rows never get marked published" into a fast failure. Without
	// it every worker would keep re-finding the same rows and the test would hang.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		p := newTestPublisher(pool, rec)
		p.batch = 10
		wg.Go(func() {
			for {
				n, err := p.publishBatch(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()

	seen := make(map[string]int)
	for _, m := range rec.messages() {
		seen[m.Headers[events.HeaderMessageID]]++
	}
	if len(seen) != rows {
		t.Errorf("%d distinct rows were sent, want %d", len(seen), rows)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("row %s was sent %d times, want exactly once (row locking)", id, count)
		}
	}
	if published, _ := countPublished(t, pool); published != rows {
		t.Errorf("%d rows marked published, want %d", published, rows)
	}
}

func TestRun_PicksUpNewRowsAndStopsOnCancel(t *testing.T) {
	pool := newTestPool(t)
	rec := &recorder{}
	p := newTestPublisher(pool, rec)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	// Rows arrive while Run is already polling, as they do in production.
	seed(t, pool, 3)

	deadline := time.Now().Add(3 * time.Second)
	for len(rec.messages()) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(rec.messages()); got != 3 {
		t.Fatalf("Run published %d messages, want 3", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancellation")
	}
}
