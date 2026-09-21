//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// These tests talk to a real broker. Run them with, for example:
//
//	KAFKA_BROKERS=localhost:9092 go test -tags integration ./pkg/kafka/
//
// Every test creates its own uniquely named topics and consumer group, so tests
// never interfere with each other or with the real order-events topic.

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func testBrokers(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	return strings.Split(v, ",")
}

func shortID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
}

// controllerConn dials the cluster controller, the only broker that accepts
// topic creation and deletion.
func controllerConn(ctx context.Context, brokers []string) (*kafkago.Conn, error) {
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	c, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	return kafkago.DialContext(ctx, "tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)))
}

func createTopic(t *testing.T, brokers []string, partitions int) string {
	t.Helper()
	ctx := context.Background()
	name := "it-" + shortID()

	ctrl, err := controllerConn(ctx, brokers)
	must(t, err)
	defer func() { _ = ctrl.Close() }()
	must(t, ctrl.CreateTopics(kafkago.TopicConfig{
		Topic:             name,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}))

	t.Cleanup(func() {
		if c, err := controllerConn(context.Background(), brokers); err == nil {
			_ = c.DeleteTopics(name)
			_ = c.Close()
		}
	})

	// Creating a topic returns before every partition has a leader. Wait, so the
	// first write cannot race the election.
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	must(t, err)
	defer func() { _ = conn.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		parts, err := conn.ReadPartitions(name)
		if err == nil && len(parts) == partitions && allHaveLeaders(parts) {
			return name
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s did not become ready: partitions=%d err=%v", name, len(parts), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func allHaveLeaders(parts []kafkago.Partition) bool {
	for _, p := range parts {
		if p.Leader.Host == "" {
			return false
		}
	}
	return true
}

func newTestConsumer(t *testing.T, brokers []string, topic, dlqTopic, group string, batchSize int, h Handler) *Consumer {
	t.Helper()
	dlq := NewProducer(brokers, dlqTopic)
	t.Cleanup(func() { _ = dlq.Close() })

	c, err := NewConsumer(ConsumerConfig{
		Brokers:      brokers,
		Topic:        topic,
		GroupID:      group,
		DLQ:          dlq,
		Concurrency:  1,
		BatchSize:    batchSize,
		BatchWait:    300 * time.Millisecond,
		MaxAttempts:  2,
		RetryBackoff: 10 * time.Millisecond,
	}, h)
	must(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// runUntil runs c until done is closed, then stops it and returns what Run
// returned. It fails the test if done is not closed before ctx expires.
func runUntil(ctx context.Context, t *testing.T, c *Consumer, done <-chan struct{}) error {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- c.Run(runCtx) }()

	select {
	case <-done:
	case <-ctx.Done():
		cancel()
		<-errc
		t.Fatal("timed out waiting for the consumer to reach its goal")
	}
	cancel()
	return <-errc
}

// counter closes done once n calls have been recorded.
type counter struct {
	mu   sync.Mutex
	n    int
	want int
	done chan struct{}
}

func newCounter(want int) *counter {
	return &counter{want: want, done: make(chan struct{})}
}

func (c *counter) hit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if c.n == c.want {
		close(c.done)
	}
}

func TestIntegration_KeysStayOnOnePartition(t *testing.T) {
	brokers := testBrokers(t)
	topic := createTopic(t, brokers, 3)
	dlqTopic := createTopic(t, brokers, 1)

	const keys, perKey = 12, 3
	var msgs []Message
	for k := 0; k < keys; k++ {
		for i := 0; i < perKey; i++ {
			msgs = append(msgs, Message{
				Key:   []byte(fmt.Sprintf("order-%d", k)),
				Value: []byte(fmt.Sprintf("%d-%d", k, i)),
			})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prod := NewProducer(brokers, topic)
	defer func() { _ = prod.Close() }()
	must(t, prod.Publish(ctx, msgs...))

	var mu sync.Mutex
	partitionsByKey := map[string]map[int]bool{}
	seen := newCounter(len(msgs))
	c := newTestConsumer(t, brokers, topic, dlqTopic, "it-group-"+shortID(), 100, func(_ context.Context, m Message) error {
		mu.Lock()
		if partitionsByKey[string(m.Key)] == nil {
			partitionsByKey[string(m.Key)] = map[int]bool{}
		}
		partitionsByKey[string(m.Key)][m.Partition] = true
		mu.Unlock()
		seen.hit()
		return nil
	})

	if err := runUntil(ctx, t, c, seen.done); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	used := map[int]bool{}
	for key, parts := range partitionsByKey {
		if len(parts) != 1 {
			t.Errorf("key %s was spread over partitions %v, want exactly one so its events stay in order", key, parts)
		}
		for p := range parts {
			used[p] = true
		}
	}
	if len(used) < 2 {
		t.Errorf("%d keys all landed on partitions %v; the balancer looks like it ignores the key", keys, used)
	}
}

func TestIntegration_ResumesAfterCommitWithoutRedelivery(t *testing.T) {
	brokers := testBrokers(t)
	topic := createTopic(t, brokers, 1) // one partition, so order is deterministic
	dlqTopic := createTopic(t, brokers, 1)

	var msgs []Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, Message{Key: []byte("k"), Value: []byte(fmt.Sprintf("m%d", i))})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prod := NewProducer(brokers, topic)
	defer func() { _ = prod.Close() }()
	must(t, prod.Publish(ctx, msgs...))

	group := "it-group-" + shortID()
	collect := func(want int) []string {
		var mu sync.Mutex
		var got []string
		seen := newCounter(want)
		// BatchSize == want, so the consumer commits exactly `want` messages and no more.
		c := newTestConsumer(t, brokers, topic, dlqTopic, group, want, func(_ context.Context, m Message) error {
			mu.Lock()
			got = append(got, string(m.Value))
			mu.Unlock()
			seen.hit()
			return nil
		})
		if err := runUntil(ctx, t, c, seen.done); err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
		must(t, c.Close()) // leave the group so the next consumer can take over promptly
		mu.Lock()
		defer mu.Unlock()
		return got
	}

	first := collect(5)
	second := collect(5)

	if strings.Join(first, ",") != "m0,m1,m2,m3,m4" {
		t.Errorf("first consumer saw %v, want m0..m4", first)
	}
	if strings.Join(second, ",") != "m5,m6,m7,m8,m9" {
		t.Errorf("second consumer saw %v, want m5..m9: it must resume after the commit, neither repeating nor skipping", second)
	}
}

func TestIntegration_PoisonMessageIsDeadLetteredAndTheRestContinue(t *testing.T) {
	brokers := testBrokers(t)
	topic := createTopic(t, brokers, 1)
	dlqTopic := createTopic(t, brokers, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prod := NewProducer(brokers, topic)
	defer func() { _ = prod.Close() }()
	must(t, prod.Publish(ctx,
		Message{Key: []byte("k"), Value: []byte("ok-1")},
		Message{Key: []byte("k"), Value: []byte("poison")},
		Message{Key: []byte("k"), Value: []byte("ok-2")},
	))

	var mu sync.Mutex
	var handled []string
	good := newCounter(2)
	c := newTestConsumer(t, brokers, topic, dlqTopic, "it-group-"+shortID(), 100, func(_ context.Context, m Message) error {
		if string(m.Value) == "poison" {
			return Permanent(errors.New("cannot parse"))
		}
		mu.Lock()
		handled = append(handled, string(m.Value))
		mu.Unlock()
		good.hit()
		return nil
	})
	if err := runUntil(ctx, t, c, good.done); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	mu.Lock()
	if strings.Join(handled, ",") != "ok-1,ok-2" {
		t.Errorf("handled %v, want [ok-1 ok-2]: the poison message must not block its neighbours", handled)
	}
	mu.Unlock()

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     dlqTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1 << 20,
	})
	defer func() { _ = r.Close() }()
	readCtx, cancelRead := context.WithTimeout(ctx, 15*time.Second)
	defer cancelRead()
	dead, err := r.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("nothing arrived on the DLQ: %v", err)
	}

	headers := map[string]string{}
	for _, h := range dead.Headers {
		headers[h.Key] = string(h.Value)
	}
	if string(dead.Value) != "poison" {
		t.Errorf("DLQ value = %q, want the original poison payload", dead.Value)
	}
	if !strings.Contains(headers["dlq-error"], "cannot parse") {
		t.Errorf("dlq-error = %q, want the cause", headers["dlq-error"])
	}
	if headers["dlq-source-topic"] != topic || headers["dlq-source-offset"] != "1" {
		t.Errorf("source headers = %v, want topic %s offset 1", headers, topic)
	}
	if headers["dlq-attempts"] != "1" {
		t.Errorf("dlq-attempts = %q, want 1 for a permanent failure", headers["dlq-attempts"])
	}
}
