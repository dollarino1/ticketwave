//go:build integration

package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// collector records the values a tail delivers, and lets a test wait for one.
type collector struct {
	mu   sync.Mutex
	vals []string
}

func (c *collector) add(m Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vals = append(c.vals, string(m.Value))
}

func (c *collector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.vals...)
}

func (c *collector) has(v string) bool {
	for _, got := range c.snapshot() {
		if got == v {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Two tails, standing in for two realtime-svc instances, must EACH see every
// message. A consumer group would split them; that difference is the whole point.
func TestIntegration_TailBroadcastsToEveryReaderAndSkipsHistory(t *testing.T) {
	brokers := testBrokers(t)
	topic := createTopic(t, brokers, 1)
	prod := NewProducer(brokers, topic)
	t.Cleanup(func() { _ = prod.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Written BEFORE anyone starts tailing: must never be delivered.
	must(t, prod.Publish(ctx, Message{Key: []byte("k"), Value: []byte("history")}))

	a, b := &collector{}, &collector{}
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, c := range []*collector{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := Tail(runCtx, TailConfig{Brokers: brokers, Topic: topic}, c.add); err != nil {
				t.Errorf("Tail: %v", err)
			}
		}()
	}

	// A tail's starting position is decided when it first reads, so a message
	// published too early would be missed. Keep sending probes until both readers
	// have proven they are positioned, then send the real ones.
	for i := 0; !a.has("probe") || !b.has("probe"); i++ {
		if i > 100 {
			t.Fatal("tails never became ready")
		}
		must(t, prod.Publish(ctx, Message{Key: []byte("k"), Value: []byte("probe")}))
		time.Sleep(200 * time.Millisecond)
	}
	for i := 1; i <= 5; i++ {
		must(t, prod.Publish(ctx, Message{Key: []byte("k"), Value: []byte(fmt.Sprintf("m%d", i))}))
	}

	for name, c := range map[string]*collector{"first": a, "second": b} {
		waitFor(t, name+" tail to receive m5", func() bool { return c.has("m5") })
		var real []string
		for _, v := range c.snapshot() {
			if v != "probe" {
				real = append(real, v)
			}
		}
		want := []string{"m1", "m2", "m3", "m4", "m5"}
		if fmt.Sprint(real) != fmt.Sprint(want) {
			t.Errorf("%s tail got %v, want %v (every message once, in order, no history)", name, real, want)
		}
	}

	stop()
	wg.Wait()
}
