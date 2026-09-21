package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// fakeReader serves a fixed queue of messages, then blocks until the context
// ends, like a real reader on an idle partition. It records every commit.
type fakeReader struct {
	mu      sync.Mutex
	queue   []kafkago.Message
	commits [][]kafkago.Message
}

func newFakeReader(n int) *fakeReader {
	f := &fakeReader{}
	for i := 0; i < n; i++ {
		f.queue = append(f.queue, kafkago.Message{
			Topic:  "orders",
			Offset: int64(i),
			Key:    []byte("k"),
			Value:  []byte(fmt.Sprintf("v%d", i)),
		})
	}
	return f
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	f.mu.Lock()
	if len(f.queue) > 0 {
		m := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		return m, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, msgs)
	return nil
}

func (f *fakeReader) Close() error { return nil }

func (f *fakeReader) commitSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	sizes := make([]int, len(f.commits))
	for i, c := range f.commits {
		sizes[i] = len(c)
	}
	return sizes
}

type fakeDLQ struct {
	mu   sync.Mutex
	msgs []Message
	err  error
}

func (d *fakeDLQ) Publish(_ context.Context, msgs ...Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.msgs = append(d.msgs, msgs...)
	return nil
}

func (d *fakeDLQ) published() []Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Message(nil), d.msgs...)
}

func testConfig(dlq Publisher) ConsumerConfig {
	return ConsumerConfig{
		Brokers:      []string{"unused"},
		Topic:        "orders",
		GroupID:      "test-group",
		DLQ:          dlq,
		Concurrency:  1,
		BatchSize:    100,
		BatchWait:    20 * time.Millisecond,
		MaxAttempts:  3,
		RetryBackoff: time.Millisecond,
	}
}

// start runs the consumer in the background. The returned stop function cancels
// it and returns whatever Run returned.
func start(t *testing.T, c *Consumer) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop within 2s of cancellation")
			return nil
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestConsumer_HandlesWholeBatchAndCommitsOnce(t *testing.T) {
	r := newFakeReader(5)
	var handled atomic.Int32
	c := newConsumer(testConfig(&fakeDLQ{}), r, func(context.Context, Message) error {
		handled.Add(1)
		return nil
	})

	stop := start(t, c)
	eventually(t, "one commit", func() bool { return len(r.commitSizes()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil on shutdown", err)
	}

	if got := handled.Load(); got != 5 {
		t.Errorf("handled %d messages, want 5", got)
	}
	if got := r.commitSizes(); len(got) != 1 || got[0] != 5 {
		t.Errorf("commit sizes = %v, want [5]", got)
	}
}

func TestConsumer_SplitsIntoBatchesOfBatchSize(t *testing.T) {
	r := newFakeReader(5)
	cfg := testConfig(&fakeDLQ{})
	cfg.BatchSize = 2
	c := newConsumer(cfg, r, func(context.Context, Message) error { return nil })

	stop := start(t, c)
	eventually(t, "three commits", func() bool { return len(r.commitSizes()) == 3 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	got := r.commitSizes()
	want := []int{2, 2, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("commit sizes = %v, want %v", got, want)
		}
	}
}

func TestConsumer_RetriesUntilHandlerSucceeds(t *testing.T) {
	r := newFakeReader(1)
	dlq := &fakeDLQ{}
	var calls atomic.Int32
	c := newConsumer(testConfig(dlq), r, func(context.Context, Message) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})

	stop := start(t, c)
	eventually(t, "commit", func() bool { return len(r.commitSizes()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("handler called %d times, want 3", got)
	}
	if got := dlq.published(); len(got) != 0 {
		t.Errorf("DLQ got %d messages, want 0", len(got))
	}
}

func TestConsumer_DeadLettersAfterMaxAttempts(t *testing.T) {
	r := newFakeReader(1)
	dlq := &fakeDLQ{}
	cfg := testConfig(dlq)
	cfg.MaxAttempts = 2
	var calls atomic.Int32
	c := newConsumer(cfg, r, func(context.Context, Message) error {
		calls.Add(1)
		return errors.New("boom")
	})

	stop := start(t, c)
	eventually(t, "commit", func() bool { return len(r.commitSizes()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("handler called %d times, want MaxAttempts=2", got)
	}
	dead := dlq.published()
	if len(dead) != 1 {
		t.Fatalf("DLQ got %d messages, want 1", len(dead))
	}
	if !strings.Contains(dead[0].Headers["dlq-error"], "boom") {
		t.Errorf("dlq-error header = %q, want it to mention the cause", dead[0].Headers["dlq-error"])
	}
	if dead[0].Headers["dlq-source-topic"] != "orders" || dead[0].Headers["dlq-source-offset"] != "0" {
		t.Errorf("source headers = %v, want topic=orders offset=0", dead[0].Headers)
	}
	if string(dead[0].Value) != "v0" || string(dead[0].Key) != "k" {
		t.Errorf("DLQ message key/value = %q/%q, want the original k/v0", dead[0].Key, dead[0].Value)
	}
}

func TestConsumer_PermanentErrorSkipsRetriesAndDeadLettersAtOnce(t *testing.T) {
	r := newFakeReader(1)
	dlq := &fakeDLQ{}
	cfg := testConfig(dlq)
	cfg.MaxAttempts = 5
	var calls atomic.Int32
	c := newConsumer(cfg, r, func(context.Context, Message) error {
		calls.Add(1)
		return Permanent(errors.New("bad json"))
	})

	stop := start(t, c)
	eventually(t, "commit", func() bool { return len(r.commitSizes()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("handler called %d times, want 1: a permanent error must not be retried", got)
	}
	dead := dlq.published()
	if len(dead) != 1 {
		t.Fatalf("DLQ got %d messages, want 1", len(dead))
	}
	if !strings.Contains(dead[0].Headers["dlq-error"], "bad json") {
		t.Errorf("dlq-error = %q, want it to mention the cause", dead[0].Headers["dlq-error"])
	}
	if dead[0].Headers["dlq-attempts"] != "1" {
		t.Errorf("dlq-attempts = %q, want 1", dead[0].Headers["dlq-attempts"])
	}
}

func TestPermanent_IsDetectableAndKeepsTheCause(t *testing.T) {
	cause := errors.New("bad json")
	err := Permanent(cause)

	if !IsPermanent(err) {
		t.Error("IsPermanent(Permanent(err)) = false, want true")
	}
	if !errors.Is(err, cause) {
		t.Error("the original cause is no longer reachable through errors.Is")
	}
	if IsPermanent(cause) {
		t.Error("IsPermanent reported an ordinary error as permanent")
	}
}

func TestConsumer_DoesNotCommitWhenDLQIsDown(t *testing.T) {
	errDLQDown := errors.New("dlq down")
	r := newFakeReader(1)
	cfg := testConfig(&fakeDLQ{err: errDLQDown})
	c := newConsumer(cfg, r, func(context.Context, Message) error { return errors.New("boom") })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)

	if !errors.Is(err, errDLQDown) {
		t.Fatalf("Run returned %v, want an error wrapping the DLQ failure", err)
	}
	if got := r.commitSizes(); len(got) != 0 {
		t.Errorf("committed %v, want nothing: an unparked message must be redelivered", got)
	}
}

func TestConsumer_RespectsConcurrencyLimit(t *testing.T) {
	r := newFakeReader(6)
	cfg := testConfig(&fakeDLQ{})
	cfg.Concurrency = 2
	var inFlight, maxInFlight atomic.Int32
	c := newConsumer(cfg, r, func(context.Context, Message) error {
		n := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if n <= old || maxInFlight.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	})

	stop := start(t, c)
	eventually(t, "commit", func() bool { return len(r.commitSizes()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := maxInFlight.Load(); got != 2 {
		t.Errorf("max handlers in flight = %d, want exactly Concurrency=2", got)
	}
}

func TestConsumer_ShutdownWhileIdleReturnsNil(t *testing.T) {
	r := newFakeReader(0)
	c := newConsumer(testConfig(&fakeDLQ{}), r, func(context.Context, Message) error { return nil })

	stop := start(t, c)
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if got := r.commitSizes(); len(got) != 0 {
		t.Errorf("committed %v, want nothing", got)
	}
}

func TestConsumer_ShutdownDuringRetryLeavesBatchUncommitted(t *testing.T) {
	r := newFakeReader(1)
	dlq := &fakeDLQ{}
	cfg := testConfig(dlq)
	cfg.RetryBackoff = time.Hour // the retry wait is what shutdown must interrupt
	var calls atomic.Int32
	c := newConsumer(cfg, r, func(context.Context, Message) error {
		calls.Add(1)
		return errors.New("boom")
	})

	stop := start(t, c)
	eventually(t, "first attempt", func() bool { return calls.Load() >= 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v, want nil on shutdown", err)
	}

	if got := r.commitSizes(); len(got) != 0 {
		t.Errorf("committed %v, want nothing so the message is redelivered", got)
	}
	if got := dlq.published(); len(got) != 0 {
		t.Errorf("DLQ got %d messages, want 0: shutdown is not a handler failure", len(got))
	}
}

func TestNewConsumer_RejectsMissingConfig(t *testing.T) {
	noop := func(context.Context, Message) error { return nil }
	valid := ConsumerConfig{Brokers: []string{"b"}, Topic: "t", GroupID: "g", DLQ: &fakeDLQ{}}

	cases := map[string]func(*ConsumerConfig){
		"no brokers":  func(c *ConsumerConfig) { c.Brokers = nil },
		"no topic":    func(c *ConsumerConfig) { c.Topic = "" },
		"no group ID": func(c *ConsumerConfig) { c.GroupID = "" },
		"no DLQ":      func(c *ConsumerConfig) { c.DLQ = nil },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			breakIt(&cfg)
			if _, err := NewConsumer(cfg, noop); err == nil {
				t.Fatal("NewConsumer returned nil error, want a validation error")
			}
		})
	}
}
