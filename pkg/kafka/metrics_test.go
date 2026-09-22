package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Metrics are process-global, so each test uses its own consumer group: that keeps
// its counters separate from every other test's without having to reset anything.
func outcomes(group string) (handled, retried, dead float64) {
	get := func(result string) float64 {
		return testutil.ToFloat64(consumed.WithLabelValues("orders", group, result))
	}
	return get(resultHandled), get(resultRetried), get(resultDeadLettered)
}

func runOne(t *testing.T, group string, messages, maxAttempts int, h Handler) {
	t.Helper()
	r := newFakeReader(messages)
	cfg := testConfig(&fakeDLQ{})
	cfg.GroupID = group
	cfg.MaxAttempts = maxAttempts
	c := newConsumer(cfg, r, h)
	stop := start(t, c)
	eventually(t, "commit", func() bool { return len(r.commitSizes()) >= 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func TestMetrics_CountHandledMessages(t *testing.T) {
	runOne(t, "m-handled", 3, 3, func(context.Context, Message) error { return nil })

	if h, r, d := outcomes("m-handled"); h != 3 || r != 0 || d != 0 {
		t.Errorf("handled/retried/dead = %v/%v/%v, want 3/0/0", h, r, d)
	}
}

func TestMetrics_CountEachRetryButNotTheFinalSuccess(t *testing.T) {
	var calls atomic.Int32
	runOne(t, "m-retry", 1, 5, func(context.Context, Message) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})

	if h, r, d := outcomes("m-retry"); h != 1 || r != 2 || d != 0 {
		t.Errorf("handled/retried/dead = %v/%v/%v, want 1/2/0", h, r, d)
	}
}

func TestMetrics_CountADeadLetterOnceAfterTheRetriesRunOut(t *testing.T) {
	runOne(t, "m-dead", 1, 3, func(context.Context, Message) error { return errors.New("always") })

	// Three attempts means two retries, then the message is given up on.
	if h, r, d := outcomes("m-dead"); h != 0 || r != 2 || d != 1 {
		t.Errorf("handled/retried/dead = %v/%v/%v, want 0/2/1", h, r, d)
	}
}

func TestMetrics_APermanentFailureIsDeadLetteredWithoutCountingRetries(t *testing.T) {
	runOne(t, "m-permanent", 1, 5, func(context.Context, Message) error { return Permanent(errors.New("bad json")) })

	if h, r, d := outcomes("m-permanent"); h != 0 || r != 0 || d != 1 {
		t.Errorf("handled/retried/dead = %v/%v/%v, want 0/0/1", h, r, d)
	}
}

// Without this a dashboard shows "no data" for a healthy consumer, and rate() misses
// the first dead letter because the series is born already at 1.
func TestMetrics_EverySeriesExistsAtZeroBeforeAnythingHappens(t *testing.T) {
	cfg := testConfig(&fakeDLQ{})
	cfg.GroupID = "m-fresh"

	_ = newConsumer(cfg, newFakeReader(0), func(context.Context, Message) error { return nil })

	for _, result := range []string{resultHandled, resultRetried, resultDeadLettered} {
		if got := testutil.CollectAndCount(consumed, "ticketwave_kafka_consumer_messages_total"); got == 0 {
			t.Fatal("no series exist for a brand-new consumer")
		}
		if v := testutil.ToFloat64(consumed.WithLabelValues("orders", "m-fresh", result)); v != 0 {
			t.Errorf("%s starts at %v, want 0", result, v)
		}
	}
}
