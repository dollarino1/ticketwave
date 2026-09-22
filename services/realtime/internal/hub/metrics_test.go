package hub

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The metrics are process-global and other tests in this package run hubs too, so
// every assertion is about how far a value MOVED during the test, never its level.

func TestMetrics_TheSubscriberGaugeFollowsSubscribeAndClose(t *testing.T) {
	before := testutil.ToFloat64(subscribersGauge)
	h := New(10)

	a, b := mustSubscribe(t, h, "A"), mustSubscribe(t, h, "B")
	if got := testutil.ToFloat64(subscribersGauge) - before; got != 2 {
		t.Errorf("gauge rose by %v after two subscribes, want 2", got)
	}

	a.Close()
	a.Close() // a double close must not decrement twice
	if got := testutil.ToFloat64(subscribersGauge) - before; got != 1 {
		t.Errorf("gauge is %v above where it started after one close, want 1", got)
	}

	h.Close()
	_ = b
	if got := testutil.ToFloat64(subscribersGauge) - before; got != 0 {
		t.Errorf("gauge is %v above where it started after the hub closed, want 0: it must not leak", got)
	}
}

func TestMetrics_CountUpdatesQueuedToEachBrowser(t *testing.T) {
	before := testutil.ToFloat64(updatesSent)
	h := New(10)
	mustSubscribe(t, h, "A")
	mustSubscribe(t, h, "A")
	mustSubscribe(t, h, "B")

	h.Publish("A", up("held", "s1"))

	if got := testutil.ToFloat64(updatesSent) - before; got != 2 {
		t.Errorf("counted %v queued updates, want 2 (two watchers of A, none for B)", got)
	}
}

func TestMetrics_CountSlowSubscribersThatAreCutOff(t *testing.T) {
	before := testutil.ToFloat64(droppedSlow)
	h := New(10)
	mustSubscribe(t, h, "A") // never reads

	for i := 0; i < subscriberBuffer+5; i++ {
		h.Publish("A", up("held", "s"))
	}

	if got := testutil.ToFloat64(droppedSlow) - before; got != 1 {
		t.Errorf("counted %v cut-offs, want exactly 1", got)
	}
}

func TestMetrics_CountRefusedSubscribers(t *testing.T) {
	before := testutil.ToFloat64(refusedFull)
	h := New(1)
	mustSubscribe(t, h, "A")

	_, _ = h.Subscribe("A")
	_, _ = h.Subscribe("A")

	if got := testutil.ToFloat64(refusedFull) - before; got != 2 {
		t.Errorf("counted %v refusals, want 2", got)
	}
}
