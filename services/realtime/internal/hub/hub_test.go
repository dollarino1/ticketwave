package hub

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func up(state string, seats ...string) Update { return Update{SeatIDs: seats, State: state} }

func recv(t *testing.T, s *Subscription) Update {
	t.Helper()
	select {
	case u, ok := <-s.C:
		if !ok {
			t.Fatal("subscription was closed, want an update")
		}
		return u
	case <-time.After(time.Second):
		t.Fatal("no update arrived")
		return Update{}
	}
}

func mustSubscribe(t *testing.T, h *Hub, event string) *Subscription {
	t.Helper()
	s, err := h.Subscribe(event)
	if err != nil {
		t.Fatalf("Subscribe(%s): %v", event, err)
	}
	return s
}

func TestPublish_ReachesEveryWatcherOfThatEventOnly(t *testing.T) {
	h := New(10)
	a1, a2 := mustSubscribe(t, h, "A"), mustSubscribe(t, h, "A")
	b := mustSubscribe(t, h, "B")

	h.Publish("A", up("held", "s1"))

	for _, s := range []*Subscription{a1, a2} {
		if got := recv(t, s); got.State != "held" || got.SeatIDs[0] != "s1" {
			t.Errorf("watcher of A got %+v", got)
		}
	}
	select {
	case u := <-b.C:
		t.Errorf("a watcher of B received A's update %+v", u)
	default:
	}
}

func TestPublish_PreservesOrder(t *testing.T) {
	h := New(10)
	s := mustSubscribe(t, h, "A")

	for i := 0; i < 10; i++ {
		h.Publish("A", up("sold", fmt.Sprint(i)))
	}

	for i := 0; i < 10; i++ {
		if got := recv(t, s).SeatIDs[0]; got != fmt.Sprint(i) {
			t.Fatalf("update %d arrived as %s", i, got)
		}
	}
}

func TestPublish_WithNobodyWatchingIsHarmless(t *testing.T) {
	New(10).Publish("nobody", up("held", "s1")) // must not panic or block
}

// One slow browser must not stall the rest, and must not be able to make the hub
// buffer without limit either.
func TestPublish_CutsOffASlowSubscriberAndKeepsTheFastOnes(t *testing.T) {
	h := New(10)
	slow := mustSubscribe(t, h, "A") // never reads
	fast := mustSubscribe(t, h, "A")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < subscriberBuffer*3; i++ {
			h.Publish("A", up("held", fmt.Sprint(i)))
			<-fast.C // the fast one keeps up
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on the slow subscriber")
	}

	// The slow one got the updates that fitted, then its channel was closed.
	n := 0
	for range slow.C {
		n++
	}
	if n != subscriberBuffer {
		t.Errorf("slow subscriber drained %d updates, want exactly its buffer of %d before the cut-off", n, subscriberBuffer)
	}
	if got := h.Subscribers(); got != 1 {
		t.Errorf("Subscribers = %d, want 1 (only the fast one left)", got)
	}
}

func TestClose_IsIdempotentAndFreesTheSlot(t *testing.T) {
	h := New(1)
	s := mustSubscribe(t, h, "A")

	s.Close()
	s.Close() // must not panic on a double close, or drive the count negative

	if got := h.Subscribers(); got != 0 {
		t.Errorf("Subscribers = %d after close, want 0", got)
	}
	mustSubscribe(t, h, "A") // the slot is reusable
	if _, ok := <-s.C; ok {
		t.Error("a closed subscription's channel is still open")
	}
}

func TestSubscribe_RefusesBeyondTheLimit(t *testing.T) {
	h := New(2)
	mustSubscribe(t, h, "A")
	mustSubscribe(t, h, "B")

	if _, err := h.Subscribe("C"); !errors.Is(err, ErrFull) {
		t.Errorf("third Subscribe = %v, want ErrFull", err)
	}
}

func TestHubClose_EndsEverySubscriptionAndRefusesNewOnes(t *testing.T) {
	h := New(10)
	a, b := mustSubscribe(t, h, "A"), mustSubscribe(t, h, "B")

	h.Close()

	for _, s := range []*Subscription{a, b} {
		if _, ok := <-s.C; ok {
			t.Error("a subscription survived hub.Close")
		}
	}
	if _, err := h.Subscribe("A"); err == nil {
		t.Error("Subscribe succeeded on a closed hub")
	}
	a.Close() // closing after the hub closed must be safe
}

// A send on a closed channel panics. Hammer every path that could cause one:
// publishing while subscribers come, go, and get cut off for being slow.
func TestConcurrentSubscribePublishClose_NeverPanics(t *testing.T) {
	h := New(1000)
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				h.Publish("A", up("held", "s"))
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s, err := h.Subscribe("A")
				if err != nil {
					continue
				}
				if i%2 == 0 { // half read a little, half never read and get cut off
					select {
					case <-s.C:
					default:
					}
				}
				s.Close()
			}
		}(i)
	}
	wg.Wait()

	if got := h.Subscribers(); got != 0 {
		t.Errorf("Subscribers = %d after everyone left, want 0 (a leak or a double count)", got)
	}
}
