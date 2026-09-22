// Package hub fans seat updates out to the browsers watching each event.
package hub

import (
	"errors"
	"sync"
)

// Update is what a browser is told: these seats of the watched event are now in
// this state. It is the JSON the stream sends.
type Update struct {
	SeatIDs []string `json:"seat_ids"`
	State   string   `json:"state"`
}

// ErrFull means the hub is at its subscriber limit.
var ErrFull = errors.New("hub: too many subscribers")

// subscriberBuffer is how many updates may queue for one browser. A browser that
// falls further behind than this is cut off (see Publish).
const subscriberBuffer = 64

// Hub keeps track of who is watching which event. It is safe for concurrent use.
type Hub struct {
	mu     sync.Mutex
	subs   map[string]map[*Subscription]struct{} // event ID -> watchers
	count  int
	max    int
	closed bool
}

// New returns a hub that accepts at most max subscribers in total. Each one is a
// held-open connection and a goroutine, so an unbounded hub is a way to exhaust
// the process's memory or file descriptors.
func New(max int) *Hub {
	return &Hub{subs: make(map[string]map[*Subscription]struct{}), max: max}
}

// Subscription is one browser's view of one event. Read updates from C. C is
// closed when the subscription ends for any reason, including being cut off for
// reading too slowly, so `for u := range sub.C` always terminates.
type Subscription struct {
	C       <-chan Update
	c       chan Update
	hub     *Hub
	eventID string
}

// Subscribe starts watching eventID. Call Close when done.
func (h *Hub) Subscribe(eventID string) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.count >= h.max {
		refusedFull.Inc()
		return nil, ErrFull
	}
	c := make(chan Update, subscriberBuffer)
	s := &Subscription{C: c, c: c, hub: h, eventID: eventID}
	if h.subs[eventID] == nil {
		h.subs[eventID] = make(map[*Subscription]struct{})
	}
	h.subs[eventID][s] = struct{}{}
	h.count++
	subscribersGauge.Inc()
	return s, nil
}

// Close stops the subscription. It is safe to call more than once.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	s.hub.remove(s)
}

// remove must be called with h.mu held. Removal and closing the channel happen
// together, under the lock, so Publish can never send on a closed channel.
func (h *Hub) remove(s *Subscription) {
	watchers, ok := h.subs[s.eventID]
	if !ok {
		return
	}
	if _, ok := watchers[s]; !ok {
		return // already removed
	}
	delete(watchers, s)
	if len(watchers) == 0 {
		delete(h.subs, s.eventID)
	}
	h.count--
	subscribersGauge.Dec()
	close(s.c)
}

// Publish delivers u to everyone watching eventID, without ever blocking.
//
// One slow browser must not hold up the others, and Publish runs on the Kafka
// reading goroutine, so it cannot wait for anyone. If a subscriber's buffer is
// full it is removed and its channel closed instead. Its stream then ends, the
// browser reconnects, and it refetches the whole seat list, so nothing is lost:
// it just stops being incremental for that client.
func (h *Hub) Publish(eventID string, u Update) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs[eventID] {
		select {
		case s.c <- u:
			updatesSent.Inc()
		default:
			droppedSlow.Inc()
			h.remove(s) // deleting from a map during range is allowed in Go
		}
	}
}

// Close ends every subscription and refuses new ones. It lets a shutting-down
// server finish, since an HTTP server waits for open streams and they would
// otherwise never end.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, watchers := range h.subs {
		for s := range watchers {
			h.remove(s)
		}
	}
}

// Subscribers reports how many subscriptions are open.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}
