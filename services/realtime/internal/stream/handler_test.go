package stream

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dollarino1/ticketwave/services/realtime/internal/hub"
)

const eventID = "55555555-5555-4555-8555-555555555555"

// start runs the handler on a real HTTP server, because streaming (flushing,
// disconnects) is exactly what httptest.ResponseRecorder cannot imitate.
func start(t *testing.T, h *hub.Hub, heartbeat time.Duration) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /api/events/{id}/stream", New(h, slog.New(slog.DiscardHandler), heartbeat))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// reply is the part of the HTTP response the tests look at. open closes the real
// body itself when the test ends, so callers cannot forget to.
type reply struct {
	StatusCode int
	Header     http.Header
	Body       io.Reader
}

func open(t *testing.T, srv *httptest.Server, id string) (reply, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events/"+id+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { cancel(); _ = resp.Body.Close() })
	return reply{StatusCode: resp.StatusCode, Header: resp.Header, Body: resp.Body}, cancel
}

// lines feeds each line of the stream to a channel, so tests can wait with a timeout.
func lines(r io.Reader) <-chan string {
	out := make(chan string, 64)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			out <- sc.Text()
		}
	}()
	return out
}

func next(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case l, ok := <-ch:
		if !ok {
			t.Fatal("stream ended, want another line")
		}
		return l
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a line from the stream")
		return ""
	}
}

func waitSubscribers(t *testing.T, h *hub.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for h.Subscribers() != want {
		if time.Now().After(deadline) {
			t.Fatalf("Subscribers = %d, want %d", h.Subscribers(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStream_SendsHeadersThenUpdatesInSSEFormat(t *testing.T) {
	h := hub.New(10)
	resp, _ := open(t, start(t, h, time.Hour), eventID)

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", got)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no, or nginx buffers the stream", got)
	}

	ch := lines(resp.Body)
	if got := next(t, ch); got != "retry: 3000" {
		t.Errorf("first line = %q, want the reconnect delay", got)
	}
	next(t, ch) // ": connected"
	next(t, ch) // blank line ending the first block
	waitSubscribers(t, h, 1)

	h.Publish(eventID, hub.Update{SeatIDs: []string{"s1", "s2"}, State: "held"})

	if got := next(t, ch); got != "event: seats" {
		t.Errorf("line = %q, want the event name", got)
	}
	if got := next(t, ch); got != `data: {"seat_ids":["s1","s2"],"state":"held"}` {
		t.Errorf("data line = %q", got)
	}
	if got := next(t, ch); got != "" {
		t.Errorf("line = %q, want the blank line that ends an SSE message", got)
	}
}

func TestStream_OnlyReceivesItsOwnEvent(t *testing.T) {
	h := hub.New(10)
	resp, _ := open(t, start(t, h, time.Hour), eventID)
	ch := lines(resp.Body)
	next(t, ch)
	next(t, ch)
	next(t, ch)
	waitSubscribers(t, h, 1)

	h.Publish("66666666-6666-4666-8666-666666666666", hub.Update{SeatIDs: []string{"x"}, State: "sold"})
	h.Publish(eventID, hub.Update{SeatIDs: []string{"mine"}, State: "sold"})

	next(t, ch) // event: seats
	if got := next(t, ch); !strings.Contains(got, "mine") || strings.Contains(got, `"x"`) {
		t.Errorf("data = %q, want only this event's update", got)
	}
}

func TestStream_SendsHeartbeatsSoIdleConnectionsStayOpen(t *testing.T) {
	h := hub.New(10)
	resp, _ := open(t, start(t, h, 30*time.Millisecond), eventID)
	ch := lines(resp.Body)
	next(t, ch)
	next(t, ch)
	next(t, ch)

	if got := next(t, ch); got != ": ping" {
		t.Errorf("line = %q, want a ping comment", got)
	}
}

func TestStream_RejectsAMalformedEventIDWithoutSubscribing(t *testing.T) {
	h := hub.New(10)
	srv := start(t, h, time.Hour)

	for _, id := range []string{"not-a-uuid", "1", "55555555-5555-4555-8555-55555555555Z"} {
		resp, _ := open(t, srv, id)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("id %q: status = %d, want 400", id, resp.StatusCode)
		}
	}
	if h.Subscribers() != 0 {
		t.Errorf("a rejected request left %d subscriber(s)", h.Subscribers())
	}
}

func TestStream_Answers503WhenTheHubIsFull(t *testing.T) {
	h := hub.New(1)
	srv := start(t, h, time.Hour)
	open(t, srv, eventID)
	waitSubscribers(t, h, 1)

	resp, _ := open(t, srv, eventID)

	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Errorf("got %d with Retry-After %q, want 503 and a Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

// If the goroutine and hub slot of a departed browser were not released, every
// page reload would leak one until the process fell over.
func TestStream_ReleasesTheSlotWhenTheBrowserLeaves(t *testing.T) {
	h := hub.New(10)
	resp, cancel := open(t, start(t, h, time.Hour), eventID)
	ch := lines(resp.Body)
	next(t, ch)
	waitSubscribers(t, h, 1)

	cancel()

	waitSubscribers(t, h, 0)
}

func TestStream_EndsWhenTheHubShutsDown(t *testing.T) {
	h := hub.New(10)
	resp, _ := open(t, start(t, h, time.Hour), eventID)
	ch := lines(resp.Body)
	next(t, ch)
	next(t, ch)
	next(t, ch)
	waitSubscribers(t, h, 1)

	h.Close()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("received data after shutdown, want the stream to end")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the stream did not end when the hub closed, which would block graceful shutdown forever")
	}
}
