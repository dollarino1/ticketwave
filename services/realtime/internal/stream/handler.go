// Package stream serves the live seat map over Server-Sent Events.
package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dollarino1/ticketwave/services/realtime/internal/hub"
	"github.com/google/uuid"
)

// Subscriber is the part of the hub the handler needs.
type Subscriber interface {
	Subscribe(eventID string) (*hub.Subscription, error)
}

type Handler struct {
	hub       Subscriber
	log       *slog.Logger
	heartbeat time.Duration
}

// New returns the handler for GET /api/events/{id}/stream.
//
// A comment line is sent every heartbeat interval. It keeps proxies and load
// balancers from closing a connection they think is idle, and it makes a dead
// client show up as a failed write, so its goroutine and hub slot are freed
// instead of lingering until TCP notices.
func New(h Subscriber, log *slog.Logger, heartbeat time.Duration) *Handler {
	if heartbeat <= 0 {
		heartbeat = 15 * time.Second
	}
	return &Handler{hub: h, log: log, heartbeat: heartbeat}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("id")
	if _, err := uuid.Parse(eventID); err != nil {
		http.Error(w, "invalid event id", http.StatusBadRequest)
		return
	}

	rc := http.NewResponseController(w)
	// The server's WriteTimeout would kill a stream that lives for minutes, so the
	// deadline is lifted for this response only. ErrNotSupported means there is no
	// deadline to lift, which is fine; a flush failure below is what matters.
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		h.log.Warn("could not lift the write deadline", slog.Any("error", err))
	}

	sub, err := h.hub.Subscribe(eventID)
	if err != nil {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "too many viewers, try again shortly", http.StatusServiceUnavailable)
		return
	}
	defer sub.Close()

	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Accel-Buffering", "no") // tells nginx not to hold the stream back in a buffer
	w.WriteHeader(http.StatusOK)

	// "retry" is how long the browser waits before reconnecting after a drop.
	if _, err := fmt.Fprint(w, "retry: 3000\n: connected\n\n"); err != nil || rc.Flush() != nil {
		return
	}

	ticker := time.NewTicker(h.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return // the browser left
		case u, ok := <-sub.C:
			if !ok {
				return // cut off for being slow, or the server is shutting down
			}
			data, err := json.Marshal(u)
			if err != nil {
				h.log.Error("could not encode a seat update", slog.Any("error", err))
				continue
			}
			if _, err := fmt.Fprintf(w, "event: seats\ndata: %s\n\n", data); err != nil || rc.Flush() != nil {
				return
			}
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}
