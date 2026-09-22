package kafka

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// scripted returns its messages in order, then the given error (or blocks until
// ctx is cancelled when finally is nil).
type scripted struct {
	msgs    []kafkago.Message
	finally error
	closed  bool
}

func (s *scripted) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	if len(s.msgs) > 0 {
		m := s.msgs[0]
		s.msgs = s.msgs[1:]
		return m, nil
	}
	if s.finally != nil {
		return kafkago.Message{}, s.finally
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (s *scripted) Close() error {
	s.closed = true
	return nil
}

func TestRunTail_DeliversEveryMessageInOrderAndStopsCleanlyOnCancel(t *testing.T) {
	r := &scripted{msgs: []kafkago.Message{
		{Key: []byte("a"), Value: []byte("1"), Headers: []kafkago.Header{{Key: "h", Value: []byte("v")}}},
		{Key: []byte("b"), Value: []byte("2")},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	var got []Message
	done := make(chan error, 1)
	go func() {
		done <- runTail(ctx, r, func(m Message) {
			got = append(got, m)
			if len(got) == 2 {
				cancel()
			}
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runTail returned %v on a normal shutdown, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runTail did not stop after its context was cancelled")
	}
	if len(got) != 2 || string(got[0].Value) != "1" || string(got[1].Value) != "2" {
		t.Fatalf("delivered %v, want both messages in order", got)
	}
	if got[0].Headers["h"] != "v" {
		t.Errorf("headers = %v, want them carried through", got[0].Headers)
	}
	if !r.closed {
		t.Error("the reader was not closed, leaking its connections")
	}
}

func TestRunTail_AReaderThatIsFinishedIsAnErrorNotASilentStop(t *testing.T) {
	r := &scripted{finally: io.EOF}

	err := runTail(context.Background(), r, func(Message) {})

	if !errors.Is(err, io.EOF) {
		t.Errorf("runTail = %v, want the reader's error, so the caller can restart or crash loudly", err)
	}
	if !r.closed {
		t.Error("the reader was not closed")
	}
}

func TestTail_RequiresBrokersAndTopic(t *testing.T) {
	for _, cfg := range []TailConfig{{}, {Brokers: []string{"x:1"}}, {Topic: "t"}} {
		if err := Tail(context.Background(), cfg, func(Message) {}); err == nil {
			t.Errorf("Tail(%+v) accepted an incomplete config", cfg)
		}
	}
}
