package handler

import (
	"context"
	"errors"
	"strings"
	"testing"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/services/notification/internal/email"
)

type fakeSender struct {
	sent []email.Email
	err  error
}

func (f *fakeSender) Send(_ context.Context, e email.Email) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, e)
	return nil
}

type fakeStore struct {
	seen    map[string]bool
	seenErr error
	markErr error
}

func newFakeStore() *fakeStore { return &fakeStore{seen: map[string]bool{}} }

func (s *fakeStore) Seen(_ context.Context, id string) (bool, error) {
	if s.seenErr != nil {
		return false, s.seenErr
	}
	return s.seen[id], nil
}

func (s *fakeStore) Mark(_ context.Context, id string) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.seen[id] = true
	return nil
}

func event(typ eventsv1.OrderEventType) *eventsv1.OrderEvent {
	return &eventsv1.OrderEvent{
		MessageId:   "msg-1",
		Type:        typ,
		OrderId:     "order-1",
		UserId:      "user-1",
		EventId:     "event-1",
		SeatIds:     []string{"seat-a", "seat-b"},
		AmountCents: 5000,
	}
}

func message(t *testing.T, ev *eventsv1.OrderEvent) kafka.Message {
	t.Helper()
	b, err := events.EncodeOrderEvent(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return kafka.Message{Value: b}
}

func TestHandle_ConfirmedEmailsTheCustomerOnce(t *testing.T) {
	sender, store := &fakeSender{}, newFakeStore()
	h := New(sender, store)

	err := h.Handle(context.Background(), message(t, event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sent %d emails, want 1", len(sender.sent))
	}
	mail := sender.sent[0]
	if mail.To != "user-1" {
		t.Errorf("To = %q, want the order's user", mail.To)
	}
	if !strings.Contains(mail.Subject, "confirmed") {
		t.Errorf("Subject = %q, want it to say confirmed", mail.Subject)
	}
	for _, want := range []string{"order-1", "2 seat(s)", "50.00"} {
		if !strings.Contains(mail.Body, want) {
			t.Errorf("Body = %q, want it to contain %q", mail.Body, want)
		}
	}
	if !store.seen["msg-1"] {
		t.Error("the event was not recorded as handled")
	}
}

func TestHandle_FailedEmailExplainsTheReason(t *testing.T) {
	cases := map[eventsv1.OrderFailureReason]string{
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED:  "payment was declined",
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE: "seats are no longer available",
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_UNSPECIFIED:       "unexpected problem",
	}
	for reason, want := range cases {
		t.Run(reason.String(), func(t *testing.T) {
			sender := &fakeSender{}
			ev := event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED)
			ev.FailureReason = reason

			if err := New(sender, newFakeStore()).Handle(context.Background(), message(t, ev)); err != nil {
				t.Fatalf("Handle: %v", err)
			}

			if len(sender.sent) != 1 || !strings.Contains(sender.sent[0].Body, want) {
				t.Fatalf("sent %v, want one email whose body contains %q", sender.sent, want)
			}
		})
	}
}

func TestHandle_CreatedSendsNothing(t *testing.T) {
	sender, store := &fakeSender{}, newFakeStore()

	err := New(sender, store).Handle(context.Background(), message(t, event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CREATED)))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(sender.sent) != 0 || len(store.seen) != 0 {
		t.Errorf("sent %d emails and recorded %d events, want neither for ORDER_CREATED", len(sender.sent), len(store.seen))
	}
}

func TestHandle_SameMessageTwiceSendsOneEmail(t *testing.T) {
	sender := &fakeSender{}
	h := New(sender, newFakeStore())
	msg := message(t, event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED))

	for i := 0; i < 2; i++ {
		if err := h.Handle(context.Background(), msg); err != nil {
			t.Fatalf("Handle #%d: %v", i+1, err)
		}
	}

	if len(sender.sent) != 1 {
		t.Errorf("sent %d emails for a redelivered message, want 1", len(sender.sent))
	}
}

func TestHandle_MalformedMessageIsPermanent(t *testing.T) {
	err := New(&fakeSender{}, newFakeStore()).Handle(context.Background(), kafka.Message{Value: []byte("not json")})

	if err == nil || !kafka.IsPermanent(err) {
		t.Fatalf("Handle returned %v, want a permanent error so the message is dead-lettered without retries", err)
	}
}

func TestHandle_MissingMessageIDIsPermanent(t *testing.T) {
	ev := event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)
	ev.MessageId = ""

	err := New(&fakeSender{}, newFakeStore()).Handle(context.Background(), message(t, ev))

	if err == nil || !kafka.IsPermanent(err) {
		t.Fatalf("Handle returned %v, want a permanent error: without a message_id it can never be deduplicated", err)
	}
}

func TestHandle_SendFailureIsRetryableAndNotRecorded(t *testing.T) {
	store := newFakeStore()
	h := New(&fakeSender{err: errors.New("smtp down")}, store)

	err := h.Handle(context.Background(), message(t, event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)))

	if err == nil || kafka.IsPermanent(err) {
		t.Fatalf("Handle returned %v, want an ordinary error so the consumer retries", err)
	}
	if store.seen["msg-1"] {
		t.Error("an event whose email failed was recorded as handled, so its retry would be skipped")
	}
}

func TestHandle_DedupStoreDownBlocksTheSend(t *testing.T) {
	sender := &fakeSender{}
	store := newFakeStore()
	store.seenErr = errors.New("redis down")

	err := New(sender, store).Handle(context.Background(), message(t, event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)))

	if err == nil {
		t.Fatal("Handle returned nil while the duplicate check was failing, want an error")
	}
	if len(sender.sent) != 0 {
		t.Error("an email was sent without being able to check for a duplicate")
	}
}

func TestHandle_FailureToRecordDoesNotRetryAnEmailAlreadySent(t *testing.T) {
	sender := &fakeSender{}
	store := newFakeStore()
	store.markErr = errors.New("redis down")

	err := New(sender, store).Handle(context.Background(), message(t, event(eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED)))

	if err != nil {
		t.Fatalf("Handle returned %v, want nil: a retry would resend an email that already went out", err)
	}
	if len(sender.sent) != 1 {
		t.Errorf("sent %d emails, want 1", len(sender.sent))
	}
}

func TestMoney(t *testing.T) {
	cases := map[int64]string{0: "0.00", 5: "0.05", 100: "1.00", 5000: "50.00", 12345: "123.45"}
	for cents, want := range cases {
		if got := money(cents); got != want {
			t.Errorf("money(%d) = %q, want %q", cents, got, want)
		}
	}
}
