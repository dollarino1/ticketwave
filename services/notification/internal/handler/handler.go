// Package handler turns order events into customer emails.
package handler

import (
	"context"
	"errors"
	"fmt"
	"log"

	eventsv1 "github.com/dollarino1/ticketwave/gen/ticketwave/events/v1"
	"github.com/dollarino1/ticketwave/pkg/events"
	"github.com/dollarino1/ticketwave/pkg/kafka"
	"github.com/dollarino1/ticketwave/services/notification/internal/dedup"
	"github.com/dollarino1/ticketwave/services/notification/internal/email"
)

type Handler struct {
	sender email.Sender
	sent   dedup.Store
}

func New(sender email.Sender, sent dedup.Store) *Handler {
	return &Handler{sender: sender, sent: sent}
}

// Handle processes one order event. It is safe to call twice with the same
// message: the second call sends nothing.
//
// Errors are classified for the consumer: a message that can never be handled
// is wrapped with kafka.Permanent and goes straight to the dead-letter topic,
// while anything else is returned as is and retried.
func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeOrderEvent(msg.Value)
	if err != nil {
		return kafka.Permanent(err)
	}
	if ev.GetMessageId() == "" {
		return kafka.Permanent(errors.New("event has no message_id, so it cannot be deduplicated"))
	}

	mail, ok := compose(ev)
	if !ok {
		return nil // e.g. ORDER_CREATED: nothing to tell the customer yet
	}

	seen, err := h.sent.Seen(ctx, ev.GetMessageId())
	if err != nil {
		return fmt.Errorf("check duplicate: %w", err)
	}
	if seen {
		log.Printf("skipping duplicate event %s (%s)", ev.GetMessageId(), events.EventTypeName(ev.GetType()))
		return nil
	}

	if err := h.sender.Send(ctx, mail); err != nil {
		return fmt.Errorf("send email: %w", err)
	}

	// The email is out. If remembering that fails we must not return an error:
	// the retry would send the email again on every attempt. Worst case, a
	// redelivery of this one message sends a duplicate, which beats a lost email.
	if err := h.sent.Mark(ctx, ev.GetMessageId()); err != nil {
		log.Printf("email for event %s was sent but could not be recorded, a redelivery may repeat it: %v",
			ev.GetMessageId(), err)
	}
	return nil
}

func compose(ev *eventsv1.OrderEvent) (email.Email, bool) {
	switch ev.GetType() {
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_CONFIRMED:
		return email.Email{
			To:      ev.GetUserId(),
			Subject: "Your tickets are confirmed",
			Body: fmt.Sprintf("Order %s is confirmed: %d seat(s), %s charged.",
				ev.GetOrderId(), len(ev.GetSeatIds()), money(ev.GetAmountCents())),
		}, true
	case eventsv1.OrderEventType_ORDER_EVENT_TYPE_FAILED:
		return email.Email{
			To:      ev.GetUserId(),
			Subject: "We couldn't complete your order",
			Body: fmt.Sprintf("Order %s was not completed because %s. %s",
				ev.GetOrderId(), reasonText(ev.GetFailureReason()), chargeNote(ev.GetFailureReason())),
		}, true
	default:
		return email.Email{}, false
	}
}

func reasonText(r eventsv1.OrderFailureReason) string {
	switch r {
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE:
		return "the seats are no longer available"
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED:
		return "your payment was declined"
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SERVICE_UNAVAILABLE:
		return "one of our services was temporarily unavailable"
	default:
		return "of an unexpected problem"
	}
}

// chargeNote must never promise more than we know. After a refusal we know
// nothing was charged. After a breakdown we do not: the payment may have gone
// through just before the failure, so the customer is told what to do instead.
func chargeNote(r eventsv1.OrderFailureReason) string {
	switch r {
	case eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_SEATS_UNAVAILABLE,
		eventsv1.OrderFailureReason_ORDER_FAILURE_REASON_PAYMENT_DECLINED:
		return "You have not been charged."
	default:
		return "Please try again. If you see a charge for this order, contact support and we will refund it."
	}
}

// money formats cents as a decimal amount without ever touching floating point.
func money(cents int64) string {
	return fmt.Sprintf("%d.%02d", cents/100, cents%100)
}
