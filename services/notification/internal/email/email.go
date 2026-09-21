// Package email is the outbound edge of notification-svc. Only a logging sender
// exists: the project deliberately stops short of a real mail provider.
package email

import (
	"context"
	"log"
)

// Email is addressed by user ID because order events carry no address; a real
// sender would look the address up from auth-svc first.
type Email struct {
	To      string
	Subject string
	Body    string
}

type Sender interface {
	Send(ctx context.Context, e Email) error
}

// LogSender "delivers" mail by logging it.
type LogSender struct{}

func (LogSender) Send(_ context.Context, e Email) error {
	log.Printf("email sent to user %s: %s | %s", e.To, e.Subject, e.Body)
	return nil
}
