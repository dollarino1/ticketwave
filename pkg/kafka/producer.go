package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer publishes to one topic. Publish is synchronous: when it returns nil
// the broker has acknowledged the write on every in-sync replica.
type Producer struct {
	w *kafkago.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{w: &kafkago.Writer{
		Addr:  kafkago.TCP(brokers...),
		Topic: topic,
		// Murmur2 is the Java client's default partitioner. kafka-go's default
		// balancer ignores the key entirely, which would silently break the
		// "all events of one order land on one partition, in order" guarantee.
		Balancer: &kafkago.Murmur2Balancer{},
		// A publish that returned nil must be durable, so wait for all replicas.
		RequiredAcks: kafkago.RequireAll,
		// The default batch timeout is one second, which would add a second of
		// latency to every synchronous publish.
		BatchTimeout: 10 * time.Millisecond,
		// Topics are created explicitly (see docker-compose), so a typo'd topic
		// name fails loudly instead of creating a new empty topic.
		AllowAutoTopicCreation: false,
	}}
}

const (
	// A freshly created topic can answer UnknownTopicOrPartition for a few
	// seconds while its metadata propagates, longer under load. These attempts,
	// with linear backoff, cover roughly 10s worst case.
	unknownTopicAttempts = 8
	unknownTopicBackoff  = 250 * time.Millisecond
)

func (p *Producer) Publish(ctx context.Context, msgs ...Message) error {
	out := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		out[i] = toKafka(m)
	}
	err := retryUnknownTopic(ctx, unknownTopicAttempts, unknownTopicBackoff, func() error {
		return p.w.WriteMessages(ctx, out...)
	})
	if err != nil {
		return fmt.Errorf("write %d messages to %s: %w", len(msgs), p.w.Topic, err)
	}
	return nil
}

// retryUnknownTopic retries a write that Kafka rejected with UnknownTopicOrPartition.
// The broker answers that way while a topic's metadata is still propagating, for
// example right after the topic was created or a broker restarted, and it clears
// within moments. Kafka's own Java client treats it as retriable; kafka-go does not
// when topic auto-creation is off. Any other error is returned at once.
//
// Retrying is safe because this error rejects the whole write: no message was
// stored, so nothing can be duplicated.
func retryUnknownTopic(ctx context.Context, attempts int, backoff time.Duration, write func() error) error {
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = write(); err == nil || !isUnknownTopic(err) {
			return err
		}
		if attempt == attempts {
			break
		}
		t := time.NewTimer(backoff * time.Duration(attempt))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return err
		}
	}
	return err
}

// isUnknownTopic reports whether err means "the topic is not known (yet)" for
// every message in the write.
func isUnknownTopic(err error) bool {
	var perMessage kafkago.WriteErrors
	if errors.As(err, &perMessage) {
		for _, e := range perMessage {
			if e != nil && !errors.Is(e, kafkago.UnknownTopicOrPartition) {
				return false
			}
		}
		return true
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition)
}

func (p *Producer) Close() error {
	return p.w.Close()
}
