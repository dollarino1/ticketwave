// Package kafka wraps segmentio/kafka-go with the two behaviours this project
// needs to get right: key-based partitioning on the way in, and batched
// at-least-once processing with a dead-letter topic on the way out.
package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

// Message is this package's view of a Kafka record. Keeping it separate from
// kafka-go's type means callers, and their tests, never import the driver.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// Publisher is the seam callers depend on, so tests can swap in a fake.
type Publisher interface {
	Publish(ctx context.Context, msgs ...Message) error
}

// toKafka deliberately leaves Topic unset: the Writer owns the topic, and
// kafka-go rejects a message that names one when the Writer already does.
func toKafka(m Message) kafkago.Message {
	headers := make([]kafkago.Header, 0, len(m.Headers))
	for k, v := range m.Headers {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return kafkago.Message{Key: m.Key, Value: m.Value, Headers: headers}
}

func fromKafka(m kafkago.Message) Message {
	headers := make(map[string]string, len(m.Headers))
	for _, h := range m.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   headers,
	}
}
