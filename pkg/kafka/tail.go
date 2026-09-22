package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// TailConfig describes a topic to follow from its newest message onwards.
type TailConfig struct {
	Brokers []string
	Topic   string
	// Partition to read. Tail follows exactly one, so the topic should have one.
	Partition int
}

// tailReader is the slice of kafka-go's Reader that Tail uses, so the loop can be
// tested without a broker.
type tailReader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// Tail calls handle for every message that arrives after it starts, until ctx is
// cancelled (which returns nil).
//
// This is the opposite of Consumer. A consumer group SPLITS a topic between its
// members, so each message reaches one of them: right for work to be done once.
// Tail is a broadcast: it reads without a group and commits nothing, so every
// process running it sees every message. That is what pushing live updates to
// browsers needs, because a browser may be connected to any instance. The price is
// no memory: a process that is down misses what was sent meanwhile, so callers
// must be able to resync from another source (here, the seat list) after a gap.
//
// handle runs on the reading goroutine, so it must return quickly; a slow handler
// delays every message behind it.
func Tail(ctx context.Context, cfg TailConfig, handle func(Message)) error {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" {
		return errors.New("kafka tail: brokers and topic are required")
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   cfg.Brokers,
		Topic:     cfg.Topic,
		Partition: cfg.Partition,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   250 * time.Millisecond,
	})
	// Without a group there is no committed offset to start from, so ask for the end.
	if err := r.SetOffset(kafkago.LastOffset); err != nil {
		_ = r.Close()
		return fmt.Errorf("kafka tail: seek to newest offset: %w", err)
	}
	return runTail(ctx, r, handle)
}

func runTail(ctx context.Context, r tailReader, handle func(Message)) error {
	defer func() { _ = r.Close() }()
	for {
		m, err := r.ReadMessage(ctx)
		switch {
		case err == nil:
			handle(fromKafka(m))
		case ctx.Err() != nil:
			return nil // shutting down
		default:
			// kafka-go reconnects by itself, so a returned error means the reader
			// is finished (closed) or the request is invalid; neither will heal.
			return fmt.Errorf("kafka tail: read: %w", err)
		}
	}
}
