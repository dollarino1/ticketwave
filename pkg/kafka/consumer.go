package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

// Handler processes one message. Returning an error means "try again"; after
// MaxAttempts failures the message is parked on the dead-letter topic.
type Handler func(ctx context.Context, msg Message) error

var errPermanent = errors.New("permanent failure")

// Permanent marks a handler failure that retrying cannot fix, such as a message
// that does not parse. The consumer skips the remaining attempts and dead-letters
// the message immediately instead of burning retries on it.
func Permanent(err error) error {
	return fmt.Errorf("%w: %w", errPermanent, err)
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	return errors.Is(err, errPermanent)
}

type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string

	// DLQ receives messages whose handler kept failing. It is required:
	// silently dropping a poison message is worse than parking it.
	DLQ Publisher

	// Concurrency is how many messages of one batch are handled in parallel.
	// Use 1 when handling order matters. Default 1.
	Concurrency int
	// BatchSize caps a batch. Default 100.
	BatchSize int
	// BatchWait is how long to keep filling a batch after its first message.
	// Default 200ms.
	BatchWait time.Duration
	// MaxAttempts is how many times the handler runs before dead-lettering.
	// Default 3.
	MaxAttempts int
	// RetryBackoff is the base delay between attempts, multiplied by the
	// attempt number. Default 200ms.
	RetryBackoff time.Duration
}

func (c *ConsumerConfig) validate() error {
	if len(c.Brokers) == 0 || c.Topic == "" || c.GroupID == "" {
		return errors.New("kafka consumer: brokers, topic and group ID are required")
	}
	if c.DLQ == nil {
		return errors.New("kafka consumer: a dead-letter publisher is required")
	}
	return nil
}

func (c *ConsumerConfig) applyDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.BatchWait <= 0 {
		c.BatchWait = 200 * time.Millisecond
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
}

// reader is the slice of kafka-go's Reader that Consumer uses, so the batching,
// retry and commit logic can be tested without a broker.
type reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Consumer reads a topic as part of a consumer group. It fetches messages in
// batches, handles a batch (optionally in parallel), and only then commits the
// batch's offsets. Committing after the whole batch is what makes delivery
// at-least-once without gaps: a crash mid-batch redelivers the batch, it never
// skips a message. Handlers must therefore be idempotent.
type Consumer struct {
	cfg     ConsumerConfig
	r       reader
	handler Handler
}

func NewConsumer(cfg ConsumerConfig, h Handler) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
		// Applies only when the group has no committed offset yet: a brand-new
		// group reads the topic from the beginning instead of missing history.
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     250 * time.Millisecond,
		// No CommitInterval: commits are explicit and synchronous.
	})
	return newConsumer(cfg, r, h), nil
}

func newConsumer(cfg ConsumerConfig, r reader, h Handler) *Consumer {
	cfg.applyDefaults()
	initMetrics(cfg.Topic, cfg.GroupID)
	return &Consumer{cfg: cfg, r: r, handler: h}
}

// Run consumes until ctx is cancelled, which is a clean shutdown and returns
// nil. Any other return is a failure the caller should treat as fatal.
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		batch, err := c.fetchBatch(ctx)
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}
		if len(batch) == 0 {
			continue
		}

		if err := c.process(ctx, batch); err != nil {
			if ctx.Err() != nil {
				// Shutdown mid-batch: leave it uncommitted so it is redelivered.
				return nil
			}
			return err
		}

		// A shutdown that arrives right after processing must still commit the
		// finished batch, so the commit does not inherit ctx's cancellation.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = c.r.CommitMessages(commitCtx, batch...)
		cancel()
		if err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.r.Close()
}

// fetchBatch blocks for the first message, then keeps filling the batch until
// it is full or BatchWait has elapsed. It returns an empty batch on shutdown.
func (c *Consumer) fetchBatch(ctx context.Context) ([]kafkago.Message, error) {
	first, err := c.r.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, err
	}
	batch := []kafkago.Message{first}

	fillCtx, cancel := context.WithTimeout(ctx, c.cfg.BatchWait)
	defer cancel()
	for len(batch) < c.cfg.BatchSize {
		m, err := c.r.FetchMessage(fillCtx)
		if err != nil {
			if fillCtx.Err() != nil {
				break // window elapsed or shutting down: ship what we have
			}
			return nil, err
		}
		batch = append(batch, m)
	}
	return batch, nil
}

func (c *Consumer) process(ctx context.Context, batch []kafkago.Message) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.cfg.Concurrency)
	for _, m := range batch {
		g.Go(func() error { return c.handleOne(gctx, m) })
	}
	return g.Wait()
}

// handleOne runs the handler with retries. It returns nil once the message is
// either handled or safely parked on the DLQ, and an error only when neither
// happened (shutdown, or the DLQ itself is down), so the batch stays uncommitted.
func (c *Consumer) handleOne(ctx context.Context, km kafkago.Message) error {
	msg := fromKafka(km)

	var err error
	attempts := 0
	for attempts < c.cfg.MaxAttempts {
		attempts++
		if err = c.handler(ctx, msg); err == nil {
			c.count(msg.Topic, resultHandled)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsPermanent(err) {
			log.Printf("kafka consumer %s: %s[%d]@%d failed permanently, not retrying: %v",
				c.cfg.GroupID, msg.Topic, msg.Partition, msg.Offset, err)
			break
		}
		log.Printf("kafka consumer %s: %s[%d]@%d attempt %d/%d failed: %v",
			c.cfg.GroupID, msg.Topic, msg.Partition, msg.Offset, attempts, c.cfg.MaxAttempts, err)

		if attempts < c.cfg.MaxAttempts {
			c.count(msg.Topic, resultRetried)
			t := time.NewTimer(c.cfg.RetryBackoff * time.Duration(attempts))
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
	}
	return c.deadLetter(ctx, msg, attempts, err)
}

func (c *Consumer) deadLetter(ctx context.Context, msg Message, attempts int, cause error) error {
	headers := make(map[string]string, len(msg.Headers)+5)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	headers["dlq-error"] = cause.Error()
	headers["dlq-attempts"] = strconv.Itoa(attempts)
	headers["dlq-source-topic"] = msg.Topic
	headers["dlq-source-partition"] = strconv.Itoa(msg.Partition)
	headers["dlq-source-offset"] = strconv.FormatInt(msg.Offset, 10)

	dead := Message{Key: msg.Key, Value: msg.Value, Headers: headers}
	if err := c.cfg.DLQ.Publish(ctx, dead); err != nil {
		return fmt.Errorf("dead-letter %s[%d]@%d: %w", msg.Topic, msg.Partition, msg.Offset, err)
	}
	c.count(msg.Topic, resultDeadLettered)
	log.Printf("kafka consumer %s: %s[%d]@%d dead-lettered after %d attempt(s): %v",
		c.cfg.GroupID, msg.Topic, msg.Partition, msg.Offset, attempts, cause)
	return nil
}
