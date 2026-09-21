// Package dedup remembers which events have already produced an email.
//
// Kafka delivery is at-least-once, so the same event can arrive twice, for
// example after a crash between handling a batch and committing its offsets.
// Without this, a redelivered "order confirmed" would email the customer again.
package dedup

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Store records handled message IDs.
type Store interface {
	Seen(ctx context.Context, messageID string) (bool, error)
	Mark(ctx context.Context, messageID string) error
}

// Redis keeps the IDs for a day, which is far longer than any redelivery window.
type Redis struct {
	client *goredis.Client
	ttl    time.Duration
}

func NewRedis(client *goredis.Client) *Redis {
	return &Redis{client: client, ttl: 24 * time.Hour}
}

func key(messageID string) string {
	return "notification:sent:" + messageID
}

func (r *Redis) Seen(ctx context.Context, messageID string) (bool, error) {
	n, err := r.client.Exists(ctx, key(messageID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}
	return n > 0, nil
}

func (r *Redis) Mark(ctx context.Context, messageID string) error {
	if err := r.client.Set(ctx, key(messageID), "1", r.ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
