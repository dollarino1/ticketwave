//go:build integration

package dedup

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Run with REDIS_ADDR set, e.g. REDIS_ADDR=localhost:6379 go test -tags integration ./services/notification/...
func TestRedis_MarksAndRemembers(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedis(client)

	ctx := context.Background()
	id := uuid.NewString() // unique per run, so reruns and real data never collide
	t.Cleanup(func() { client.Del(ctx, key(id)) })

	seen, err := store.Seen(ctx, id)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatal("a never-marked ID was reported as seen")
	}

	if err := store.Mark(ctx, id); err != nil {
		t.Fatalf("Mark: %v", err)
	}

	seen, err = store.Seen(ctx, id)
	if err != nil {
		t.Fatalf("Seen after Mark: %v", err)
	}
	if !seen {
		t.Error("a marked ID was not reported as seen")
	}

	ttl, err := client.TTL(ctx, key(id)).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		t.Errorf("TTL = %v, want a positive expiry of at most 24h so memory cannot grow forever", ttl)
	}
}
