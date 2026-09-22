//go:build integration

package ratelimit

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Run with REDIS_ADDR set, e.g. REDIS_ADDR=localhost:6379 go test -tags integration ./pkg/ratelimit/
func newLimiter(t *testing.T) (*Limiter, *goredis.Client) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return New(client), client
}

// uniqueKey keeps tests, reruns and real traffic from ever sharing a counter.
func uniqueKey(t *testing.T, client *goredis.Client) string {
	t.Helper()
	key := "test-" + uuid.NewString()
	t.Cleanup(func() { client.Del(context.Background(), "ratelimit:"+key) })
	return key
}

func TestAllow_PermitsUpToTheLimitThenBlocks(t *testing.T) {
	l, client := newLimiter(t)
	key := uniqueKey(t, client)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		res, err := l.Allow(ctx, key, 3, time.Minute)
		if err != nil {
			t.Fatalf("Allow #%d: %v", i, err)
		}
		if !res.Allowed || res.Remaining != 3-i {
			t.Errorf("attempt %d: allowed=%v remaining=%d, want allowed with %d left", i, res.Allowed, res.Remaining, 3-i)
		}
	}

	res, err := l.Allow(ctx, key, 3, time.Minute)
	if err != nil {
		t.Fatalf("Allow #4: %v", err)
	}
	if res.Allowed || res.Remaining != 0 {
		t.Errorf("attempt 4: allowed=%v remaining=%d, want blocked with 0 left", res.Allowed, res.Remaining)
	}
	if res.ResetIn <= 0 || res.ResetIn > time.Minute {
		t.Errorf("ResetIn = %v, want between 0 and the 1m window so a client knows when to retry", res.ResetIn)
	}
}

func TestAllow_TheWindowResetsWhenItExpires(t *testing.T) {
	l, client := newLimiter(t)
	key := uniqueKey(t, client)
	ctx := context.Background()
	const window = 400 * time.Millisecond

	if _, err := l.Allow(ctx, key, 1, window); err != nil {
		t.Fatal(err)
	}
	if res, _ := l.Allow(ctx, key, 1, window); res.Allowed {
		t.Fatal("the second attempt inside the window was allowed")
	}

	time.Sleep(window + 200*time.Millisecond)

	res, err := l.Allow(ctx, key, 1, window)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Allowed {
		t.Error("still blocked after the window expired: the counter never reset")
	}
}

func TestAllow_KeysAreIndependent(t *testing.T) {
	l, client := newLimiter(t)
	a, b := uniqueKey(t, client), uniqueKey(t, client)
	ctx := context.Background()

	if _, err := l.Allow(ctx, a, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if res, _ := l.Allow(ctx, a, 1, time.Minute); res.Allowed {
		t.Fatal("key a should now be blocked")
	}

	res, err := l.Allow(ctx, b, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Allowed {
		t.Error("key b was blocked because of key a's traffic")
	}
}

// A counter must always carry an expiry. One without it would make a single
// burst of requests a permanent ban.
func TestAllow_EveryCounterExpires(t *testing.T) {
	l, client := newLimiter(t)
	key := uniqueKey(t, client)

	if _, err := l.Allow(context.Background(), key, 5, time.Minute); err != nil {
		t.Fatal(err)
	}

	ttl, err := client.PTTL(context.Background(), "ratelimit:"+key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Errorf("counter TTL = %v, want a positive expiry of at most 1m", ttl)
	}
}

func TestAllow_RepairsACounterThatLostItsExpiry(t *testing.T) {
	l, client := newLimiter(t)
	key := uniqueKey(t, client)
	ctx := context.Background()
	// Simulate the failure the script guards against: a counter with no TTL.
	if err := client.Set(ctx, "ratelimit:"+key, 7, 0).Err(); err != nil {
		t.Fatal(err)
	}

	res, err := l.Allow(ctx, key, 5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if res.Allowed {
		t.Error("a counter already over the limit was allowed")
	}
	ttl, _ := client.PTTL(ctx, "ratelimit:"+key).Result()
	if ttl <= 0 {
		t.Errorf("the immortal counter was not given an expiry (TTL %v), so the ban would last forever", ttl)
	}
}

func TestAllow_ConcurrentAttemptsNeverExceedTheLimit(t *testing.T) {
	l, client := newLimiter(t)
	key := uniqueKey(t, client)

	const limit, attempts = 10, 100
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Go(func() {
			res, err := l.Allow(context.Background(), key, limit, time.Minute)
			if err != nil {
				t.Errorf("Allow: %v", err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()

	if got := allowed.Load(); got != limit {
		t.Errorf("%d of %d simultaneous attempts were allowed, want exactly %d", got, attempts, limit)
	}
}
