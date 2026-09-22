// Package ratelimit limits how often something may happen, using a counter in
// Redis so every gateway replica shares one view of who has done what.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Result describes one attempt.
type Result struct {
	Allowed   bool
	Remaining int           // attempts left in the current window; 0 once blocked
	ResetIn   time.Duration // how long until the window ends and the counter starts over
}

// allowScript counts an attempt and reports the count and the time left.
//
// INCR and the expiry are in one script because Redis runs a script atomically.
// Done as two separate commands, a crash between them would leave a counter with
// no expiry: a client that hit the limit once would then be blocked forever.
//
// The PTTL check also repairs a counter that somehow has no expiry.
const allowScript = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  ttl = tonumber(ARGV[1])
end
return {count, ttl}
`

// Limiter is a fixed-window rate limiter: at most `limit` attempts per `window`
// for each key. Its known weakness is the window edge: a client can spend a full
// allowance at the end of one window and another at the start of the next, so
// up to twice the limit can pass in a short burst. That is acceptable for
// protecting login and the API from abuse; a sliding window or token bucket
// would smooth it out at the price of more Redis work per request.
type Limiter struct {
	client *goredis.Client
	script *goredis.Script
}

func New(client *goredis.Client) *Limiter {
	return &Limiter{client: client, script: goredis.NewScript(allowScript)}
}

// Allow records an attempt for key and reports whether it is within the limit.
// Different keys never affect one another.
func (l *Limiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	raw, err := l.script.Run(ctx, l.client, []string{"ratelimit:" + key}, window.Milliseconds()).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("rate limit script: %w", err)
	}
	if len(raw) != 2 {
		return Result{}, fmt.Errorf("rate limit script returned %d values, want 2", len(raw))
	}
	count, okCount := raw[0].(int64)
	ttlMillis, okTTL := raw[1].(int64)
	if !okCount || !okTTL {
		// Reporting "count 0" here would read as "allowed" and quietly disable the
		// limiter, so an unexpected reply is an error, never a free pass.
		return Result{}, fmt.Errorf("rate limit script returned %T and %T, want two integers", raw[0], raw[1])
	}

	remaining := limit - int(count)
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:   int(count) <= limit,
		Remaining: remaining,
		ResetIn:   time.Duration(ttlMillis) * time.Millisecond,
	}, nil
}
