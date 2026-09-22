import { ApiError } from '../api/client'

// KeyKeeper decides which Idempotency-Key to send with a checkout.
//
// The rule that makes idempotency work: RETRYING must reuse the key, and doing
// something NEW must not.
//
//   - A retry is the same seats, sent again because the last try ended without an
//     answer. Reusing the key lets the server say "already done, here is the result"
//     instead of buying twice.
//   - A new attempt (different seats, or the previous one got a definite answer) needs
//     a fresh key. Reusing an old one would replay a stale result, for example the old
//     "card declined", forever.
//
// "Ended without an answer" is exactly: the connection dropped (status 0) or the
// gateway timed out (504). Any other response, even an error, IS an answer: the
// server decided, so the next click is a new attempt.
export class KeyKeeper {
  private current: { seats: string; key: string } | null = null

  // keyFor returns the key for a checkout of these seats: the pending one if this is
  // a retry of the same seats, otherwise a new one.
  keyFor(seatIDs: readonly string[]): string {
    // Order must not matter: [a, b] and [b, a] are the same purchase.
    const seats = [...seatIDs].sort().join(',')
    if (this.current?.seats !== seats) {
      this.current = { seats, key: crypto.randomUUID() }
    }
    return this.current.key
  }

  // settle records how the attempt ended. Pass undefined for success.
  settle(error?: unknown): void {
    if (isUnknownOutcome(error)) return // keep the key: the retry must reuse it
    this.current = null
  }
}

// isUnknownOutcome reports whether a failed request may or may not have taken effect,
// as opposed to the server having answered "no".
export function isUnknownOutcome(err: unknown): boolean {
  return err instanceof ApiError && (err.status === 0 || err.status === 504)
}
