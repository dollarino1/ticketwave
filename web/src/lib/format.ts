import { ApiError } from '../api/client'

const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })

// Money is integer cents everywhere on the wire. Floating-point dollars would round
// (0.1 + 0.2 !== 0.3), so conversion to dollars happens only here, at display time.
export function money(cents: number): string {
  return usd.format(cents / 100)
}

export function shortDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })
}

// What a person should read for each failure. The gateway's `code` is stable and
// safe to branch on; its `message` is written for developers, so anything a user is
// likely to hit gets its own wording here.
const messages: Record<string, string> = {
  network_error: 'Cannot reach the server. Check your connection and try again.',
  seats_unavailable: 'Someone got there first. Those seats are no longer available; pick different ones.',
  payment_declined: 'Your payment was declined. You were not charged and the seats were released.',
  service_unavailable: 'Something on our side is down. You were not charged. Please try again in a moment.',
  timeout: 'That took too long. Please try again.',
  rate_limited: 'Too many attempts. Wait a moment and try again.',
  already_exists: 'An account with that email already exists.',
  unauthenticated: 'Wrong email or password.',
}

export function describeError(err: unknown): string {
  if (err instanceof ApiError) return messages[err.code] ?? err.message
  return 'Something went wrong. Please try again.'
}
