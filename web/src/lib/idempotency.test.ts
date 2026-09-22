import { describe, expect, it, vi } from 'vitest'
import { ApiError } from '../api/client'
import { KeyKeeper } from './idempotency'

const err = (status: number, code = 'x') => new ApiError(status, code, 'msg')

describe('KeyKeeper', () => {
  it('gives a fresh, valid key for a first attempt', () => {
    const key = new KeyKeeper().keyFor(['a', 'b'])

    expect(key).toMatch(/^[A-Za-z0-9_-]{1,64}$/) // what the gateway accepts
  })

  it('reuses the key while the same seats are retried', () => {
    const keeper = new KeyKeeper()

    const first = keeper.keyFor(['a', 'b'])
    keeper.settle(err(0, 'network_error')) // no answer arrived
    const retry = keeper.keyFor(['a', 'b'])

    expect(retry).toBe(first)
  })

  it('treats the same seats in another order as the same purchase', () => {
    const keeper = new KeyKeeper()

    expect(keeper.keyFor(['b', 'a'])).toBe(keeper.keyFor(['a', 'b']))
  })

  it('does not reorder the caller\'s array', () => {
    const seats = ['b', 'a']

    new KeyKeeper().keyFor(seats)

    expect(seats).toEqual(['b', 'a'])
  })

  it('uses a new key when the seats change, even before the last try settled', () => {
    const keeper = new KeyKeeper()

    const first = keeper.keyFor(['a'])

    expect(keeper.keyFor(['a', 'b'])).not.toBe(first)
  })

  it('uses a new key after a success', () => {
    const keeper = new KeyKeeper()
    const first = keeper.keyFor(['a'])

    keeper.settle() // success

    expect(keeper.keyFor(['a'])).not.toBe(first)
  })

  // Reusing the key here would replay "card declined" for ever.
  it.each([
    [400, 'invalid_argument'],
    [402, 'payment_declined'],
    [409, 'seats_unavailable'],
    [409, 'request_in_progress'],
    [422, 'idempotency_key_reused'],
    [429, 'rate_limited'],
    [500, 'internal'],
    [503, 'service_unavailable'],
  ])('uses a new key after a definite answer (%i %s)', (status, code) => {
    const keeper = new KeyKeeper()
    const first = keeper.keyFor(['a'])

    keeper.settle(err(status, code))

    expect(keeper.keyFor(['a'])).not.toBe(first)
  })

  it.each([
    [0, 'a dropped connection'],
    [504, 'a gateway timeout'],
  ])('keeps the key when the outcome is unknown (%i, %s)', (status) => {
    const keeper = new KeyKeeper()
    const first = keeper.keyFor(['a'])

    keeper.settle(err(status))

    expect(keeper.keyFor(['a'])).toBe(first)
  })

  it('drops the key after an error that is not an ApiError', () => {
    const keeper = new KeyKeeper()
    const first = keeper.keyFor(['a'])

    keeper.settle(new TypeError('something in our own code'))

    expect(keeper.keyFor(['a'])).not.toBe(first)
  })

  it('never hands the same key to two separate keepers', () => {
    vi.spyOn(crypto, 'randomUUID')

    expect(new KeyKeeper().keyFor(['a'])).not.toBe(new KeyKeeper().keyFor(['a']))
  })
})
