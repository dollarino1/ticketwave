import { describe, expect, it } from 'vitest'
import { ApiError } from '../api/client'
import { describeError, money } from './format'

describe('money', () => {
  it.each([
    [0, '$0.00'],
    [5000, '$50.00'],
    [35000, '$350.00'],
    [1999, '$19.99'],
    [1, '$0.01'],
    [123456789, '$1,234,567.89'],
  ])('%i cents is %s', (cents, want) => {
    expect(money(cents)).toBe(want)
  })
})

describe('describeError', () => {
  it('uses friendly wording for failures a user can hit', () => {
    expect(describeError(new ApiError(409, 'seats_unavailable', 'dev message'))).toMatch(/no longer available/)
    expect(describeError(new ApiError(402, 'payment_declined', 'dev message'))).toMatch(/not charged/)
    expect(describeError(new ApiError(503, 'service_unavailable', 'dev message'))).toMatch(/not charged/)
  })

  it('falls back to the gateway message for codes it has no wording for', () => {
    expect(describeError(new ApiError(400, 'invalid_argument', 'email is not valid'))).toBe('email is not valid')
  })

  it('never shows raw exception text', () => {
    expect(describeError(new TypeError('x is not a function at bundle.js:1'))).not.toMatch(/bundle/)
  })
})
