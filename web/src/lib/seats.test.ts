import { describe, expect, it } from 'vitest'
import type { Seat } from '../api/types'
import { applyUpdate, parseUpdate } from './seats'

const seats: Seat[] = [
  { id: 'a', label: 'Seat 1', status: 'available' },
  { id: 'b', label: 'Seat 2', status: 'available' },
  { id: 'c', label: 'Seat 3', status: 'sold' },
]

describe('applyUpdate', () => {
  it('changes only the named seats', () => {
    const next = applyUpdate(seats, { seat_ids: ['a', 'c'], state: 'held' })

    expect(next?.map((s) => s.status)).toEqual(['held', 'available', 'held'])
    expect(next?.[1]).toBe(seats[1]) // untouched seats keep their identity
  })

  it('never mutates its input', () => {
    const before = structuredClone(seats)

    applyUpdate(seats, { seat_ids: ['a'], state: 'sold' })

    expect(seats).toEqual(before)
  })

  it('returns the SAME array when nothing changed, so React does not re-render', () => {
    expect(applyUpdate(seats, { seat_ids: ['a'], state: 'available' })).toBe(seats)
    expect(applyUpdate(seats, { seat_ids: ['zzz'], state: 'sold' })).toBe(seats)
  })

  it('ignores seats it does not know about while still applying the known ones', () => {
    const next = applyUpdate(seats, { seat_ids: ['zzz', 'b'], state: 'sold' })

    expect(next?.map((s) => s.status)).toEqual(['available', 'sold', 'sold'])
  })

  it('does nothing before the seats have loaded', () => {
    expect(applyUpdate(undefined, { seat_ids: ['a'], state: 'held' })).toBeUndefined()
  })
})

describe('parseUpdate', () => {
  it.each(['available', 'held', 'sold'])('accepts state %s', (state) => {
    expect(parseUpdate(JSON.stringify({ seat_ids: ['a'], state }))).toEqual({ seat_ids: ['a'], state })
  })

  it.each([
    ['not JSON', 'nope'],
    ['null', 'null'],
    ['a number', '5'],
    ['an array', '[]'],
    ['no state', '{"seat_ids":["a"]}'],
    ['an unknown state', '{"seat_ids":["a"],"state":"broken"}'],
    ['"unknown" state, which would blank seats', '{"seat_ids":["a"],"state":"unknown"}'],
    ['no seat ids', '{"state":"held"}'],
    ['seat ids of the wrong type', '{"seat_ids":[1,2],"state":"held"}'],
    ['seat ids not an array', '{"seat_ids":"a","state":"held"}'],
  ])('rejects %s', (_name, raw) => {
    expect(parseUpdate(raw)).toBeNull()
  })
})
