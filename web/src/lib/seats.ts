import type { Seat, SeatStatus } from '../api/types'

export interface SeatUpdate {
  seat_ids: string[]
  state: SeatStatus
}

const states: readonly SeatStatus[] = ['available', 'held', 'sold']

// parseUpdate checks a stream message before it is trusted. The data comes off the
// network, so a bad message must be ignored rather than corrupt the seat map or
// throw inside an event handler.
export function parseUpdate(raw: string): SeatUpdate | null {
  let value: unknown
  try {
    value = JSON.parse(raw)
  } catch {
    return null
  }
  if (typeof value !== 'object' || value === null) return null
  const { seat_ids, state } = value as Record<string, unknown>
  if (!Array.isArray(seat_ids) || !seat_ids.every((id) => typeof id === 'string')) return null
  if (typeof state !== 'string' || !states.includes(state as SeatStatus)) return null
  // `state as SeatStatus` is safe: the includes() check above just proved it.
  return { seat_ids, state: state as SeatStatus }
}

// applyUpdate returns the seat list with the update applied. It never mutates its
// input (React and the query cache rely on new references to notice changes), and
// returns the same array when nothing changed so no re-render happens for nothing.
// Ids the list does not contain are ignored: the update may be about a seat the
// page has not loaded yet, and the resync on connect will cover it.
export function applyUpdate(seats: Seat[] | undefined, update: SeatUpdate): Seat[] | undefined {
  if (!seats) return seats
  const ids = new Set(update.seat_ids)
  const next = seats.map((seat) =>
    ids.has(seat.id) && seat.status !== update.state ? { ...seat, status: update.state } : seat,
  )
  // Untouched seats keep their identity, so a changed element is a changed reference.
  return next.some((seat, i) => seat !== seats[i]) ? next : seats
}
