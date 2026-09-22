import { useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router'
import { useCreateOrder, useEvent, useSeats } from '../api/queries'
import { useSeatStream } from '../api/useSeatStream'
import type { Seat } from '../api/types'
import { Button, Card, Message } from '../components/ui'
import { describeError, money } from '../lib/format'
import { isUnknownOutcome } from '../lib/idempotency'
import { useAuth } from '../stores/auth'

// Mirrors the gateway's limit; the gateway enforces it, this only saves a round trip.
const MAX_SEATS = 10

const seatStyle: Record<Seat['status'], string> = {
  available: 'border-green-300 bg-green-50 text-green-900 hover:bg-green-100',
  held: 'cursor-not-allowed border-amber-200 bg-amber-50 text-amber-700',
  sold: 'cursor-not-allowed border-slate-200 bg-slate-100 text-slate-400 line-through',
  unknown: 'cursor-not-allowed border-slate-200 bg-slate-100 text-slate-400',
}

export function EventPage() {
  const { id = '' } = useParams()
  const navigate = useNavigate()
  const signedIn = useAuth((s) => s.status === 'authenticated')

  const event = useEvent(id)
  const seats = useSeats(id)
  useSeatStream(id)
  const order = useCreateOrder(id)
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set())

  if (event.isPending || seats.isPending) return <Message>Loading…</Message>
  if (event.error) return <Message tone="error">{describeError(event.error)}</Message>
  if (seats.error) return <Message tone="error">{describeError(seats.error)}</Message>

  // Only seats that are still available count. If somebody else buys a seat while
  // it sits in this person's selection, it silently drops out instead of failing
  // the whole order at checkout.
  const availableIDs = new Set(seats.data.filter((s) => s.status === 'available').map((s) => s.id))
  const chosen = [...selected].filter((seatID) => availableIDs.has(seatID))
  const total = chosen.length * event.data.seat_price_cents

  const toggle = (seat: Seat) => {
    order.reset()
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(seat.id)) next.delete(seat.id)
      else if (next.size < MAX_SEATS) next.add(seat.id)
      return next
    })
  }

  const buy = () => {
    order.mutate(chosen, {
      onSuccess: (created) => void navigate(`/orders/${created.order_id}`),
      onError: (error) => {
        // If the answer never arrived (dropped connection, timeout) the order may
        // well have gone through. Keep the selection so pressing Buy again retries
        // the SAME purchase, which the idempotency key makes safe.
        if (isUnknownOutcome(error)) return
        // Otherwise the server decided: the map changed under us, or the card failed.
        // Start over from what is really free.
        setSelected(new Set())
        void seats.refetch()
      },
    })
  }

  return (
    <>
      <Link to="/" className="text-sm text-indigo-600 hover:underline">
        ← All events
      </Link>
      <h1 className="mt-2 text-2xl font-bold tracking-tight">{event.data.name}</h1>
      <p className="mt-1 text-sm text-slate-600">
        {event.data.available_seats} of {event.data.total_seats} seats left · {money(event.data.seat_price_cents)} each
      </p>

      <div className="mt-6 grid gap-6 md:grid-cols-[1fr_18rem]">
        <Card>
          <ul className="grid grid-cols-4 gap-2 sm:grid-cols-6 lg:grid-cols-8" aria-label="Seat map">
            {seats.data.map((seat) => {
              const isSelected = selected.has(seat.id) && availableIDs.has(seat.id)
              return (
                <li key={seat.id}>
                  <button
                    type="button"
                    disabled={seat.status !== 'available'}
                    aria-pressed={isSelected}
                    aria-label={`${seat.label}, ${seat.status}`}
                    onClick={() => {
                      toggle(seat)
                    }}
                    className={`w-full rounded-md border px-1 py-2 text-xs font-medium ${
                      isSelected ? 'border-indigo-600 bg-indigo-600 text-white hover:bg-indigo-600' : seatStyle[seat.status]
                    }`}
                  >
                    {seat.label.replace('Seat ', '')}
                  </button>
                </li>
              )
            })}
          </ul>
          <ul className="mt-4 flex flex-wrap gap-4 text-xs text-slate-600">
            <li>
              <span className="mr-1 inline-block size-3 rounded border border-green-300 bg-green-50 align-middle" />
              Available
            </li>
            <li>
              <span className="mr-1 inline-block size-3 rounded border border-indigo-600 bg-indigo-600 align-middle" />
              Yours
            </li>
            <li>
              <span className="mr-1 inline-block size-3 rounded border border-amber-200 bg-amber-50 align-middle" />
              Held by someone
            </li>
            <li>
              <span className="mr-1 inline-block size-3 rounded border border-slate-200 bg-slate-100 align-middle" />
              Sold
            </li>
          </ul>
        </Card>

        <Card className="h-fit space-y-3">
          <h2 className="font-semibold">Your order</h2>
          {chosen.length === 0 ? (
            <p className="text-sm text-slate-500">Pick up to {MAX_SEATS} seats.</p>
          ) : (
            <p className="text-sm">
              {chosen.length} seat{chosen.length === 1 ? '' : 's'} · <strong>{money(total)}</strong>
            </p>
          )}
          {order.isError && <Message tone="error">{describeError(order.error)}</Message>}
          {signedIn ? (
            <Button className="w-full" disabled={chosen.length === 0 || order.isPending} onClick={buy}>
              {order.isPending ? 'Processing…' : 'Buy tickets'}
            </Button>
          ) : (
            <Link
              to="/login"
              state={{ from: `/events/${id}` }}
              className="block rounded-lg bg-indigo-600 px-4 py-2 text-center text-sm font-semibold text-white hover:bg-indigo-500"
            >
              Sign in to buy
            </Link>
          )}
        </Card>
      </div>
    </>
  )
}
