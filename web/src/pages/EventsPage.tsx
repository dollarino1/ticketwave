import { Link } from 'react-router'
import { useEvents } from '../api/queries'
import { Card, Message } from '../components/ui'
import { describeError, money, shortDate } from '../lib/format'

export function EventsPage() {
  const { data, error, isPending } = useEvents()

  if (isPending) return <Message>Loading events…</Message>
  if (error) return <Message tone="error">{describeError(error)}</Message>
  if (data.length === 0) return <Message>No events yet. Check back soon.</Message>

  return (
    <>
      <h1 className="mb-6 text-2xl font-bold tracking-tight">Upcoming events</h1>
      <ul className="grid gap-4 sm:grid-cols-2">
        {data.map((event) => {
          const soldOut = event.available_seats === 0
          return (
            <li key={event.id}>
              <Link to={`/events/${event.id}`} className="block rounded-xl focus:outline-none focus:ring-2 focus:ring-indigo-500">
                <Card className="transition hover:border-indigo-300 hover:shadow-md">
                  <h2 className="text-lg font-semibold">{event.name}</h2>
                  <p className="mt-1 text-sm text-slate-500">Added {shortDate(event.created_at)}</p>
                  <div className="mt-4 flex items-end justify-between">
                    <span className={`text-sm font-medium ${soldOut ? 'text-red-600' : 'text-green-700'}`}>
                      {soldOut ? 'Sold out' : `${event.available_seats} of ${event.total_seats} seats left`}
                    </span>
                    <span className="text-sm font-semibold">{money(event.seat_price_cents)} / seat</span>
                  </div>
                </Card>
              </Link>
            </li>
          )
        })}
      </ul>
    </>
  )
}
