import { useState, type SyntheticEvent } from 'react'
import { useAnalytics, useCreateEvent } from '../api/queries'
import { Button, Card, Field, Message } from '../components/ui'
import { describeError, money } from '../lib/format'

export function OrganizerPage() {
  return (
    <div className="space-y-8">
      <h1 className="text-2xl font-bold tracking-tight">Organizer dashboard</h1>
      <CreateEventForm />
      <Analytics />
    </div>
  )
}

function CreateEventForm() {
  const create = useCreateEvent()
  const [name, setName] = useState('')
  const [seats, setSeats] = useState('50')

  const submit = (e: SyntheticEvent) => {
    e.preventDefault()
    create.mutate(
      { name: name.trim(), seat_count: Number(seats) },
      {
        onSuccess: () => {
          setName('')
        },
      },
    )
  }

  return (
    <Card>
      <h2 className="mb-4 font-semibold">Create an event</h2>
      <form onSubmit={submit} className="grid gap-4 sm:grid-cols-[1fr_8rem_auto] sm:items-end">
        <Field
          label="Name"
          required
          maxLength={200}
          value={name}
          onChange={(e) => {
            setName(e.target.value)
          }}
        />
        <Field
          label="Seats"
          type="number"
          required
          min={1}
          max={10000}
          value={seats}
          onChange={(e) => {
            setSeats(e.target.value)
          }}
        />
        <Button type="submit" disabled={create.isPending}>
          {create.isPending ? 'Creating…' : 'Create'}
        </Button>
      </form>
      {create.isError && (
        <div className="mt-4">
          <Message tone="error">{describeError(create.error)}</Message>
        </div>
      )}
      {create.isSuccess && (
        <div className="mt-4">
          <Message tone="success">Event created.</Message>
        </div>
      )}
    </Card>
  )
}

function Analytics() {
  const { data, error, isPending } = useAnalytics()

  if (isPending) return <Message>Loading sales…</Message>
  if (error) return <Message tone="error">{describeError(error)}</Message>

  return (
    <Card className="overflow-x-auto">
      <h2 className="mb-4 font-semibold">Sales</h2>
      {data.length === 0 ? (
        <p className="text-sm text-slate-500">No orders yet.</p>
      ) : (
        <table className="w-full text-left text-sm">
          <thead className="text-xs uppercase text-slate-500">
            <tr>
              <th className="pb-2 pr-4">Event</th>
              <th className="pb-2 pr-4 text-right">Tickets</th>
              <th className="pb-2 pr-4 text-right">Revenue</th>
              <th className="pb-2 pr-4 text-right">Confirmed</th>
              <th className="pb-2 text-right">Failed</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {data.map((row) => (
              <tr key={row.event_id}>
                <td className="py-2 pr-4">{row.event_name || <span className="text-slate-400">Unknown event</span>}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{row.tickets_sold}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{money(row.revenue_cents)}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{row.orders_confirmed}</td>
                <td className="py-2 text-right tabular-nums">{row.orders_failed}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  )
}
