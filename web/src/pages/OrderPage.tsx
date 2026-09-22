import { Link, useParams } from 'react-router'
import { useOrder } from '../api/queries'
import { ApiError } from '../api/client'
import { Card, Message } from '../components/ui'
import { describeError, money } from '../lib/format'

export function OrderPage() {
  const { id = '' } = useParams()
  const { data, error, isPending } = useOrder(id)

  if (isPending) return <Message>Loading your order…</Message>
  if (error instanceof ApiError && error.status === 404) return <Message tone="error">We could not find that order.</Message>
  if (error) return <Message tone="error">{describeError(error)}</Message>

  const seatCount = data.seat_ids?.length ?? 0
  return (
    <div className="mx-auto max-w-md">
      <Card className="space-y-4">
        {data.status === 'confirmed' && <Message tone="success">Payment received. Your seats are booked.</Message>}
        {data.status === 'pending' && <Message>Processing your payment…</Message>}
        {data.status === 'failed' && (
          <Message tone="error">This order did not go through. You were not charged and the seats were released.</Message>
        )}
        <dl className="grid grid-cols-2 gap-y-2 text-sm">
          <dt className="text-slate-500">Order</dt>
          <dd className="truncate font-mono text-xs">{data.order_id}</dd>
          <dt className="text-slate-500">Seats</dt>
          <dd>{seatCount}</dd>
          <dt className="text-slate-500">Total</dt>
          <dd>{data.amount_cents === undefined ? '—' : money(data.amount_cents)}</dd>
          <dt className="text-slate-500">Status</dt>
          <dd className="capitalize">{data.status}</dd>
        </dl>
        <Link to="/" className="inline-block text-sm text-indigo-600 hover:underline">
          ← Back to events
        </Link>
      </Card>
    </div>
  )
}
