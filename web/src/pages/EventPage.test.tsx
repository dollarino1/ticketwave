import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Seat } from '../api/types'
import { useAuth } from '../stores/auth'
import { apiError, json, mockFetch, session, type Call } from '../test/helpers'
import { renderAt } from '../test/render'
import { EventPage } from './EventPage'

const event = {
  id: 'e1',
  name: 'Jazz Night',
  total_seats: 12,
  available_seats: 10,
  seat_price_cents: 5000,
  created_at: '2026-09-01T12:00:00Z',
}

function seatList(n: number, overrides: Record<number, Seat['status']> = {}): Seat[] {
  return Array.from({ length: n }, (_, i) => ({
    id: `s${i + 1}`,
    label: `Seat ${i + 1}`,
    status: overrides[i + 1] ?? 'available',
  }))
}

function serve(seats: Seat[], onOrder?: (c: Call) => ReturnType<typeof json>) {
  return mockFetch((c) => {
    if (c.path === '/api/events/e1') return json(200, { event })
    if (c.path === '/api/events/e1/seats') return json(200, { seats })
    if (c.path === '/api/orders') return onOrder ? onOrder(c) : json(201, { order_id: 'o1', status: 'confirmed' })
    return apiError(404, 'not_found')
  })
}

const open = () => renderAt('/events/e1', '/events/:id', <EventPage />)
const seat = (name: RegExp | string) => screen.findByRole('button', { name })

beforeEach(() => {
  useAuth.getState().setSession(session('tok'))
})
afterEach(() => {
  vi.unstubAllGlobals()
  useAuth.getState().clear()
})

describe('EventPage', () => {
  it('shows the seat map with each seat labelled by its state', async () => {
    serve(seatList(4, { 2: 'held', 3: 'sold' }))
    open()

    expect(await seat('Seat 1, available')).toBeEnabled()
    expect(await seat('Seat 2, held')).toBeDisabled()
    expect(await seat('Seat 3, sold')).toBeDisabled()
  })

  it('totals the selection from the price the server reported', async () => {
    serve(seatList(4))
    open()
    const user = userEvent.setup()

    await user.click(await seat('Seat 1, available'))
    await user.click(await seat('Seat 2, available'))

    expect(screen.getByText('$100.00')).toBeInTheDocument()
    expect(screen.getByText('$100.00').parentElement).toHaveTextContent('2 seats')
  })

  it('lets a seat be deselected', async () => {
    serve(seatList(3))
    open()
    const user = userEvent.setup()

    await user.click(await seat('Seat 1, available'))
    await user.click(await seat('Seat 1, available'))

    expect(screen.getByText(/Pick up to 10 seats/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Buy tickets' })).toBeDisabled()
  })

  it('stops the selection at 10 seats', async () => {
    serve(seatList(12))
    open()
    const user = userEvent.setup()

    for (let i = 1; i <= 12; i++) await user.click(await seat(`Seat ${i}, available`))

    expect(screen.getByText('$500.00').parentElement).toHaveTextContent('10 seats')
    expect(screen.getByText('$500.00')).toBeInTheDocument()
  })

  it('sends only the event and the chosen seats, never a price or a user', async () => {
    const calls = serve(seatList(3))
    open()
    const user = userEvent.setup()

    await user.click(await seat('Seat 2, available'))
    await user.click(await seat('Seat 3, available'))
    await user.click(screen.getByRole('button', { name: 'Buy tickets' }))

    await waitFor(() => {
      expect(screen.getByText('ORDER PAGE')).toBeInTheDocument()
    })
    const order = calls.find((c) => c.path === '/api/orders')
    expect(order?.body).toEqual({ event_id: 'e1', seat_ids: ['s2', 's3'] })
    expect(order?.headers.Authorization).toBe('Bearer tok')
  })

  it('explains a lost race and clears the selection', async () => {
    serve(seatList(3), () => apiError(409, 'seats_unavailable'))
    open()
    const user = userEvent.setup()

    await user.click(await seat('Seat 1, available'))
    await user.click(screen.getByRole('button', { name: 'Buy tickets' }))

    expect(await screen.findByRole('alert')).toHaveTextContent(/no longer available/)
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Buy tickets' })).toBeDisabled()
    })
  })

  it('tells the buyer a declined card cost them nothing', async () => {
    serve(seatList(3), () => apiError(402, 'payment_declined'))
    open()
    const user = userEvent.setup()

    await user.click(await seat('Seat 1, available'))
    await user.click(screen.getByRole('button', { name: 'Buy tickets' }))

    expect(await screen.findByRole('alert')).toHaveTextContent(/not charged/)
  })

  it('offers sign-in instead of a buy button to an anonymous visitor', async () => {
    useAuth.getState().clear()
    serve(seatList(3))
    open()

    const link = await screen.findByRole('link', { name: 'Sign in to buy' })

    expect(link).toHaveAttribute('href', '/login')
    expect(screen.queryByRole('button', { name: 'Buy tickets' })).not.toBeInTheDocument()
  })

  it('reports a missing event', async () => {
    mockFetch(() => apiError(404, 'not_found', 'event not found'))
    open()

    expect(await screen.findByRole('alert')).toHaveTextContent('event not found')
  })
})
