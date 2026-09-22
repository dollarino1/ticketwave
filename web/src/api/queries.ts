import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useRef } from 'react'
import { KeyKeeper } from '../lib/idempotency'
import { api, publicApi } from './client'
import type { EventStats, EventSummary, Order, Seat } from './types'

export const keys = {
  events: ['events'] as const,
  event: (id: string) => ['events', id] as const,
  seats: (id: string) => ['events', id, 'seats'] as const,
  order: (id: string) => ['orders', id] as const,
  analytics: ['analytics'] as const,
}

export function useEvents() {
  return useQuery({
    queryKey: keys.events,
    queryFn: async () => (await publicApi<{ events: EventSummary[] }>('GET', '/api/events')).events,
  })
}

export function useEvent(id: string) {
  return useQuery({
    queryKey: keys.event(id),
    queryFn: async () => (await publicApi<{ event: EventSummary }>('GET', `/api/events/${id}`)).event,
  })
}

export function useSeats(id: string) {
  return useQuery({
    queryKey: keys.seats(id),
    queryFn: async () => (await publicApi<{ seats: Seat[] }>('GET', `/api/events/${id}/seats`)).seats,
    // The live stream (useSeatStream) keeps this current. This slow poll is only the
    // safety net for a stream that is down.
    refetchInterval: 30_000,
  })
}

export function useOrder(id: string) {
  return useQuery({
    queryKey: keys.order(id),
    queryFn: () => api<Order>('GET', `/api/orders/${id}`),
    // Keep asking only while the order can still change.
    refetchInterval: (query) => (query.state.data?.status === 'pending' ? 1500 : false),
  })
}

export function useCreateOrder(eventId: string) {
  const qc = useQueryClient()
  // One keeper for the life of the page, so a retry after a dropped connection reuses
  // the key and the server can recognise it as the same purchase.
  const keeper = useRef(new KeyKeeper())

  return useMutation({
    mutationFn: (seatIds: string[]) =>
      api<Order>(
        'POST',
        '/api/orders',
        { event_id: eventId, seat_ids: seatIds },
        { 'Idempotency-Key': keeper.current.keyFor(seatIds) },
      ),
    onSuccess: () => {
      keeper.current.settle()
    },
    onError: (error) => {
      keeper.current.settle(error)
    },
    // Win or lose, availability changed, so drop the cached seat map and event.
    onSettled: () => qc.invalidateQueries({ queryKey: keys.event(eventId) }),
  })
}

export function useCreateEvent() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (input: { name: string; seat_count: number }) => api<{ event_id: string }>('POST', '/api/events', input),
    onSuccess: () => qc.invalidateQueries({ queryKey: keys.events }),
  })
}

export function useAnalytics() {
  return useQuery({
    queryKey: keys.analytics,
    queryFn: async () => (await api<{ events: EventStats[] }>('GET', '/api/analytics/events')).events,
    refetchInterval: 10_000,
  })
}
