import { useQueryClient } from '@tanstack/react-query'
import { useEffect } from 'react'
import { applyUpdate, parseUpdate } from '../lib/seats'
import { keys } from './queries'
import type { Seat } from './types'

// How long to wait before reconnecting a stream the browser has given up on.
const RECONNECT_MS = 5000

// useSeatStream keeps the cached seat map of one event live. The server pushes
// "these seats are now held/sold/available" over Server-Sent Events, and each push
// is written straight into the query cache, so every component showing the seats
// repaints with no refetch.
//
// Pushes only ever carry changes, so anything missed while disconnected would leave
// the map wrong for good. Hence on EVERY (re)connect the seat list is refetched,
// which also closes the small window between the first fetch and the stream opening.
export function useSeatStream(eventId: string): void {
  const qc = useQueryClient()

  useEffect(() => {
    if (typeof EventSource === 'undefined' || eventId === '') return

    let source: EventSource | null = null
    let retry: ReturnType<typeof setTimeout> | undefined
    let stopped = false

    const connect = () => {
      const es = new EventSource(`/api/events/${eventId}/stream`)
      source = es

      es.onopen = () => {
        void qc.invalidateQueries({ queryKey: keys.seats(eventId) })
      }

      es.addEventListener('seats', (e: MessageEvent<string>) => {
        const update = parseUpdate(e.data)
        if (!update) return
        qc.setQueryData<Seat[] | undefined>(keys.seats(eventId), (old) => applyUpdate(old, update))
        // The "N seats left" line comes from the event, not from the seats.
        void qc.invalidateQueries({ queryKey: keys.event(eventId), exact: true })
      })

      es.onerror = () => {
        // While readyState is CONNECTING the browser is already retrying by itself.
        // CLOSED means it has given up (for instance the server answered 503 because
        // it is full), and it never retries after that, so do it here.
        if (es.readyState === EventSource.CLOSED && !stopped) {
          es.close()
          retry = setTimeout(connect, RECONNECT_MS)
        }
      }
    }

    connect()
    return () => {
      stopped = true
      clearTimeout(retry)
      source?.close()
    }
  }, [eventId, qc])
}
