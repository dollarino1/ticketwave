import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { renderHook } from '@testing-library/react'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { keys } from './queries'
import type { Seat } from './types'
import { useSeatStream } from './useSeatStream'

// A stand-in for the browser's EventSource that the test drives by hand.
class FakeEventSource {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSED = 2
  static instances: FakeEventSource[] = []

  readyState = FakeEventSource.CONNECTING
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  closed = false
  private listeners = new Map<string, ((e: MessageEvent<string>) => void)[]>()

  constructor(readonly url: string) {
    FakeEventSource.instances.push(this)
  }

  addEventListener(type: string, fn: (e: MessageEvent<string>) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), fn])
  }

  close() {
    this.closed = true
    this.readyState = FakeEventSource.CLOSED
  }

  // --- test controls ---
  open() {
    this.readyState = FakeEventSource.OPEN
    this.onopen?.()
  }
  emit(data: string) {
    for (const fn of this.listeners.get('seats') ?? []) fn(new MessageEvent('seats', { data }))
  }
  fail(readyState: number) {
    this.readyState = readyState
    this.onerror?.()
  }
}

const seats: Seat[] = [
  { id: 'a', label: 'Seat 1', status: 'available' },
  { id: 'b', label: 'Seat 2', status: 'available' },
]

let client: QueryClient
const wrapper = ({ children }: { children: ReactNode }) => (
  <QueryClientProvider client={client}>{children}</QueryClientProvider>
)
const current = () => FakeEventSource.instances[FakeEventSource.instances.length - 1]
const cached = () => client.getQueryData<Seat[]>(keys.seats('e1'))

beforeEach(() => {
  FakeEventSource.instances = []
  vi.stubGlobal('EventSource', FakeEventSource)
  client = new QueryClient()
  client.setQueryData(keys.seats('e1'), seats)
})
afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe('useSeatStream', () => {
  it('connects to the stream of that event', () => {
    renderHook(() => {
      useSeatStream('e1')
    }, { wrapper })

    expect(FakeEventSource.instances).toHaveLength(1)
    expect(current()?.url).toBe('/api/events/e1/stream')
  })

  it('writes pushed updates straight into the seat cache', () => {
    renderHook(() => {
      useSeatStream('e1')
    }, { wrapper })

    current()?.emit('{"seat_ids":["a"],"state":"held"}')

    expect(cached()?.map((s) => s.status)).toEqual(['held', 'available'])
  })

  it('refreshes the event too, since "seats left" is counted there', () => {
    const spy = vi.spyOn(client, 'invalidateQueries')
    renderHook(() => {
      useSeatStream('e1')
    }, { wrapper })

    current()?.emit('{"seat_ids":["a"],"state":"sold"}')

    expect(spy).toHaveBeenCalledWith({ queryKey: keys.event('e1'), exact: true })
  })

  it('ignores a malformed message instead of corrupting the map or throwing', () => {
    renderHook(() => {
      useSeatStream('e1')
    }, { wrapper })

    expect(() => {
      current()?.emit('garbage')
      current()?.emit('{"seat_ids":["a"],"state":"exploded"}')
    }).not.toThrow()

    expect(cached()).toBe(seats)
  })

  // What was missed while disconnected can only be recovered by refetching.
  it('refetches the seats on every connect, first one included', () => {
    const spy = vi.spyOn(client, 'invalidateQueries')
    renderHook(() => {
      useSeatStream('e1')
    }, { wrapper })

    current()?.open()
    current()?.open() // the browser's own automatic reconnect fires onopen again

    expect(spy.mock.calls.filter(([f]) => JSON.stringify(f) === JSON.stringify({ queryKey: keys.seats('e1') }))).toHaveLength(2)
  })

  it('closes the connection when the page is left', () => {
    const { unmount } = renderHook(() => {
      useSeatStream('e1')
    }, { wrapper })

    unmount()

    expect(current()?.closed).toBe(true)
  })

  it('switches streams when the event changes, closing the old one', () => {
    const { rerender } = renderHook(({ id }) => {
      useSeatStream(id)
    }, { wrapper, initialProps: { id: 'e1' } })
    const first = current()

    rerender({ id: 'e2' })

    expect(first?.closed).toBe(true)
    expect(current()?.url).toBe('/api/events/e2/stream')
  })

  describe('when the connection fails', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    it('leaves it to the browser while it is still retrying (CONNECTING)', () => {
      renderHook(() => {
        useSeatStream('e1')
      }, { wrapper })

      current()?.fail(FakeEventSource.CONNECTING)
      vi.advanceTimersByTime(60_000)

      expect(FakeEventSource.instances).toHaveLength(1)
    })

    // The browser never retries a stream it has closed (e.g. after a 503).
    it('reconnects itself once the browser has given up (CLOSED), after a pause', () => {
      renderHook(() => {
        useSeatStream('e1')
      }, { wrapper })
      const first = current()

      first?.fail(FakeEventSource.CLOSED)
      vi.advanceTimersByTime(4999)
      expect(FakeEventSource.instances).toHaveLength(1) // not before the pause: no hammering a struggling server
      vi.advanceTimersByTime(1)

      expect(FakeEventSource.instances).toHaveLength(2)
      expect(current()).not.toBe(first)
    })

    it('ignores an error that arrives after the page was left', () => {
      const { unmount } = renderHook(() => {
        useSeatStream('e1')
      }, { wrapper })
      const first = current()

      unmount()
      first?.fail(FakeEventSource.CLOSED) // an event already queued when the page closed it
      vi.advanceTimersByTime(60_000)

      expect(FakeEventSource.instances).toHaveLength(1)
    })

    it('does not reconnect after the page was left', () => {
      const { unmount } = renderHook(() => {
        useSeatStream('e1')
      }, { wrapper })
      current()?.fail(FakeEventSource.CLOSED)

      unmount()
      vi.advanceTimersByTime(60_000)

      expect(FakeEventSource.instances).toHaveLength(1)
    })
  })

  it('does nothing in a browser without EventSource, leaving the slow poll to cope', () => {
    vi.stubGlobal('EventSource', undefined)

    expect(() =>
      renderHook(() => {
        useSeatStream('e1')
      }, { wrapper }),
    ).not.toThrow()
  })
})
