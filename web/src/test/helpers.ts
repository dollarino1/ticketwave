import { vi } from 'vitest'

export interface Call {
  method: string
  path: string
  headers: Record<string, string>
  body: unknown
}

type Reply = { status: number; body?: unknown } | Error

export function json(status: number, body?: unknown): Reply {
  return body === undefined ? { status } : { status, body }
}

export function apiError(status: number, code: string, message = 'msg'): Reply {
  return { status, body: { error: { code, message } } }
}

// mockFetch replaces fetch with a router: `handler` sees every request and returns
// the reply (or throws-by-returning an Error to simulate a dropped connection).
// Every request is recorded so tests can assert on what the app actually sent.
export function mockFetch(handler: (call: Call) => Reply | Promise<Reply>) {
  const calls: Call[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn(async (path: string, init?: RequestInit) => {
      const call: Call = {
        method: init?.method ?? 'GET',
        path,
        headers: (init?.headers ?? {}) as Record<string, string>,
        body: typeof init?.body === 'string' ? (JSON.parse(init.body) as unknown) : undefined,
      }
      calls.push(call)
      const reply = await handler(call)
      if (reply instanceof Error) throw reply
      return new Response(reply.body === undefined ? null : JSON.stringify(reply.body), { status: reply.status })
    }),
  )
  return calls
}

export const session = (token: string, role: 'user' | 'organizer' | 'admin' = 'user') => ({
  access_token: token,
  expires_in: 900,
  user: { id: 'u1', email: 'ann@example.com', role },
})
