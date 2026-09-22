import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useAuth } from '../stores/auth'
import { apiError, json, mockFetch, session } from '../test/helpers'
import { api, ApiError, login, logout, publicApi, refreshSession } from './client'

beforeEach(() => {
  useAuth.getState().clear()
  useAuth.getState().setSession(session('old-token'))
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('api', () => {
  it('sends the access token from memory', async () => {
    const calls = mockFetch(() => json(200, { ok: true }))

    await api('GET', '/api/orders/1')

    expect(calls[0]?.headers.Authorization).toBe('Bearer old-token')
  })

  it('turns the gateway error envelope into an ApiError', async () => {
    mockFetch(() => apiError(409, 'seats_unavailable', 'gone'))

    const err = await api('POST', '/api/orders', {}).catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ApiError)
    expect(err).toMatchObject({ status: 409, code: 'seats_unavailable', message: 'gone' })
  })

  it('survives an error body that is not the envelope (a proxy error page)', async () => {
    mockFetch(() => json(502))

    const err = await api('GET', '/x').catch((e: unknown) => e)

    expect(err).toMatchObject({ status: 502, code: 'unknown' })
  })

  it('reports a dropped connection as a network_error with status 0', async () => {
    mockFetch(() => new Error('Failed to fetch'))

    const err = await api('GET', '/x').catch((e: unknown) => e)

    expect(err).toMatchObject({ status: 0, code: 'network_error' })
  })

  it('does not try to refresh after an ordinary error', async () => {
    const calls = mockFetch(() => apiError(403, 'permission_denied'))

    await api('GET', '/x').catch(() => undefined)

    expect(calls).toHaveLength(1)
  })

  it('refreshes once on a 401 and retries with the new token', async () => {
    const calls = mockFetch((c) => {
      if (c.path === '/api/auth/refresh') return json(200, session('new-token'))
      return c.headers.Authorization === 'Bearer new-token' ? json(200, { ok: 1 }) : apiError(401, 'unauthenticated')
    })

    await expect(api('GET', '/api/orders/1')).resolves.toEqual({ ok: 1 })

    expect(calls.map((c) => c.path)).toEqual(['/api/orders/1', '/api/auth/refresh', '/api/orders/1'])
    expect(useAuth.getState().accessToken).toBe('new-token')
  })

  it('sends the CSRF header, and no bearer token, on refresh', async () => {
    const calls = mockFetch(() => json(200, session('new-token')))

    await refreshSession()

    expect(calls[0]?.headers['X-Requested-With']).toBe('ticketwave-web')
    expect(calls[0]?.headers.Authorization).toBeUndefined()
  })

  // The important one: parallel 401s must not each rotate the refresh token, because
  // the server reads a reused (already rotated) token as theft and kills the session.
  it('shares ONE refresh between concurrent requests that all get a 401', async () => {
    let releaseRefresh: () => void = () => undefined
    const refreshGate = new Promise<void>((resolve) => {
      releaseRefresh = resolve
    })
    const calls = mockFetch(async (c) => {
      if (c.path === '/api/auth/refresh') {
        await refreshGate
        return json(200, session('new-token'))
      }
      return c.headers.Authorization === 'Bearer new-token' ? json(200, { ok: 1 }) : apiError(401, 'unauthenticated')
    })

    const all = Promise.all([api('GET', '/a'), api('GET', '/b'), api('GET', '/c')])
    await vi.waitFor(() => {
      expect(calls.filter((c) => c.path !== '/api/auth/refresh')).toHaveLength(3)
    })
    releaseRefresh()
    await all

    expect(calls.filter((c) => c.path === '/api/auth/refresh')).toHaveLength(1)
  })

  it('can refresh again later (the shared promise is not kept forever)', async () => {
    const calls = mockFetch(() => json(200, session('t')))

    await refreshSession()
    await refreshSession()

    expect(calls).toHaveLength(2)
  })

  it('gives up if the request is still refused after a successful refresh', async () => {
    const calls = mockFetch((c) => (c.path === '/api/auth/refresh' ? json(200, session('new')) : apiError(401, 'unauthenticated')))

    const err = await api('GET', '/x').catch((e: unknown) => e)

    expect(err).toMatchObject({ status: 401 })
    expect(calls.filter((c) => c.path === '/x')).toHaveLength(2) // no endless loop
  })

  it('ends the session when the refresh token is dead', async () => {
    mockFetch((c) => (c.path === '/api/auth/refresh' ? apiError(401, 'unauthenticated') : apiError(401, 'unauthenticated')))

    await api('GET', '/x').catch(() => undefined)

    expect(useAuth.getState().status).toBe('anonymous')
    expect(useAuth.getState().accessToken).toBeNull()
  })

  it('keeps the session when the refresh fails only because the network dropped', async () => {
    mockFetch((c) => (c.path === '/api/auth/refresh' ? new Error('offline') : apiError(401, 'unauthenticated')))

    await api('GET', '/x').catch(() => undefined)

    expect(useAuth.getState().status).toBe('authenticated')
  })
})

describe('publicApi', () => {
  it('sends no credentials', async () => {
    const calls = mockFetch(() => json(200, {}))

    await publicApi('GET', '/api/events')

    expect(calls[0]?.headers.Authorization).toBeUndefined()
  })
})

describe('login / logout', () => {
  it('stores the session in memory on login', async () => {
    useAuth.getState().clear()
    mockFetch(() => json(200, session('fresh', 'organizer')))

    await login({ email: 'ann@example.com', password: 'pw' })

    expect(useAuth.getState()).toMatchObject({ status: 'authenticated', accessToken: 'fresh' })
    expect(useAuth.getState().user?.role).toBe('organizer')
  })

  it('never writes the token to web storage', async () => {
    useAuth.getState().clear()
    mockFetch(() => json(200, session('fresh')))

    await login({ email: 'a@b.co', password: 'pw' })

    expect(JSON.stringify(Object.entries(localStorage))).not.toContain('fresh')
    expect(JSON.stringify(Object.entries(sessionStorage))).not.toContain('fresh')
  })

  it('logs out locally even when the server call fails', async () => {
    mockFetch(() => new Error('offline'))

    await logout().catch(() => undefined)

    expect(useAuth.getState().status).toBe('anonymous')
  })
})
