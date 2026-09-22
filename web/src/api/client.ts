import { useAuth } from '../stores/auth'
import type { Session } from './types'

// Mirrors the gateway's error envelope: {"error":{"code":"...","message":"..."}}.
export class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

interface SendOptions {
  body?: unknown
  token?: string | null
  csrf?: boolean
  // Extra request headers, such as Idempotency-Key.
  headers?: Record<string, string>
}

interface ErrorEnvelope {
  error?: { code?: string; message?: string }
}

async function send<T>(method: string, path: string, opts: SendOptions = {}): Promise<T> {
  const headers: Record<string, string> = { ...opts.headers }
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json'
  if (opts.token) headers.Authorization = `Bearer ${opts.token}`
  // The gateway requires this on cookie-authenticated endpoints. A page on another
  // origin cannot add a custom header without a CORS preflight, which the gateway
  // never approves, so this header proves the request came from our own page.
  if (opts.csrf) headers['X-Requested-With'] = 'ticketwave-web'

  let res: Response
  try {
    res = await fetch(path, {
      method,
      headers,
      credentials: 'same-origin',
      ...(opts.body !== undefined && { body: JSON.stringify(opts.body) }),
    })
  } catch {
    throw new ApiError(0, 'network_error', 'Cannot reach the server. Check your connection.')
  }

  if (res.status === 204) return undefined as T

  const text = await res.text()
  let payload: unknown
  try {
    payload = text ? JSON.parse(text) : undefined
  } catch {
    payload = undefined
  }

  if (!res.ok) {
    const env = (payload as ErrorEnvelope | undefined)?.error
    throw new ApiError(res.status, env?.code ?? 'unknown', env?.message ?? `Request failed (${res.status})`)
  }
  return payload as T
}

// One refresh at a time. When the access token expires, several requests fail at
// once; each starting its own refresh would rotate the token repeatedly, and the
// server treats reuse of a rotated token as theft and ends the whole session.
// Everyone waiting shares the one in-flight promise instead.
let inflight: Promise<boolean> | null = null

export function refreshSession(): Promise<boolean> {
  inflight ??= (async () => {
    try {
      const session = await send<Session>('POST', '/api/auth/refresh', { csrf: true })
      useAuth.getState().setSession(session)
      return true
    } catch (err) {
      // Only a definite "no" ends the session. A dropped connection must not log
      // the user out.
      if (err instanceof ApiError && err.status === 401) useAuth.getState().clear()
      return false
    } finally {
      inflight = null
    }
  })()
  return inflight
}

// api sends an authenticated request. If the access token has expired it refreshes
// once and retries once; a second 401 is a real one.
export async function api<T>(
  method: string,
  path: string,
  body?: unknown,
  headers?: Record<string, string>,
): Promise<T> {
  // The same headers go out again on the transparent retry after a token refresh, so an
  // Idempotency-Key survives it: the retry is still the same request.
  const attempt = (token: string | null) =>
    send<T>(method, path, { token, ...(body !== undefined && { body }), ...(headers && { headers }) })

  try {
    return await attempt(useAuth.getState().accessToken)
  } catch (err) {
    if (!(err instanceof ApiError) || err.status !== 401) throw err
    if (!(await refreshSession())) throw err
    return attempt(useAuth.getState().accessToken)
  }
}

// publicApi is for endpoints that need no sign-in.
export function publicApi<T>(method: string, path: string, body?: unknown): Promise<T> {
  return send<T>(method, path, body !== undefined ? { body } : {})
}

export interface Credentials {
  email: string
  password: string
}

export async function login(credentials: Credentials): Promise<void> {
  const session = await send<Session>('POST', '/api/auth/login', { body: credentials })
  useAuth.getState().setSession(session)
}

export async function register(credentials: Credentials): Promise<void> {
  await send('POST', '/api/auth/register', { body: credentials })
  await login(credentials)
}

export async function logout(): Promise<void> {
  try {
    await send('POST', '/api/auth/logout', { csrf: true })
  } finally {
    useAuth.getState().clear()
  }
}
