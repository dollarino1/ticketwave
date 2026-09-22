import { Navigate, Outlet, useLocation } from 'react-router'
import { useAuth } from '../stores/auth'
import { Message } from './ui'

// RequireAuth waits for the first silent refresh before deciding, so a signed-in
// user who reloads the page is not bounced to the login form.
export function RequireAuth() {
  const status = useAuth((s) => s.status)
  const location = useLocation()

  if (status === 'unknown') return <Message>Loading…</Message>
  if (status === 'anonymous') return <Navigate to="/login" replace state={{ from: location.pathname }} />
  return <Outlet />
}

// RequireOrganizer hides pages a user could not use anyway. This is a convenience,
// not security: the gateway enforces the role on every request regardless.
export function RequireOrganizer() {
  const role = useAuth((s) => s.user?.role)
  if (role !== 'organizer' && role !== 'admin') {
    return <Message tone="error">This page is for event organizers.</Message>
  }
  return <Outlet />
}
