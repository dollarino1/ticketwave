import { useEffect } from 'react'
import { Route, Routes } from 'react-router'
import { refreshSession } from './api/client'
import { Layout } from './components/Layout'
import { RequireAuth, RequireOrganizer } from './components/guards'
import { Message } from './components/ui'
import { AuthPage } from './pages/AuthPage'
import { EventPage } from './pages/EventPage'
import { EventsPage } from './pages/EventsPage'
import { OrderPage } from './pages/OrderPage'
import { OrganizerPage } from './pages/OrganizerPage'
import { useAuth } from './stores/auth'

export function App() {
  // The access token lives only in memory, so every page load starts signed out.
  // Try the httpOnly refresh cookie once to quietly sign the person back in.
  useEffect(() => {
    void refreshSession().then(() => {
      // No cookie, or the server was unreachable: either way, stop saying "loading".
      if (useAuth.getState().status === 'unknown') useAuth.getState().clear()
    })
  }, [])

  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<EventsPage />} />
        <Route path="events/:id" element={<EventPage />} />
        <Route path="login" element={<AuthPage mode="login" />} />
        <Route path="register" element={<AuthPage mode="register" />} />
        <Route element={<RequireAuth />}>
          <Route path="orders/:id" element={<OrderPage />} />
          <Route element={<RequireOrganizer />}>
            <Route path="organizer" element={<OrganizerPage />} />
          </Route>
        </Route>
        <Route path="*" element={<Message>Page not found.</Message>} />
      </Route>
    </Routes>
  )
}
