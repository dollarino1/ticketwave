import { useQueryClient } from '@tanstack/react-query'
import { Link, NavLink, Outlet } from 'react-router'
import { logout } from '../api/client'
import { useAuth } from '../stores/auth'

const navLink = ({ isActive }: { isActive: boolean }) =>
  `rounded px-3 py-1.5 text-sm font-medium ${isActive ? 'bg-slate-900 text-white' : 'text-slate-700 hover:bg-slate-200'}`

export function Layout() {
  const user = useAuth((s) => s.user)
  const qc = useQueryClient()

  const signOut = async () => {
    await logout()
    // Cached orders belong to the person who just left.
    qc.clear()
  }

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-5xl items-center justify-between gap-4 px-4 py-3">
          <Link to="/" className="text-lg font-bold tracking-tight text-indigo-600">
            TicketWave
          </Link>
          <nav className="flex items-center gap-1" aria-label="Main">
            <NavLink to="/" end className={navLink}>
              Events
            </NavLink>
            {user && user.role !== 'user' && (
              <NavLink to="/organizer" className={navLink}>
                Organizer
              </NavLink>
            )}
            {user ? (
              <>
                <span className="ml-2 hidden text-sm text-slate-500 sm:inline">{user.email}</span>
                <button
                  type="button"
                  onClick={() => void signOut()}
                  className="ml-1 rounded px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-200"
                >
                  Sign out
                </button>
              </>
            ) : (
              <NavLink to="/login" className={navLink}>
                Sign in
              </NavLink>
            )}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-5xl px-4 py-8">
        <Outlet />
      </main>
    </div>
  )
}
