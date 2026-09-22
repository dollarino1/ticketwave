import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, describe, expect, it } from 'vitest'
import { useAuth } from '../stores/auth'
import { session } from '../test/helpers'
import { RequireAuth, RequireOrganizer } from './guards'

function renderGuarded() {
  return render(
    <MemoryRouter initialEntries={['/secret']}>
      <Routes>
        <Route path="/login" element={<p>LOGIN PAGE</p>} />
        <Route element={<RequireAuth />}>
          <Route path="/secret" element={<p>SECRET</p>} />
          <Route element={<RequireOrganizer />}>
            <Route path="/secret-organizer" element={<p>ORGANIZER SECRET</p>} />
          </Route>
        </Route>
      </Routes>
    </MemoryRouter>,
  )
}

afterEach(() => {
  useAuth.getState().clear()
})

describe('RequireAuth', () => {
  it('waits, rather than redirecting, until the first refresh has finished', () => {
    useAuth.setState({ status: 'unknown', user: null, accessToken: null })
    renderGuarded()

    expect(screen.getByText('Loading…')).toBeInTheDocument()
    expect(screen.queryByText('LOGIN PAGE')).not.toBeInTheDocument()
  })

  it('sends an anonymous visitor to the login page', () => {
    useAuth.getState().clear()
    renderGuarded()

    expect(screen.getByText('LOGIN PAGE')).toBeInTheDocument()
    expect(screen.queryByText('SECRET')).not.toBeInTheDocument()
  })

  it('lets a signed-in user through', () => {
    useAuth.getState().setSession(session('t'))
    renderGuarded()

    expect(screen.getByText('SECRET')).toBeInTheDocument()
  })
})

describe('RequireOrganizer', () => {
  const at = (role: 'user' | 'organizer' | 'admin') => {
    useAuth.getState().setSession(session('t', role))
    return render(
      <MemoryRouter initialEntries={['/secret-organizer']}>
        <Routes>
          <Route element={<RequireAuth />}>
            <Route element={<RequireOrganizer />}>
              <Route path="/secret-organizer" element={<p>ORGANIZER SECRET</p>} />
            </Route>
          </Route>
        </Routes>
      </MemoryRouter>,
    )
  }

  it('turns an ordinary user away', () => {
    at('user')
    expect(screen.queryByText('ORGANIZER SECRET')).not.toBeInTheDocument()
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  it.each(['organizer', 'admin'] as const)('lets a %s in', (role) => {
    at(role)
    expect(screen.getByText('ORGANIZER SECRET')).toBeInTheDocument()
  })
})
