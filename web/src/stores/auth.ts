import { create } from 'zustand'
import type { Session, User } from '../api/types'

// 'unknown' until the first silent refresh finishes, so the app can tell "not signed
// in" apart from "not checked yet" and avoid flashing the login page at a user who
// is in fact signed in.
type Status = 'unknown' | 'anonymous' | 'authenticated'

interface AuthState {
  status: Status
  user: User | null
  // The access token lives only in memory. It is deliberately not put in
  // localStorage, where any script on the page could read it. A reload loses it and
  // the httpOnly refresh cookie quietly gets a new one.
  accessToken: string | null
  setSession: (session: Session) => void
  clear: () => void
}

export const useAuth = create<AuthState>()((set) => ({
  status: 'unknown',
  user: null,
  accessToken: null,
  setSession: (session) => {
    set({ status: 'authenticated', user: session.user, accessToken: session.access_token })
  },
  clear: () => {
    set({ status: 'anonymous', user: null, accessToken: null })
  },
}))
