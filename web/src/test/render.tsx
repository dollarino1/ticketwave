import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import type { ReactElement } from 'react'
import { MemoryRouter, Route, Routes } from 'react-router'

// renderAt mounts `ui` at `path` inside fresh routing and query state, and adds a
// sentinel route for every place the code under test might navigate to.
export function renderAt(path: string, routePattern: string, ui: ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route path={routePattern} element={ui} />
          <Route path="/orders/:id" element={<p>ORDER PAGE</p>} />
          <Route path="/login" element={<p>LOGIN PAGE</p>} />
          <Route path="/" element={<p>HOME PAGE</p>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}
