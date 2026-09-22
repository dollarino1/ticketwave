/// <reference types="vitest/config" />
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// In dev the browser talks to Vite, and Vite forwards /api to the gateway. To the
// browser everything is one origin, exactly as it will be behind nginx in production,
// so there is no CORS to configure and the SameSite=Strict cookie just works.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    // Order matters: the first matching rule wins. The live seat stream is served by
    // realtime-svc; everything else under /api is the gateway. Same split as nginx.
    proxy: {
      '^/api/events/[^/]+/stream$': 'http://localhost:8082',
      '/api': 'http://localhost:8081',
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
    css: false,
  },
})
