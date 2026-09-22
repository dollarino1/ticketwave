// These mirror the gateway's JSON exactly (services/gateway/internal/httpapi).

export type Role = 'user' | 'organizer' | 'admin'

export interface User {
  id: string
  email: string
  role: Role
}

export interface Session {
  access_token: string
  expires_in: number
  user: User
}

export interface EventSummary {
  id: string
  name: string
  total_seats: number
  available_seats: number
  seat_price_cents: number
  created_at: string
}

export type SeatStatus = 'available' | 'held' | 'sold' | 'unknown'

export interface Seat {
  id: string
  label: string
  status: SeatStatus
}

export type OrderStatus = 'pending' | 'confirmed' | 'failed' | 'unknown'

export interface Order {
  order_id: string
  event_id?: string
  seat_ids?: string[]
  amount_cents?: number
  status: OrderStatus
}

export interface EventStats {
  event_id: string
  event_name: string
  tickets_sold: number
  revenue_cents: number
  orders_confirmed: number
  orders_failed: number
  updated_at: string
}
