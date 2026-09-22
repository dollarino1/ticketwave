-- Transactional outbox for seat-state changes. A row is written in the same
-- transaction as the seat UPDATE, so "the seat changed" and "an event about it will
-- be published" either both happen or neither does. pkg/outbox relays the rows.
CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(id),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT false,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The publisher polls "WHERE published = false ORDER BY created_at". A partial
-- index holds only the unpublished rows, so it stays tiny however large the pile
-- of already-published history grows.
CREATE INDEX outbox_unpublished_idx ON outbox (created_at, id) WHERE published = false;
