-- The publisher polls "WHERE published = false ORDER BY created_at". A partial
-- index holds only the unpublished rows, so it stays tiny however large the
-- table of already-published history grows.
ALTER TABLE outbox ADD COLUMN published_at TIMESTAMPTZ;

CREATE INDEX outbox_unpublished_idx ON outbox (created_at, id) WHERE published = false;
