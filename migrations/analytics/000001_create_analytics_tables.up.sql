-- The projection: one row per concert, updated as order events arrive. It is
-- derived data. Dropping it loses nothing that replaying order-events cannot restore.
CREATE TABLE event_stats (
    event_id UUID PRIMARY KEY,
    tickets_sold BIGINT NOT NULL DEFAULT 0,
    revenue_cents BIGINT NOT NULL DEFAULT 0,
    orders_confirmed BIGINT NOT NULL DEFAULT 0,
    orders_failed BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every message that has been counted. A row is inserted in the same transaction
-- as the counter update, which is what makes a redelivered message harmless: the
-- second insert conflicts and the counters are not touched again.
--
-- This table grows with traffic. Rows older than Kafka's retention window can
-- never be redelivered, so they could be pruned; nothing does that yet.
CREATE TABLE processed_messages (
    message_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
