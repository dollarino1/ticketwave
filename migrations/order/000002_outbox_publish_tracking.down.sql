DROP INDEX outbox_unpublished_idx;

ALTER TABLE outbox DROP COLUMN published_at;
