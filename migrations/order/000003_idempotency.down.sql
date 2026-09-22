DROP INDEX orders_idempotency_idx;
ALTER TABLE orders DROP COLUMN failure_reason;
ALTER TABLE orders DROP COLUMN idempotency_key;
