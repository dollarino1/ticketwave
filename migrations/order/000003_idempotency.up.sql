-- Idempotency: a client may attach a key to CreateOrder, and a repeat of the same
-- request must return the original result instead of buying the seats again.
--
-- The key is scoped to the user, so two people can never collide with (or probe)
-- each other's keys. The unique index is what makes this safe under concurrency:
-- if two identical requests arrive at once, both try to INSERT and the database lets
-- exactly one through. Checking "does this key exist?" first and inserting after is
-- NOT safe, because both requests can pass the check before either inserts.
ALTER TABLE orders ADD COLUMN idempotency_key TEXT;

-- Why the order failed, kept so a replayed request can be answered with the same
-- reason ("card declined") the first attempt got, not a vague "failed".
ALTER TABLE orders ADD COLUMN failure_reason TEXT;

-- Partial: orders without a key (the common case for API clients that do not send
-- one) are not indexed at all, and NULLs never collide.
CREATE UNIQUE INDEX orders_idempotency_idx ON orders (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
