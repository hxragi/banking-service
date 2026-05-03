-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);
