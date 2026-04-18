DROP INDEX IF EXISTS idx_idempotency_keys_expires_at;

CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);
