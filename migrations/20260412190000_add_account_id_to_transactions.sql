ALTER TABLE transactions ADD COLUMN IF NOT EXISTS account_id UUID;

UPDATE transactions
SET account_id = COALESCE(from_account_id, to_account_id)
WHERE account_id IS NULL;

ALTER TABLE transactions ALTER COLUMN account_id SET NOT NULL;

DROP INDEX IF EXISTS idx_transactions_from_account_id;
DROP INDEX IF EXISTS idx_transactions_to_account_id;

CREATE INDEX IF NOT EXISTS idx_transactions_account_id_created_at
ON transactions (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_from_created_at
ON transactions (from_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_to_created_at
ON transactions (to_account_id, created_at DESC);
