ALTER TABLE transactions ADD COLUMN IF NOT EXISTS account_id UUID NOT NULL;

DROP INDEX IF EXISTS idx_transactions_from_account_id;
DROP INDEX IF EXISTS idx_transactions_to_account_id;

CREATE INDEX IF NOT EXISTS idx_transactions_account_id_created_at 
ON transactions (account_id, created_at DESC);