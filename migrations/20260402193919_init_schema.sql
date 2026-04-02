CREATE TYPE transaction_kind AS ENUM (
  'deposit',
  'withdraw',
  'transfer'
);

CREATE TABLE accounts (
  id UUID PRIMARY KEY,
  number TEXT NOT NULL UNIQUE,
  user_id TEXT NULL,
  org_id TEXT NULL,
  balance BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT check_accounts_balance_non_negative
    CHECK (balance >= 0),

  CONSTRAINT check_accounts_owner_xor
    CHECK (
      (user_id IS NOT NULL AND org_id IS NULL) OR
      (user_id IS NULL AND org_id IS NOT NULL)
    )
);

CREATE TABLE transactions (
  id UUID PRIMARY KEY,
  kind transaction_kind NOT NULL,
  amount BIGINT NOT NULL,
  from_account_id UUID NULL,
  to_account_id UUID NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT check_transactions_amount_positive
    CHECK (amount > 0),

  CONSTRAINT check_transactions_kind_logic
    CHECK (
      (kind = 'deposit' AND from_account_id IS NULL AND to_account_id IS NOT NULL) OR
      (kind = 'withdraw' AND from_account_id IS NOT NULL AND to_account_id IS NULL) OR
      (kind = 'transfer' AND from_account_id IS NOT NULL AND to_account_id IS NOT NULL AND from_account_id <> to_account_id)
    ),

  CONSTRAINT fk_transactions_from_account
    FOREIGN KEY(from_account_id) 
    REFERENCES accounts(id)
    ON DELETE RESTRICT,

  CONSTRAINT fk_transactions_to_account
    FOREIGN KEY(to_account_id)
    REFERENCES accounts(id)
    ON DELETE RESTRICT
);

CREATE INDEX idx_accounts_user_id
  ON accounts (user_id)
  WHERE user_id IS NOT NULL;

CREATE INDEX idx_accounts_org_id 
  ON accounts (org_id)
  WHERE org_id IS NOT NULL;

CREATE INDEX idx_transactions_from_account_id
  ON transactions (from_account_id)
  WHERE from_account_id IS NOT NULL;

CREATE INDEX idx_transactions_to_account_id
  ON transactions (to_account_id)
  WHERE to_account_id IS NOT NULL;
