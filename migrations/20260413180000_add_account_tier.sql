ALTER TABLE accounts ADD COLUMN tier INTEGER NOT NULL DEFAULT 1;
CREATE INDEX idx_accounts_tier ON accounts (tier);
COMMENT ON COLUMN accounts.tier IS '1 = Basic (1 account), 2 = Premium (3 accounts), 3 = Elite (unlimited accounts)';
