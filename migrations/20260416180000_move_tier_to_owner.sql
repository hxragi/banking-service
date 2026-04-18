CREATE TABLE owner_tiers (
    owner_type VARCHAR(10) NOT NULL CHECK (owner_type IN ('user', 'org')),
    owner_id VARCHAR(255) NOT NULL,
    tier INTEGER NOT NULL DEFAULT 1 CHECK (tier IN (1, 2, 3)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_type, owner_id)
);

CREATE INDEX idx_owner_tiers_lookup ON owner_tiers (owner_type, owner_id);

INSERT INTO owner_tiers (owner_type, owner_id, tier)
SELECT 
    CASE WHEN user_id IS NOT NULL THEN 'user' ELSE 'org' END as owner_type,
    COALESCE(user_id, org_id) as owner_id,
    MAX(tier) as tier
FROM accounts
GROUP BY 
    CASE WHEN user_id IS NOT NULL THEN 'user' ELSE 'org' END,
    COALESCE(user_id, org_id);

COMMENT ON COLUMN owner_tiers.tier IS '1 = Basic (1 account), 2 = Premium (3 accounts), 3 = Elite (unlimited accounts)';

ALTER TABLE accounts DROP COLUMN tier;

COMMENT ON TABLE accounts IS 'User/org accounts. Tier is stored at owner level in owner_tiers table';
