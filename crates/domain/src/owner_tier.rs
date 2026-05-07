use crate::owner::Owner;
use crate::tier::Tier;

use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerTier {
    owner: Owner,
    tier: Tier,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl OwnerTier {
    pub fn new(
        owner: Owner,
        tier: Tier,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime,
    ) -> Self {
        Self {
            owner,
            tier,
            created_at,
            updated_at,
        }
    }

    pub fn tier(&self) -> Tier {
        self.tier
    }
}
