use time::OffsetDateTime;

use crate::domain::owner::Owner;
use crate::domain::tier::Tier;

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

    #[allow(dead_code)]
    pub fn account_limit(&self) -> u64 {
        self.tier.account_limit().unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::user_id::UserId;

    #[test]
    fn account_limit_basic() {
        let owner = Owner::User(UserId::new("user-1").unwrap());
        let owner_tier = OwnerTier::new(
            owner,
            Tier::Basic,
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(owner_tier.account_limit(), 1);
    }

    #[test]
    fn account_limit_premium() {
        let owner = Owner::User(UserId::new("user-1").unwrap());
        let owner_tier = OwnerTier::new(
            owner,
            Tier::Premium,
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(owner_tier.account_limit(), 3);
    }

    #[test]
    fn account_limit_elite() {
        let owner = Owner::User(UserId::new("user-1").unwrap());
        let owner_tier = OwnerTier::new(
            owner,
            Tier::Elite,
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(owner_tier.account_limit(), u64::MAX);
    }
}
