use std::sync::Arc;

use domain::{account::Account, balance::Balance, owner::Owner};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::ports::{
    AccountNumberGenerator, AccountRepository, OperationError, OwnerTierRepository,
};

pub struct CreateAccountInput {
    pub owner: Owner,
}

pub struct CreateAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    account_number_generator: Arc<dyn AccountNumberGenerator + Send + Sync>,
    owner_tier_repository: Arc<dyn OwnerTierRepository + Send + Sync>,
}

impl CreateAccountUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        account_number_generator: Arc<dyn AccountNumberGenerator + Send + Sync>,
        owner_tier_repository: Arc<dyn OwnerTierRepository + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            account_number_generator,
            owner_tier_repository,
        }
    }

    pub async fn execute(&self, input: CreateAccountInput) -> Result<Account, OperationError> {
        let CreateAccountInput { owner } = input;

        let owner_tier = self.owner_tier_repository.get_or_default(&owner).await?;

        let tier = owner_tier.tier();
        let limit = tier.account_limit();

        let number = self.account_number_generator.generate().await?;
        let id = Uuid::new_v4();
        let balance = Balance::zero();
        let created_at = OffsetDateTime::now_utc();
        let account = Account::new(id, number, owner, balance, created_at);

        self.account_repository
            .create_within_limit(&account, limit)
            .await?;

        Ok(account)
    }
}
