use crate::ports::{AccountRepository, BalanceCachePort, OperationError};
use domain::{account::Account, owner::Owner};
use std::sync::Arc;

pub struct GetAccountsInput {
    pub owner: Owner,
}

pub struct GetAccountsUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl GetAccountsUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        balance_cache: Arc<dyn BalanceCachePort>,
    ) -> Self {
        Self {
            account_repository,
            balance_cache,
        }
    }

    pub async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, OperationError> {
        let GetAccountsInput { owner } = input;
        let accounts = self.account_repository.find_by_owner(&owner).await?;

        for account in &accounts {
            if let Err(e) = self
                .balance_cache
                .set(account.id(), account.balance().as_u64())
                .await
            {
                tracing::warn!(error = %e, "failed to set balance in cache")
            };
        }

        Ok(accounts)
    }
}
