use std::sync::Arc;

use crate::ports::{AccountRepository, BalanceCachePort, OperationError};
use domain::{account::Account, account_number::AccountNumber};

pub struct GetAccountInput {
    pub account_number: AccountNumber,
}

pub struct GetAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl GetAccountUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        balance_cache: Arc<dyn BalanceCachePort>,
    ) -> Self {
        Self {
            account_repository,
            balance_cache,
        }
    }

    pub async fn execute(&self, input: GetAccountInput) -> Result<Account, OperationError> {
        let GetAccountInput { account_number } = input;

        let account = self
            .account_repository
            .find_by_number(&account_number)
            .await?
            .ok_or(OperationError::NotFound {
                resource: "account".to_string(),
            })?;

        if let Err(e) = self
            .balance_cache
            .set(account.id(), account.balance().as_u64())
            .await
        {
            tracing::warn!(error = %e, "failed to set balance in cache")
        };

        Ok(account)
    }
}
