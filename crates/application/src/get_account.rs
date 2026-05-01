use std::sync::Arc;

use crate::ports::{AccountRepository, BalanceCachePort, OperationError};
use domain::{account::Account, account_number::AccountNumber};

pub struct GetAccountInput {
    pub account_number: AccountNumber,
}

pub struct GetAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
}

impl GetAccountUseCase {
    pub fn new(account_repository: Arc<dyn AccountRepository + Send + Sync>) -> Self {
        Self { account_repository }
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

        Ok(account)
    }
}

#[async_trait::async_trait]
pub trait GetAccountPort: Send + Sync {
    async fn execute(&self, input: GetAccountInput) -> Result<Account, OperationError>;
}

#[async_trait::async_trait]
impl GetAccountPort for GetAccountUseCase {
    async fn execute(&self, input: GetAccountInput) -> Result<Account, OperationError> {
        self.execute(input).await
    }
}

pub struct CachingGetAccountUseCase {
    inner: Arc<dyn GetAccountPort>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl CachingGetAccountUseCase {
    pub fn new(inner: Arc<dyn GetAccountPort>, balance_cache: Arc<dyn BalanceCachePort>) -> Self {
        Self {
            inner,
            balance_cache,
        }
    }
}

#[async_trait::async_trait]
impl GetAccountPort for CachingGetAccountUseCase {
    async fn execute(&self, input: GetAccountInput) -> Result<Account, OperationError> {
        let account = self.inner.execute(input).await?;

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
