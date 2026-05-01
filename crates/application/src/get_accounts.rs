use crate::ports::{AccountRepository, BalanceCachePort, OperationError};
use domain::{account::Account, owner::Owner};
use std::sync::Arc;

pub struct GetAccountsInput {
    pub owner: Owner,
}

pub struct GetAccountsUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
}

impl GetAccountsUseCase {
    pub fn new(account_repository: Arc<dyn AccountRepository + Send + Sync>) -> Self {
        Self { account_repository }
    }

    pub async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, OperationError> {
        let GetAccountsInput { owner } = input;
        let accounts = self.account_repository.find_by_owner(&owner).await?;

        Ok(accounts)
    }
}

#[async_trait::async_trait]
pub trait GetAccountsPort: Send + Sync {
    async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, OperationError>;
}

#[async_trait::async_trait]
impl GetAccountsPort for GetAccountsUseCase {
    async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, OperationError> {
        self.execute(input).await
    }
}

pub struct CachingGetAccountsUseCase {
    inner: Arc<dyn GetAccountsPort>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl CachingGetAccountsUseCase {
    pub fn new(inner: Arc<dyn GetAccountsPort>, balance_cache: Arc<dyn BalanceCachePort>) -> Self {
        Self {
            inner,
            balance_cache,
        }
    }
}

#[async_trait::async_trait]
impl GetAccountsPort for CachingGetAccountsUseCase {
    async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, OperationError> {
        let accounts = self.inner.execute(input).await?;

        for account in &accounts {
            if let Err(e) = self
                .balance_cache
                .set(account.id(), account.balance().as_u64())
                .await
            {
                tracing::warn!(error = %e, "failed to set balance in cache")
            }
        }

        Ok(accounts)
    }
}
