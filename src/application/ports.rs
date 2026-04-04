use async_trait::async_trait;
use thiserror::Error;

use crate::domain::account::Account;
use crate::domain::account_number::AccountNumber;
use crate::domain::owner::Owner;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AccountRepositoryError {
    #[error("operation failed")]
    OperationFailed,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AccountNumberGeneratorError {
    #[error("generation failed")]
    GenerationFailed,
}

#[async_trait]
pub trait AccountRepository {
    async fn count_by_owner(&self, owner: &Owner) -> Result<u64, AccountRepositoryError>;
    async fn save(&self, account: &Account) -> Result<(), AccountRepositoryError>;
}

#[async_trait]
pub trait AccountNumberGenerator {
    async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError>;
}
