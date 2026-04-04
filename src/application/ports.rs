use async_trait::async_trait;
use thiserror::Error;

use crate::domain::account::Account;
use crate::domain::account_number::AccountNumber;
use crate::domain::owner::Owner;
use crate::domain::transaction::Transaction;

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

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransactionRepositoryError {
    #[error("transaction failed")]
    TransactionFailed,
}

#[async_trait]
pub trait AccountRepository {
    async fn count_by_owner(&self, owner: &Owner) -> Result<u64, AccountRepositoryError>;
    async fn create(&self, account: &Account) -> Result<(), AccountRepositoryError>;
    async fn find_by_number(
        &self,
        number: &AccountNumber,
    ) -> Result<Option<Account>, AccountRepositoryError>;
    async fn update(&self, account: &Account) -> Result<(), AccountRepositoryError>;
    async fn find_by_owner(&self, owner: &Owner) -> Result<Vec<Account>, AccountRepositoryError>;
}

#[async_trait]
pub trait AccountNumberGenerator {
    async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError>;
}

#[async_trait]
pub trait TransactionRepository {
    async fn create(&self, transaction: &Transaction) -> Result<(), TransactionRepositoryError>;
}
