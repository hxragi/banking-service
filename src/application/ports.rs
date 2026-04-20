use async_trait::async_trait;
use thiserror::Error;
use uuid::Uuid;

use crate::domain::account::Account;
use crate::domain::account_number::AccountNumber;
use crate::domain::owner::Owner;
use crate::domain::transaction_event::TransactionEvent;

#[derive(Debug, Clone)]
pub struct TransactionWithAccounts {
    pub transaction: crate::domain::transaction::Transaction,
    pub from_account_number: Option<String>,
    pub to_account_number: Option<String>,
}

pub struct PaginatedTransactions {
    pub transactions: Vec<TransactionWithAccounts>,
    pub total_count: u64,
    pub page: u32,
    pub page_size: u32,
    pub has_more: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AccountRepositoryError {
    #[error("operation failed: {operation} - {reason}")]
    OperationFailed { operation: String, reason: String },
    #[error("lock timeout")]
    LockTimeout,
    #[error("deadlock detected")]
    Deadlock,
    #[error("serialization failure")]
    SerializationFailure,
    #[error("unique constraint violation: {0}")]
    UniqueConstraintViolation(String),
    #[error("connection error: {0}")]
    ConnectionError(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AccountNumberGeneratorError {
    #[error("generation failed")]
    GenerationFailed,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OwnerTierRepositoryError {
    #[error("operation failed: {operation} - {reason}")]
    OperationFailed { operation: String, reason: String },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransactionRepositoryError {
    #[error("transaction failed")]
    TransactionFailed,
    #[error("lock timeout - operation should be retried")]
    LockTimeout,
    #[error("deadlock detected")]
    Deadlock,
    #[error("serialization failure")]
    SerializationFailure,
    #[error("unique constraint violation: {0}")]
    UniqueConstraintViolation(String),
    #[error("check constraint violation: {0}")]
    CheckConstraintViolation(String),
    #[error("connection error: {0}")]
    ConnectionError(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IdempotencyError {
    #[error("idempotency failed")]
    IdempotencyFailed,
    #[error("idempotency key already exists with response: {response}")]
    KeyAlreadyExists { response: String },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransactionError {
    #[error("transaction commit failed: {0}")]
    CommitFailed(String),
    #[error("transaction rollback failed: {0}")]
    RollbackFailed(String),
}

#[async_trait]
pub trait AccountRepository: Send + Sync {
    async fn count_by_owner(&self, owner: &Owner) -> Result<u64, AccountRepositoryError>;
    async fn create(&self, account: &Account) -> Result<(), AccountRepositoryError>;
    async fn find_by_number(
        &self,
        number: &AccountNumber,
    ) -> Result<Option<Account>, AccountRepositoryError>;
    async fn find_by_owner(&self, owner: &Owner) -> Result<Vec<Account>, AccountRepositoryError>;
}

#[async_trait]
pub trait AccountNumberGenerator: Send + Sync {
    async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError>;
}

#[async_trait]
pub trait TransactionRepository: Send + Sync {
    async fn find_by_account_id_paginated(
        &self,
        account_id: Uuid,
        page: u32,
        page_size: u32,
    ) -> Result<PaginatedTransactions, TransactionRepositoryError>;
    async fn count_by_account_id(
        &self,
        account_id: Uuid,
    ) -> Result<u64, TransactionRepositoryError>;
}

#[async_trait]
pub trait IdempotencyRepository: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>, IdempotencyError>;
    async fn save(&self, key: &str, response: &str) -> Result<(), IdempotencyError>;
}

#[async_trait]
pub trait IdempotencyTxRepository<T>: Send + Sync
where
    T: Transaction,
{
    async fn save_in_tx(
        &self,
        key: &str,
        response: &str,
        tx: &mut T,
    ) -> Result<(), IdempotencyError>;
}

use crate::domain::owner_tier::OwnerTier;
use crate::domain::tier::Tier;

#[async_trait]
pub trait OwnerTierRepository: Send + Sync {
    async fn get_or_default(&self, owner: &Owner) -> Result<OwnerTier, OwnerTierRepositoryError>;

    async fn set_tier(
        &self,
        owner: &Owner,
        tier: Tier,
    ) -> Result<OwnerTier, OwnerTierRepositoryError>;
}

#[async_trait]
pub trait BalanceCachePort: Send + Sync {
    #[must_use = "cache operations can fail silently if ignored"]
    async fn get(&self, account_id: &Uuid) -> Option<u64>;
    #[must_use = "cache operations can fail silently if ignored"]
    async fn set(&self, account_id: Uuid, balance: u64);
    #[must_use = "cache operations can fail silently if ignored"]
    async fn invalidate(&self, account_id: &Uuid);
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EventPublishError {
    #[error("publish failed: {0}")]
    PublishFailed(String),
    #[error("connection error: {0}")]
    ConnectionError(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
}

#[async_trait]
pub trait AccountTxRepository<Tx>: Send + Sync {
    async fn find_by_number_for_update(
        &self,
        tx: &mut Tx,
        account_number: &str,
    ) -> Result<Option<Account>, AccountRepositoryError>;

    async fn update_balance(
        &self,
        tx: &mut Tx,
        account_id: Uuid,
        balance: u64,
    ) -> Result<(), AccountRepositoryError>;

    async fn lock_for_update_by_numbers(
        &self,
        tx: &mut Tx,
        first_number: &str,
        second_number: &str,
    ) -> Result<Vec<Account>, AccountRepositoryError>;
}

#[async_trait]
pub trait TransactionWriteRepository<Tx>: Send + Sync {
    async fn create(
        &self,
        tx: &mut Tx,
        transaction: &crate::domain::transaction::Transaction,
    ) -> Result<(), TransactionRepositoryError>;
}

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(
        &self,
        topic: &str,
        event: &TransactionEvent,
    ) -> Result<(), EventPublishError>;
}

pub trait MetricsPort: Send + Sync {
    fn increment_operation(&self, operation: &str, status: &str);
    fn record_error(&self, error_type: &str, operation: &str);
}

pub trait TransactionPort: Send + Sync + Clone {
    type Transaction: Transaction;

    fn begin(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Transaction, TransactionError>> + Send;
}

#[allow(dead_code)]
pub trait Transaction: Send + Sync {
    fn commit(self) -> impl std::future::Future<Output = Result<(), TransactionError>> + Send;

    fn rollback(self) -> impl std::future::Future<Output = Result<(), TransactionError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryOperation {
    CountByOwner,
    CreateAccount,
    FindByNumber,
    FindByOwner,
    FindTransactions,

    GetTier,
    SetTier,
    GenerateAccountNumber,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OperationError {
    #[error("resource not found: {resource}")]
    NotFound { resource: String },
    #[error("service unavailable: {reason}")]
    Unavailable { reason: String },
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("invalid input: {field} - {reason}")]
    InvalidInput { field: String, reason: String },
    #[error("repository error: {operation:?}")]
    RepositoryError { operation: RepositoryOperation },
    #[error("tier limit exceeded")]
    TierLimitExceeded,
    #[error("tier downgrade not allowed: {reason}")]
    TierDowngradeNotAllowed { reason: String },
    #[error("lock timeout - operation should be retried")]
    LockTimeout,
    #[error("deadlock detected - operation should be retried")]
    Deadlock,
    #[error("serialization failure - operation should be retried")]
    SerializationFailure,
    #[error("unique constraint violation: {0}")]
    UniqueConstraintViolation(String),
    #[error("connection error: {0}")]
    ConnectionError(String),
    #[error("idempotency error: {reason}")]
    IdempotencyError { reason: String },
}

impl From<AccountRepositoryError> for OperationError {
    fn from(err: AccountRepositoryError) -> Self {
        match err {
            AccountRepositoryError::LockTimeout => OperationError::LockTimeout,
            AccountRepositoryError::Deadlock => OperationError::Deadlock,
            AccountRepositoryError::SerializationFailure => OperationError::SerializationFailure,
            AccountRepositoryError::UniqueConstraintViolation(msg) => {
                OperationError::UniqueConstraintViolation(msg)
            }
            AccountRepositoryError::ConnectionError(msg) => OperationError::ConnectionError(msg),
            AccountRepositoryError::OperationFailed { operation, .. } => {
                let op = match operation.as_str() {
                    "count_by_owner" => RepositoryOperation::CountByOwner,
                    "create" => RepositoryOperation::CreateAccount,
                    "find_by_number" => RepositoryOperation::FindByNumber,
                    "find_by_owner" => RepositoryOperation::FindByOwner,
                    _ => RepositoryOperation::CreateAccount,
                };
                OperationError::RepositoryError { operation: op }
            }
        }
    }
}

impl From<TransactionRepositoryError> for OperationError {
    fn from(err: TransactionRepositoryError) -> Self {
        match err {
            TransactionRepositoryError::LockTimeout => OperationError::LockTimeout,
            TransactionRepositoryError::Deadlock => OperationError::Deadlock,
            TransactionRepositoryError::SerializationFailure => OperationError::SerializationFailure,
            TransactionRepositoryError::UniqueConstraintViolation(msg) => {
                OperationError::UniqueConstraintViolation(msg)
            }
            TransactionRepositoryError::ConnectionError(msg) => {
                OperationError::ConnectionError(msg)
            }
            _ => OperationError::RepositoryError {
                operation: RepositoryOperation::FindTransactions,
            },
        }
    }
}

impl From<AccountNumberGeneratorError> for OperationError {
    fn from(_err: AccountNumberGeneratorError) -> Self {
        OperationError::RepositoryError {
            operation: RepositoryOperation::GenerateAccountNumber,
        }
    }
}

impl From<OwnerTierRepositoryError> for OperationError {
    fn from(err: OwnerTierRepositoryError) -> Self {
        match err {
            OwnerTierRepositoryError::OperationFailed { operation, .. } => {
                let op = match operation.as_str() {
                    "get_or_default" => RepositoryOperation::GetTier,
                    "set_tier" => RepositoryOperation::SetTier,
                    _ => RepositoryOperation::GetTier,
                };
                OperationError::RepositoryError { operation: op }
            }
        }
    }
}
