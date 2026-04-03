use thiserror::Error;
use tokio::sync::oneshot::error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid account number")]
    InvalidAccountNumber,
    #[error("invalid amount")]
    InvalidAmount,
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("invalid user id")]
    InvalidUserId,
    #[error("invalid org id")]
    InvalidOrgId,
    #[error("tier limit exceeded")]
    TierLimitExceeded,
    #[error("same account transfer")]
    SameAccountTransfer,
}
