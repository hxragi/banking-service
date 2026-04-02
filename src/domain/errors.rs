use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid account number")]
    InvalidAccountNumber,
    #[error("invalid amount")]
    InvalidAmount,
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("tier limit exceeded")]
    TierLimitExceeded,
}
