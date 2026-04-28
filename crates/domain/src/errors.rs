use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid account number")]
    InvalidAccountNumber,
    #[error("invalid amount")]
    InvalidAmount,
    #[error("invalid user id")]
    InvalidUserId,
    #[error("invalid org id")]
    InvalidOrgId,
    #[error("same account transfer")]
    SameAccountTransfer,
}
