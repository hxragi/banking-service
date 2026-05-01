use thiserror::Error;

use crate::ports::{
    AccountRepositoryError, IdempotencyError, OwnerTierRepositoryError, TransactionRepositoryError,
};

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("unique constraint violation: {0}")]
    UniqueConstraintViolation(String),
    #[error("foreign key constraint violation: {0}")]
    ForeignKeyConstraintViolation(String),
    #[error("check constraint violation: {0}")]
    CheckConstraintViolation(String),
    #[error("deadlock detected")]
    Deadlock,
    #[error("serialization failure")]
    SerializationFailure,
    #[error("lock timeout")]
    LockTimeout,
    #[error("connection error: {0}")]
    ConnectionError(String),
    #[error("operation `{operation}` failed: {reason}")]
    OperationFailed { operation: String, reason: String },
}

impl DatabaseError {
    pub fn with_operation(self, operation: impl Into<String>) -> Self {
        match self {
            Self::OperationFailed { reason, .. } => Self::OperationFailed {
                operation: operation.into(),
                reason,
            },
            other => other,
        }
    }
}

impl From<DatabaseError> for AccountRepositoryError {
    fn from(err: DatabaseError) -> Self {
        match err {
            DatabaseError::UniqueConstraintViolation(msg) => Self::UniqueConstraintViolation(msg),
            DatabaseError::ForeignKeyConstraintViolation(reason)
            | DatabaseError::CheckConstraintViolation(reason) => Self::OperationFailed {
                operation: "database".to_string(),
                reason,
            },
            DatabaseError::Deadlock => Self::Deadlock,
            DatabaseError::SerializationFailure => Self::SerializationFailure,
            DatabaseError::LockTimeout => Self::LockTimeout,
            DatabaseError::ConnectionError(msg) => Self::ConnectionError(msg),
            DatabaseError::OperationFailed { operation, reason } => {
                Self::OperationFailed { operation, reason }
            }
        }
    }
}

impl From<DatabaseError> for TransactionRepositoryError {
    fn from(err: DatabaseError) -> Self {
        match err {
            DatabaseError::UniqueConstraintViolation(msg) => Self::UniqueConstraintViolation(msg),
            DatabaseError::CheckConstraintViolation(msg) => Self::CheckConstraintViolation(msg),
            DatabaseError::ForeignKeyConstraintViolation(reason) => Self::TransactionFailed(reason),
            DatabaseError::Deadlock => Self::Deadlock,
            DatabaseError::SerializationFailure => Self::SerializationFailure,
            DatabaseError::LockTimeout => Self::LockTimeout,
            DatabaseError::ConnectionError(msg) => Self::ConnectionError(msg),
            DatabaseError::OperationFailed { operation, reason } => {
                Self::TransactionFailed(format!("{}: {}", operation, reason))
            }
        }
    }
}

impl From<DatabaseError> for OwnerTierRepositoryError {
    fn from(err: DatabaseError) -> Self {
        Self::OperationFailed {
            operation: "database".to_string(),
            reason: err.to_string(),
        }
    }
}

impl From<DatabaseError> for IdempotencyError {
    fn from(_err: DatabaseError) -> Self {
        Self::IdempotencyFailed
    }
}
