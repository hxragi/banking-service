use std::fmt::{Display, Formatter, Result};

use sqlx::Error;

use application::ports::{AccountRepositoryError, TransactionRepositoryError};

#[derive(Debug, Clone)]
pub enum SqlxErrorKind {
    UniqueConstraintViolation(String),
    ForeignKeyConstraintViolation(String),
    CheckConstraintViolation(String),
    Deadlock,
    SerializationFailure,
    LockTimeout,
    ConnectionError(String),
    Other(String),
}

impl Display for SqlxErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            SqlxErrorKind::UniqueConstraintViolation(msg) => {
                write!(f, "unique constraint violation: {}", msg)
            }
            SqlxErrorKind::ForeignKeyConstraintViolation(msg) => {
                write!(f, "foreign key constraint violation: {}", msg)
            }
            SqlxErrorKind::CheckConstraintViolation(msg) => {
                write!(f, "check constraint violation: {}", msg)
            }
            SqlxErrorKind::Deadlock => write!(f, "deadlock detected"),
            SqlxErrorKind::SerializationFailure => write!(f, "serialization failure"),
            SqlxErrorKind::LockTimeout => write!(f, "lock timeout"),
            SqlxErrorKind::ConnectionError(msg) => write!(f, "connection error: {}", msg),
            SqlxErrorKind::Other(msg) => write!(f, "{}", msg),
        }
    }
}

pub fn classify_sqlx_kind(err: &Error) -> SqlxErrorKind {
    match err {
        Error::Database(db_err) => match db_err.code().as_deref() {
            Some("23505") => SqlxErrorKind::UniqueConstraintViolation(db_err.message().to_string()),
            Some("23503") => {
                SqlxErrorKind::ForeignKeyConstraintViolation(db_err.message().to_string())
            }
            Some("23514") => SqlxErrorKind::CheckConstraintViolation(db_err.message().to_string()),
            Some("40P01") => SqlxErrorKind::Deadlock,
            Some("40001") => SqlxErrorKind::SerializationFailure,
            Some("57014") => SqlxErrorKind::LockTimeout,
            Some("08006") | Some("08001") | Some("08004") => {
                SqlxErrorKind::ConnectionError(db_err.message().to_string())
            }
            Some(c) => {
                SqlxErrorKind::Other(format!("database error (code {}): {}", c, db_err.message()))
            }
            None => SqlxErrorKind::Other(format!("database error: {}", db_err.message())),
        },
        Error::PoolTimedOut => {
            SqlxErrorKind::ConnectionError("connection pool timeout".to_string())
        }
        Error::Io(io_err) => SqlxErrorKind::ConnectionError(format!("I/O error: {}", io_err)),
        other => SqlxErrorKind::Other(other.to_string()),
    }
}

pub fn classify_sqlx(err: &Error, operation: &str) -> String {
    let kind = classify_sqlx_kind(err);
    format!("{}: {}", operation, kind)
}

pub fn map_sqlx_to_account_error(err: sqlx::Error) -> AccountRepositoryError {
    match classify_sqlx_kind(&err) {
        SqlxErrorKind::UniqueConstraintViolation(msg) => {
            AccountRepositoryError::UniqueConstraintViolation(msg)
        }
        SqlxErrorKind::Deadlock => AccountRepositoryError::Deadlock,
        SqlxErrorKind::SerializationFailure => AccountRepositoryError::SerializationFailure,
        SqlxErrorKind::LockTimeout => AccountRepositoryError::LockTimeout,
        SqlxErrorKind::ConnectionError(msg) => AccountRepositoryError::ConnectionError(msg),
        SqlxErrorKind::Other(reason) => AccountRepositoryError::OperationFailed {
            operation: "database".to_string(),
            reason,
        },
        SqlxErrorKind::ForeignKeyConstraintViolation(reason)
        | SqlxErrorKind::CheckConstraintViolation(reason) => {
            AccountRepositoryError::OperationFailed {
                operation: "database".to_string(),
                reason,
            }
        }
    }
}

pub fn map_sqlx_to_transaction_error(err: sqlx::Error) -> TransactionRepositoryError {
    match classify_sqlx_kind(&err) {
        SqlxErrorKind::UniqueConstraintViolation(msg) => {
            TransactionRepositoryError::UniqueConstraintViolation(msg)
        }
        SqlxErrorKind::CheckConstraintViolation(msg) => {
            TransactionRepositoryError::CheckConstraintViolation(msg)
        }
        SqlxErrorKind::Deadlock => TransactionRepositoryError::Deadlock,
        SqlxErrorKind::SerializationFailure => TransactionRepositoryError::SerializationFailure,
        SqlxErrorKind::LockTimeout => TransactionRepositoryError::LockTimeout,
        SqlxErrorKind::ConnectionError(msg) => TransactionRepositoryError::ConnectionError(msg),
        SqlxErrorKind::Other(reason) => TransactionRepositoryError::TransactionFailed(reason),
        SqlxErrorKind::ForeignKeyConstraintViolation(reason) => {
            TransactionRepositoryError::TransactionFailed(reason)
        }
    }
}
