use sqlx::Error;

use crate::application::ports::{AccountRepositoryError, TransactionRepositoryError};

pub fn classify(err: &Error, operation: &str) -> String {
    match err {
        Error::Database(db_err) => match db_err.code().as_deref() {
            Some("23505") => format!("{}: unique constraint violation", operation),
            Some("23503") => format!("{}: foreign key constraint violation", operation),
            Some("23514") => format!("{}: check constraint violation", operation),
            Some("40P01") => format!("{}: deadlock detected", operation),
            Some("40001") => format!("{}: serialization failure", operation),
            Some("57014") => format!("{}: lock timeout", operation),
            Some(c) => format!(
                "{}: database error (code {}): {}",
                operation,
                c,
                db_err.message()
            ),
            None => format!("{}: database error: {}", operation, db_err.message()),
        },
        Error::PoolTimedOut => format!("{}: connection pool timeout", operation),
        Error::Io(io_err) => format!("{}: I/O error: {}", operation, io_err),
        other => format!("{}: unexpected error: {}", operation, other),
    }
}

pub fn map_sqlx_to_account_error(err: sqlx::Error) -> AccountRepositoryError {
    match &err {
        sqlx::Error::Database(db_err) => match db_err.code().as_deref() {
            Some("23505") => {
                AccountRepositoryError::UniqueConstraintViolation(db_err.message().to_string())
            }
            Some("40P01") => AccountRepositoryError::Deadlock,
            Some("40001") => AccountRepositoryError::SerializationFailure,
            Some("57014") => AccountRepositoryError::LockTimeout,
            Some("08006") | Some("08001") | Some("08004") => {
                AccountRepositoryError::ConnectionError(db_err.message().to_string())
            }
            _ => AccountRepositoryError::OperationFailed {
                operation: "database".to_string(),
                reason: classify(&err, "database"),
            },
        },
        sqlx::Error::PoolTimedOut => {
            AccountRepositoryError::ConnectionError("connection pool timeout".to_string())
        }
        sqlx::Error::Io(io_err) => {
            AccountRepositoryError::ConnectionError(format!("I/O error: {}", io_err))
        }
        _ => AccountRepositoryError::OperationFailed {
            operation: "database".to_string(),
            reason: classify(&err, "database"),
        },
    }
}

pub fn map_sqlx_to_transaction_error(err: sqlx::Error) -> TransactionRepositoryError {
    match &err {
        sqlx::Error::Database(db_err) => match db_err.code().as_deref() {
            Some("23505") => {
                TransactionRepositoryError::UniqueConstraintViolation(db_err.message().to_string())
            }
            Some("23514") => {
                TransactionRepositoryError::CheckConstraintViolation(db_err.message().to_string())
            }
            Some("40P01") => TransactionRepositoryError::Deadlock,
            Some("40001") => TransactionRepositoryError::SerializationFailure,
            Some("57014") => TransactionRepositoryError::LockTimeout,
            Some("08006") | Some("08001") | Some("08004") => {
                TransactionRepositoryError::ConnectionError(db_err.message().to_string())
            }
            _ => TransactionRepositoryError::TransactionFailed(classify(&err, "database")),
        },
        sqlx::Error::PoolTimedOut => {
            TransactionRepositoryError::ConnectionError("connection pool timeout".to_string())
        }
        sqlx::Error::Io(io_err) => {
            TransactionRepositoryError::ConnectionError(format!("I/O error: {}", io_err))
        }
        _ => TransactionRepositoryError::TransactionFailed(classify(&err, "database")),
    }
}
