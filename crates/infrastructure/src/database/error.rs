use application::database_error::DatabaseError;
use sqlx::Error;

pub fn from_sqlx(err: &Error) -> DatabaseError {
    match err {
        Error::Database(db_err) => match db_err.code().as_deref() {
            Some("23505") => DatabaseError::UniqueConstraintViolation(db_err.message().to_string()),
            Some("23503") => {
                DatabaseError::ForeignKeyConstraintViolation(db_err.message().to_string())
            }
            Some("23514") => DatabaseError::CheckConstraintViolation(db_err.message().to_string()),
            Some("40P01") => DatabaseError::Deadlock,
            Some("40001") => DatabaseError::SerializationFailure,
            Some("57014") => DatabaseError::LockTimeout,
            Some("08006") | Some("08001") | Some("08004") => {
                DatabaseError::ConnectionError(db_err.message().to_string())
            }
            Some(c) => DatabaseError::OperationFailed {
                operation: "database".to_string(),
                reason: format!("database error (code {}): {}", c, db_err.message()),
            },
            None => DatabaseError::OperationFailed {
                operation: "database".to_string(),
                reason: format!("database error: {}", db_err.message()),
            },
        },
        Error::PoolTimedOut => {
            DatabaseError::ConnectionError("connection pool timeout".to_string())
        }
        Error::Io(io_err) => DatabaseError::ConnectionError(format!("I/O error: {}", io_err)),
        other => DatabaseError::OperationFailed {
            operation: "database".to_string(),
            reason: other.to_string(),
        },
    }
}
