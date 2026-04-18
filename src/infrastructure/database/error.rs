use sqlx::Error;

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
