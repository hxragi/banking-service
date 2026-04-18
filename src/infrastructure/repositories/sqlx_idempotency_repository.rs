use sqlx::PgPool;
use thiserror::Error;
use time::OffsetDateTime;

use crate::application::ports::{IdempotencyError, IdempotencyRepository, IdempotencyTxRepository};
use crate::infrastructure::database::error::classify;

pub struct SqlxIdempotencyRepository {
    pool: PgPool,
}

#[derive(Debug, Error)]
pub enum SqlxIdempotencyError {
    #[error("idempotency error: {0}")]
    IdempotencyError(#[from] IdempotencyError),
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

impl From<SqlxIdempotencyError> for IdempotencyError {
    fn from(e: SqlxIdempotencyError) -> Self {
        match e {
            SqlxIdempotencyError::IdempotencyError(e) => e,
            SqlxIdempotencyError::DatabaseError(_) => IdempotencyError::IdempotencyFailed,
        }
    }
}

impl SqlxIdempotencyRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn cleanup_expired_batched(&self, batch_size: i64) -> Result<u64, IdempotencyError> {
        let mut total_deleted = 0u64;

        loop {
            let result = sqlx::query(
                "DELETE FROM idempotency_keys WHERE key IN (
                    SELECT key FROM idempotency_keys
                    WHERE expires_at < NOW()
                    LIMIT $1
                )",
            )
            .bind(batch_size)
            .execute(&self.pool)
            .await;

            match result {
                Ok(deleted) => {
                    let rows_affected = deleted.rows_affected();
                    if rows_affected == 0 {
                        break;
                    }
                    total_deleted += rows_affected;
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to cleanup expired idempotency keys");
                    return Err(IdempotencyError::IdempotencyFailed);
                }
            }
        }

        Ok(total_deleted)
    }
}

const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 255;

fn truncate_key(key: &str) -> &str {
    if key.len() > MAX_IDEMPOTENCY_KEY_LENGTH {
        &key[..MAX_IDEMPOTENCY_KEY_LENGTH]
    } else {
        key
    }
}

#[async_trait::async_trait]
impl IdempotencyRepository for SqlxIdempotencyRepository {
    async fn get(&self, key: &str) -> Result<Option<String>, IdempotencyError> {
        let key = truncate_key(key);
        let result: Result<Option<(String,)>, sqlx::Error> = sqlx::query_as(
            "SELECT response_body FROM idempotency_keys WHERE key = $1 AND expires_at > NOW()",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await;

        match result {
            Ok(Some((response,))) => Ok(Some(response)),
            Ok(None) => Ok(None),
            Err(e) => {
                let context = classify(&e, "get");
                tracing::error!(err = %context, key = %key, "failed to get idempotency response");
                Err(IdempotencyError::IdempotencyFailed)
            }
        }
    }

    async fn save(&self, key: &str, response: &str) -> Result<(), IdempotencyError> {
        let key = truncate_key(key);
        let now = OffsetDateTime::now_utc();
        let expires = now + time::Duration::hours(24);

        let result = sqlx::query(
            "INSERT INTO idempotency_keys (key, response_body, response_status_code, created_at, expires_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (key) DO UPDATE SET response_body = $2, response_status_code = $3"
        )
        .bind(key)
        .bind(response)
        .bind(200i32)
        .bind(now)
        .bind(expires)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                let context = classify(&e, "save");
                tracing::error!(err = %context, key = %key, "failed to save idempotency response");
                Err(IdempotencyError::IdempotencyFailed)
            }
        }
    }
}

#[async_trait::async_trait]
impl IdempotencyTxRepository<sqlx::Transaction<'static, sqlx::Postgres>>
    for SqlxIdempotencyRepository
{
    async fn save_in_tx(
        &self,
        key: &str,
        response: &str,
        tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    ) -> Result<(), IdempotencyError> {
        let key = truncate_key(key);
        let now = OffsetDateTime::now_utc();
        let expires = now + time::Duration::hours(24);

        let insert_result = sqlx::query(
            "INSERT INTO idempotency_keys (key, response_body, response_status_code, created_at, expires_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (key) DO NOTHING"
        )
        .bind(key)
        .bind(response)
        .bind(200i32)
        .bind(now)
        .bind(expires)
        .execute(&mut **tx)
        .await;

        match insert_result {
            Ok(inserted) => {
                if inserted.rows_affected() > 0 {
                    return Ok(());
                }
                let stored: Result<(String,), sqlx::Error> =
                    sqlx::query_as("SELECT response_body FROM idempotency_keys WHERE key = $1")
                        .bind(key)
                        .fetch_one(&mut **tx)
                        .await;

                match stored {
                    Ok((cached_response,)) => {
                        return Err(IdempotencyError::KeyAlreadyExists {
                            response: cached_response,
                        });
                    }
                    Err(e) => {
                        tracing::error!(err = %e, key = %key, "failed to fetch stored idempotency response");
                        return Err(IdempotencyError::IdempotencyFailed);
                    }
                }
            }
            Err(e) => {
                let context = classify(&e, "save_in_tx");
                tracing::error!(err = %context, key = %key, "failed to save idempotency response in transaction");
                Err(IdempotencyError::IdempotencyFailed)
            }
        }
    }
}
