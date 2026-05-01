use sqlx::PgPool;
use time::OffsetDateTime;

use crate::database::{error::from_sqlx, transaction::DbTransaction};
use application::ports::{IdempotencyError, IdempotencyTxRepository};

pub struct SqlxIdempotencyRepository {
    pool: PgPool,
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
                    let err = from_sqlx(&e).with_operation("cleanup expired batched");
                    tracing::error!(error = %err, "failed to cleanup expired idempotency keys");
                    return Err(IdempotencyError::from(err));
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
impl IdempotencyTxRepository<DbTransaction> for SqlxIdempotencyRepository {
    async fn save_in_tx(
        &self,
        key: &str,
        response: &str,
        tx: &mut DbTransaction,
    ) -> Result<(), IdempotencyError> {
        let key = truncate_key(key);
        let now = OffsetDateTime::now_utc();
        let expires = now + time::Duration::hours(24);

        let insert_result = sqlx::query(
            "INSERT INTO idempotency_keys 
             (key, response_body, response_status_code, created_at, expires_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(key)
        .bind(response)
        .bind(200i32)
        .bind(now)
        .bind(expires)
        .execute(tx.as_sqlx())
        .await;

        match insert_result {
            Ok(inserted) => {
                if inserted.rows_affected() > 0 {
                    return Ok(());
                }
                let stored: Result<(String,), sqlx::Error> =
                    sqlx::query_as("SELECT response_body FROM idempotency_keys WHERE key = $1")
                        .bind(key)
                        .fetch_one(tx.as_sqlx())
                        .await;

                match stored {
                    Ok((cached_response,)) => Err(IdempotencyError::KeyAlreadyExists {
                        response: cached_response,
                    }),
                    Err(e) => {
                        let err = from_sqlx(&e).with_operation("fetch idempotency");
                        tracing::error!(err = %err, key = %key, "failed to fetch stored idempotency response");
                        Err(IdempotencyError::from(err))
                    }
                }
            }
            Err(e) => {
                let err = from_sqlx(&e).with_operation("save in tx");
                tracing::error!(err = %err, key = %key, "failed to save idempotency response in transaction");
                Err(IdempotencyError::from(err))
            }
        }
    }
}
