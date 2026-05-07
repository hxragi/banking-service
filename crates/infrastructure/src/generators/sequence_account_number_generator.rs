use application::ports::{AccountNumberGenerator, AccountNumberGeneratorError};
use domain::account_number::AccountNumber;

use async_trait::async_trait;
use sqlx::PgPool;
use thiserror::Error;

pub struct SequenceAccountNumberGenerator {
    pool: PgPool,
    prefix: String,
}

#[derive(Debug, Error)]
pub enum SequenceGeneratorError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    #[error("invalid account number generated")]
    InvalidAccountNumber,
}

impl SequenceAccountNumberGenerator {
    pub fn new(pool: PgPool, prefix: String) -> Self {
        Self { pool, prefix }
    }

    pub async fn generate_from_sequence(&self) -> Result<AccountNumber, SequenceGeneratorError> {
        let next_val: i64 = sqlx::query_scalar("SELECT nextval('account_number_seq')")
            .fetch_one(&self.pool)
            .await?;

        let number = format!("{}-{:012}", self.prefix, next_val);
        AccountNumber::new(&number).map_err(|_| SequenceGeneratorError::InvalidAccountNumber)
    }
}

#[async_trait]
impl AccountNumberGenerator for SequenceAccountNumberGenerator {
    async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError> {
        self.generate_from_sequence().await.map_err(|e| match e {
            SequenceGeneratorError::DatabaseError(_) => {
                AccountNumberGeneratorError::GenerationFailed
            }
            SequenceGeneratorError::InvalidAccountNumber => {
                AccountNumberGeneratorError::GenerationFailed
            }
        })
    }
}
