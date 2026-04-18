use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;

use crate::application::ports::{TransactionError, TransactionPort};

#[derive(Clone)]
pub struct Manager {
    pool: Arc<PgPool>,
}

impl Manager {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool: Arc::new(pool),
        }
    }
}

impl TransactionPort for Manager {
    type Transaction = Transaction<'static, Postgres>;

    async fn begin(&self) -> Result<Self::Transaction, TransactionError> {
        self.pool
            .begin()
            .await
            .map_err(|e| TransactionError::CommitFailed(e.to_string()))
    }
}

impl crate::application::ports::Transaction for Transaction<'static, Postgres> {
    async fn commit(self) -> Result<(), TransactionError> {
        sqlx::Transaction::commit(self)
            .await
            .map_err(|e| TransactionError::CommitFailed(e.to_string()))
    }

    async fn rollback(self) -> Result<(), TransactionError> {
        sqlx::Transaction::rollback(self)
            .await
            .map_err(|e| TransactionError::RollbackFailed(e.to_string()))
    }
}
