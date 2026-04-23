use sqlx::PgPool;
use std::sync::Arc;

use crate::application::ports::{TransactionError, TransactionPort};
use crate::infrastructure::database::transaction::DbTransaction;

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
    type Transaction = DbTransaction;

    async fn begin(&self) -> Result<Self::Transaction, TransactionError> {
        self.pool
            .begin()
            .await
            .map_err(|e| TransactionError::CommitFailed(e.to_string()))
            .map(DbTransaction::new)
    }
}
