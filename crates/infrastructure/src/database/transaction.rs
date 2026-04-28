use std::ops::DerefMut;

use sqlx::{PgConnection, Postgres, Transaction as SqlxTransaction};

use application::ports::{Transaction as TransactionTrait, TransactionError};

#[derive(Debug)]
pub struct DbTransaction {
    inner: Option<SqlxTransaction<'static, Postgres>>,
}

impl DbTransaction {
    pub(crate) fn new(inner: SqlxTransaction<'static, Postgres>) -> Self {
        Self { inner: Some(inner) }
    }

    pub(crate) fn as_sqlx(&mut self) -> &mut PgConnection {
        self.inner
            .as_mut()
            .expect("transaction already consumer")
            .deref_mut()
    }
}

impl TransactionTrait for DbTransaction {
    async fn commit(mut self) -> Result<(), TransactionError> {
        if let Some(tx) = self.inner.take() {
            tx.commit()
                .await
                .map_err(|e| TransactionError::CommitFailed(e.to_string()))?;
        }
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), TransactionError> {
        if let Some(tx) = self.inner.take() {
            tx.rollback()
                .await
                .map_err(|e| TransactionError::RollbackFailed(e.to_string()))?;
        }
        Ok(())
    }
}

impl Drop for DbTransaction {
    fn drop(&mut self) {
        if self.inner.is_some() {
            tracing::warn!("DbTransaction dropper without explicit commit/rollback")
        }
    }
}
