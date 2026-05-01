use async_trait::async_trait;
use uuid::Uuid;

use crate::database::error::from_sqlx;
use crate::database::transaction::DbTransaction;
use crate::dto::transaction_event_dto::TransactionEventDto;
use application::ports::TransactionRepositoryError;
use application::transaction_manager::OutboxRepository;
use domain::transaction_event::TransactionEvent;

#[derive(Clone, Default)]
pub struct SqlxOutboxRepository;

impl SqlxOutboxRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl OutboxRepository<DbTransaction> for SqlxOutboxRepository {
    async fn save_in_tx(
        &self,
        event: &TransactionEvent,
        tx: &mut DbTransaction,
    ) -> Result<(), TransactionRepositoryError> {
        let dto = TransactionEventDto::from(event.clone());
        let payload = serde_json::to_vec(&dto).map_err(|e| {
            TransactionRepositoryError::ConnectionError(format!("outbox serialization: {e}"))
        })?;

        sqlx::query(
            r#"
            INSERT INTO outbox (id, topic, payload, created_at)
            VALUES ($1, $2, $3, NOW())
            "#,
        )
        .bind(Uuid::new_v4())
        .bind("bank.transaction.created")
        .bind(payload)
        .execute(tx.as_sqlx())
        .await
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("save_outbox");
            tracing::error!(err = %err, "failed to save outbox event");
            TransactionRepositoryError::from(err)
        })?;

        Ok(())
    }
}
