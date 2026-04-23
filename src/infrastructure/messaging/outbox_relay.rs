use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, Row};
use tokio::time::interval;
use uuid::Uuid;

use crate::application::ports::EventPublisher;
use crate::domain::transaction_event::TransactionEvent;
use crate::infrastructure::dto::transaction_event_dto::TransactionEventDto;

pub struct OutboxRelay {
    pool: PgPool,
    publisher: Arc<dyn EventPublisher + Send + Sync>,
    poll_interval: Duration,
    batch_size: i64,
}

impl OutboxRelay {
    pub fn new(
        pool: PgPool,
        publisher: Arc<dyn EventPublisher + Send + Sync>,
        poll_interval: Duration,
        batch_size: i64,
    ) -> Self {
        Self {
            pool,
            publisher,
            poll_interval,
            batch_size,
        }
    }

    pub async fn run(self) {
        let mut ticker = interval(self.poll_interval);
        loop {
            ticker.tick().await;
            if let Err(e) = self.process_batch().await {
                tracing::error!(error = %e, "outbox relay batch failed");
            }
        }
    }

    async fn process_batch(&self) -> anyhow::Result<()> {
        let rows = sqlx::query(
            r#"
            SELECT id, topic, payload
            FROM outbox
            WHERE processed_at IS NULL
            ORDER BY created_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(self.batch_size)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let topic: String = row.try_get("topic")?;
            let payload: Vec<u8> = row.try_get("payload")?;

            let dto: TransactionEventDto = match serde_json::from_slice(&payload) {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(outbox_id = %id, error = %e, "outbox poison pill: failed to deserialize payload, marking processed to unblock queue");
                    sqlx::query("UPDATE outbox SET processed_at = NOW() WHERE id = $1")
                        .bind(id)
                        .execute(&self.pool)
                        .await?;
                    continue;
                }
            };

            let event = TransactionEvent::from(dto);

            match self.publisher.publish(&topic, &event).await {
                Ok(()) => {
                    sqlx::query("UPDATE outbox SET processed_at = NOW() WHERE id = $1")
                        .bind(id)
                        .execute(&self.pool)
                        .await?;
                    tracing::debug!(outbox_id = %id, "outbox event published");
                }
                Err(e) => {
                    tracing::warn!(outbox_id = %id, error = %e, "outbox publish failed, will retry on next poll");
                    sqlx::query("UPDATE outbox SET retry_count = retry_count + 1 WHERE id = $1")
                        .bind(id)
                        .execute(&self.pool)
                        .await?;
                }
            }
        }

        Ok(())
    }
}
