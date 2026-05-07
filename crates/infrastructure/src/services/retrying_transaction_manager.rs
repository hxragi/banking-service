use std::{sync::Arc, time::Duration};

use application::transaction_manager::{
    RetryConfig, TransactionError, TransactionManagerPort, TransactionOperation, TransactionOutput,
};

use rand::random;
use tokio::time::sleep;

pub struct RetryingTransactionManager<T> {
    inner: Arc<T>,
    retry_config: RetryConfig,
}

impl<T> RetryingTransactionManager<T> {
    pub fn new(inner: Arc<T>, retry_config: RetryConfig) -> Self {
        Self {
            inner,
            retry_config,
        }
    }
}

#[async_trait::async_trait]
impl<T> TransactionManagerPort for RetryingTransactionManager<T>
where
    T: TransactionManagerPort,
{
    async fn execute(
        &self,
        operation: TransactionOperation,
        idempotency_key: Option<String>,
    ) -> Result<TransactionOutput, TransactionError> {
        let mut attempt = 1u32;
        let mut delay_ms = self.retry_config.base_delay_ms;

        loop {
            let result = self
                .inner
                .execute(operation.clone(), idempotency_key.clone())
                .await;

            match result {
                Ok(output) => return Ok(output),
                Err(ref e) if e.is_transient() && attempt < self.retry_config.max_attempts => {
                    tracing::warn!(error = %e, attempt = attempt, max_attempts = self.retry_config.max_attempts, delay_ms = delay_ms, "transient error detected, retrying transaction");
                    sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(self.retry_config.max_delay_ms);
                    delay_ms += random::<u64>() % 10;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
