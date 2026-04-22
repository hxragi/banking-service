use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use uuid::Uuid;

use crate::application::ports::{BalanceCacheError, BalanceCachePort};
use crate::infrastructure::observability::metrics::Metrics;

#[derive(Clone)]
pub struct BalanceCache {
    redis: Arc<redis::aio::MultiplexedConnection>,
    default_ttl: Duration,
    metrics: Option<Arc<Metrics>>,
}

impl BalanceCache {
    pub async fn new(
        redis_url: &str,
        default_ttl: Duration,
        metrics: Option<Arc<Metrics>>,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let redis = client.get_multiplexed_tokio_connection().await?;

        Ok(Self {
            redis: Arc::new(redis),
            default_ttl,
            metrics,
        })
    }

    fn key(&self, account_id: &Uuid) -> String {
        format!("b:{account_id}")
    }

    async fn try_get(&self, account_id: &Uuid) -> Result<Option<u64>, BalanceCacheError> {
        let key = self.key(account_id);
        let mut conn = (*self.redis).clone();

        match conn.get::<_, Option<u64>>(key).await {
            Ok(Some(balance)) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.increment_cache_hit("balance");
                }
                Ok(Some(balance))
            }
            Ok(None) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.increment_cache_miss("balance");
                }
                Ok(None)
            }
            Err(e) => Err(BalanceCacheError::Unavailable(e.to_string())),
        }
    }

    async fn try_set(&self, account_id: Uuid, balance: u64) -> Result<(), BalanceCacheError> {
        let key = self.key(&account_id);
        let ttl_secs = self.default_ttl.as_secs();
        let mut conn = (*self.redis).clone();

        conn.set_ex(key, balance, ttl_secs)
            .await
            .map_err(|e| BalanceCacheError::OperationFailed(e.to_string()))
    }

    async fn try_invalidate(&self, account_id: &Uuid) -> Result<(), BalanceCacheError> {
        let key = self.key(account_id);
        let mut conn = (*self.redis).clone();

        conn.del::<_, ()>(key)
            .await
            .map_err(|e| BalanceCacheError::OperationFailed(e.to_string()))
    }
}

#[async_trait::async_trait]
impl BalanceCachePort for BalanceCache {
    async fn get(&self, account_id: &Uuid) -> Result<Option<u64>, BalanceCacheError> {
        self.try_get(account_id).await
    }

    async fn set(&self, account_id: Uuid, balance: u64) -> Result<(), BalanceCacheError> {
        self.try_set(account_id, balance).await
    }

    async fn invalidate(&self, account_id: &Uuid) -> Result<(), BalanceCacheError> {
        self.try_invalidate(account_id).await
    }
}

#[cfg(test)]
mod tests {
    use crate::application::ports::BalanceCachePort;
    use crate::test_utils::redis_setup::setup_redis;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_cache_set_and_get() {
        let (cache, _container) = setup_redis().await;

        let account_id = Uuid::new_v4();
        let balance = 1000u64;

        let result = BalanceCachePort::get(&cache, &account_id).await.unwrap();
        assert_eq!(result, None);

        BalanceCachePort::set(&cache, account_id, balance)
            .await
            .unwrap();

        let result = BalanceCachePort::get(&cache, &account_id).await.unwrap();
        assert_eq!(result, Some(balance));
    }

    #[tokio::test]
    async fn test_cache_invalidate() {
        let (cache, _container) = setup_redis().await;

        let account_id = Uuid::new_v4();
        let balance = 1000u64;

        BalanceCachePort::set(&cache, account_id, balance)
            .await
            .unwrap();
        assert_eq!(
            BalanceCachePort::get(&cache, &account_id).await.unwrap(),
            Some(balance)
        );

        BalanceCachePort::invalidate(&cache, &account_id)
            .await
            .unwrap();
        assert_eq!(
            BalanceCachePort::get(&cache, &account_id).await.unwrap(),
            None
        );
    }
}
