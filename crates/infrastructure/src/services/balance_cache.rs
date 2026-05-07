use std::sync::Arc;
use std::time::Duration;

use crate::observability::metrics::Metrics;
use application::ports::{BalanceCacheError, BalanceCachePort};

use redis::{AsyncCommands, aio::ConnectionManager};
use uuid::Uuid;

#[derive(Clone)]
pub struct BalanceCache {
    redis: ConnectionManager,
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
        let redis = ConnectionManager::new(client).await?;

        Ok(Self {
            redis,
            default_ttl,
            metrics,
        })
    }

    fn key(&self, account_id: &Uuid) -> String {
        format!("b:{account_id}")
    }

    async fn try_get(&self, account_id: &Uuid) -> Result<Option<u64>, BalanceCacheError> {
        let key = self.key(account_id);
        let mut conn = self.redis.clone();

        let result: Result<Option<u64>, redis::RedisError> = conn.get(key).await;
        match result {
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
        let mut conn = self.redis.clone();

        conn.set_ex::<_, _, ()>(key, balance, ttl_secs)
            .await
            .map_err(|e: redis::RedisError| BalanceCacheError::OperationFailed(e.to_string()))
    }

    async fn try_invalidate(&self, account_id: &Uuid) -> Result<(), BalanceCacheError> {
        let key = self.key(account_id);
        let mut conn = self.redis.clone();

        conn.del::<_, ()>(key)
            .await
            .map_err(|e: redis::RedisError| BalanceCacheError::OperationFailed(e.to_string()))
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
