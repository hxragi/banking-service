use std::sync::Arc;
use std::time::Duration;

use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};

use crate::application::ports::{IdempotencyError, IdempotencyRepository};
use crate::infrastructure::observability::metrics::Metrics;

pub struct IdempotencyService {
    repository: Arc<dyn IdempotencyRepository + Send + Sync>,
    redis: MultiplexedConnection,
    default_ttl: Duration,
    metrics: Option<Arc<Metrics>>,
    key_prefix: String,
}

#[derive(Debug, Clone)]
pub struct IdempotencyCheckResult {
    pub cached_response: Option<String>,
}

impl IdempotencyService {
    pub async fn new(
        repository: Arc<dyn IdempotencyRepository + Send + Sync>,
        redis_url: &str,
        default_ttl: Duration,
        metrics: Option<Arc<Metrics>>,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let redis = client.get_multiplexed_tokio_connection().await?;

        Ok(Self {
            repository,
            redis,
            default_ttl,
            metrics,
            key_prefix: "idempotency:".to_string(),
        })
    }

    fn redis_key(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }

    pub async fn check_or_acquire(
        &self,
        key: &str,
    ) -> Result<IdempotencyCheckResult, IdempotencyError> {
        let redis_key = self.redis_key(key);

        match self.redis.clone().get::<_, Option<String>>(redis_key).await {
            Ok(Some(cached)) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.increment_cache_hit("idempotency");
                }
                return Ok(IdempotencyCheckResult {
                    cached_response: Some(cached),
                });
            }
            Ok(None) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.increment_cache_miss("idempotency");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to get idempotency key from cache");
            }
        }

        if let Some(cached_response) = self.repository.get(key).await? {
            let redis_key = self.redis_key(key);
            let ttl_secs = self.default_ttl.as_secs();
            let _: RedisResult<()> = self
                .redis
                .clone()
                .set_ex(redis_key, cached_response.clone(), ttl_secs)
                .await;
            return Ok(IdempotencyCheckResult {
                cached_response: Some(cached_response),
            });
        }

        Ok(IdempotencyCheckResult {
            cached_response: None,
        })
    }

    pub async fn save_response(&self, key: &str, response: &str) -> Result<(), IdempotencyError> {
        if let Err(e) = self.repository.save(key, response).await {
            tracing::warn!(err = ?e, "failed to save idempotency key to repository");
        } else {
            let redis_key = self.redis_key(key);
            let ttl_secs = self.default_ttl.as_secs();
            let _: RedisResult<()> = self
                .redis
                .clone()
                .set_ex(redis_key, response.to_owned(), ttl_secs)
                .await;
        }
        Ok(())
    }

    pub fn generate_key() -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
