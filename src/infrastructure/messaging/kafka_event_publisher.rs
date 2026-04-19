use std::collections::HashMap;
use std::time::Duration;

use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use thiserror::Error;
use tokio::time::sleep;

use crate::infrastructure::config::config::KafkaConfig;
use crate::infrastructure::messaging::kafka_tracing::create_traceparent_for_kafka;

#[derive(Debug, Clone)]
pub struct DomainEvent {
    pub event_id: String,
    pub event_type: String,
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub payload: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EventPublishError {
    #[error("publish failed: {0}")]
    PublishFailed(String),
    #[error("connection error: {0}")]
    ConnectionError(String),
}

pub struct KafkaEventPublisher {
    producer: FutureProducer,
    timeout: Duration,
    max_retries: u32,
    base_delay: Duration,
    max_delay: Duration,
}

fn calculate_backoff_with_jitter(
    attempt: u32,
    base_delay: Duration,
    max_delay: Duration,
) -> Duration {
    let base_ms = base_delay.as_millis() as u64;
    let max_ms = max_delay.as_millis() as u64;

    let delay = (base_ms * 2_u64.pow(attempt.min(10))).min(max_ms);
    let jitter = rand::random::<u64>() % (delay / 4 + 1);

    Duration::from_millis(delay + jitter)
}

impl KafkaEventPublisher {
    pub fn new(config: &KafkaConfig) -> Result<Self, EventPublishError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("compression.type", &config.compression_type)
            .set("message.timeout.ms", config.producer_timeout_ms.to_string())
            .set("retries", config.max_retries.to_string())
            .set("retry.backoff.ms", config.retry_backoff_ms.to_string())
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .create()
            .map_err(|e| EventPublishError::ConnectionError(e.to_string()))?;

        Ok(Self {
            producer,
            timeout: Duration::from_millis(config.producer_timeout_ms),
            max_retries: config.app_max_retries,
            base_delay: Duration::from_millis(config.app_retry_base_delay_ms),
            max_delay: Duration::from_millis(config.app_retry_max_delay_ms),
        })
    }

    fn create_key(event: &DomainEvent) -> String {
        format!(
            "{}:{}: {}",
            event.aggregate_type, event.aggregate_id, event.event_id
        )
    }

    fn build_headers(event: &DomainEvent) -> rdkafka::message::OwnedHeaders {
        let mut headers = rdkafka::message::OwnedHeaders::new();
        headers = headers.insert(rdkafka::message::Header {
            key: "event_type",
            value: Some(&event.event_type),
        });
        headers = headers.insert(rdkafka::message::Header {
            key: "aggregate_type",
            value: Some(&event.aggregate_type),
        });
        headers = headers.insert(rdkafka::message::Header {
            key: "aggregate_id",
            value: Some(&event.aggregate_id),
        });
        headers = headers.insert(rdkafka::message::Header {
            key: "event_id",
            value: Some(&event.event_id),
        });

        let current_span = tracing::Span::current();
        if let Some(traceparent) = create_traceparent_for_kafka(&current_span) {
            headers = headers.insert(rdkafka::message::Header {
                key: "traceparent",
                value: Some(&traceparent),
            });
            tracing::debug!(traceparent = %traceparent, "Added traceparent to Kafka message headers");
        }

        for (key, value) in &event.metadata {
            headers = headers.insert(rdkafka::message::Header {
                key,
                value: Some(value),
            });
        }

        headers
    }
}

impl KafkaEventPublisher {
    async fn publish_with_retry<'a>(
        &self,
        topic: &'a str,
        key: &'a str,
        payload: &'a str,
        headers: &rdkafka::message::OwnedHeaders,
        event_id: &'a str,
        event_type: &'a str,
    ) -> Result<(i32, i64), EventPublishError> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            let record = FutureRecord::to(topic)
                .key(key)
                .payload(payload)
                .headers(headers.clone());

            match self.producer.send(record, self.timeout).await {
                Ok(delivery) => {
                    if attempt > 0 {
                        tracing::info!(
                            topic,
                            event_id,
                            event_type,
                            attempt,
                            partition = delivery.0,
                            offset = delivery.1,
                            "event published to Kafka after retry"
                        );
                    }
                    return Ok(delivery);
                }
                Err((err, _)) => {
                    let error_msg = err.to_string();

                    if Self::is_non_retryable_error(&error_msg) {
                        tracing::warn!(
                            topic,
                            event_id,
                            error = %error_msg,
                            "non-retryable Kafka error"
                        );
                        return Err(EventPublishError::PublishFailed(error_msg));
                    }

                    if attempt < self.max_retries {
                        let backoff =
                            calculate_backoff_with_jitter(attempt, self.base_delay, self.max_delay);
                        tracing::warn!(
                            topic,
                            event_id,
                            event_type,
                            attempt,
                            max_retries = self.max_retries,
                            backoff_ms = backoff.as_millis(),
                            error = %error_msg,
                            "Kafka publish failed, retrying with exponential backoff"
                        );
                        sleep(backoff).await;
                    }

                    last_error = Some(error_msg);
                }
            }
        }

        let final_error = last_error.unwrap_or_else(|| "unknown error".to_string());
        tracing::error!(
            topic,
            event_id,
            event_type,
            max_retries = self.max_retries,
            error = %final_error,
            "failed to publish event to Kafka after all retries"
        );
        Err(EventPublishError::PublishFailed(final_error))
    }

    fn is_non_retryable_error(error: &str) -> bool {
        let non_retryable_patterns = [
            "serialization",
            "deserialization",
            "invalid configuration",
            "unknown topic",
            "invalid topic",
        ];

        non_retryable_patterns
            .iter()
            .any(|pattern| error.to_lowercase().contains(pattern))
    }
}

impl KafkaEventPublisher {
    pub async fn publish(&self, topic: &str, event: &DomainEvent) -> Result<(), EventPublishError> {
        let key = Self::create_key(event);
        let headers = Self::build_headers(event);

        let delivery = self
            .publish_with_retry(
                topic,
                &key,
                &event.payload,
                &headers,
                &event.event_id,
                &event.event_type,
            )
            .await?;

        tracing::info!(
            topic,
            partition = delivery.0,
            offset = delivery.1,
            event_id = %event.event_id,
            event_type = %event.event_type,
            "event published to Kafka"
        );

        Ok(())
    }
}

use crate::application::ports::{EventPublisher as EventPublisherPort, EventPublishError as PortEventPublishError};
use crate::domain::transaction_event::TransactionEvent;
use crate::infrastructure::dto::transaction_event_dto::TransactionEventDto;

#[async_trait::async_trait]
impl EventPublisherPort for KafkaEventPublisher {
    async fn publish(
        &self,
        topic: &str,
        event: &TransactionEvent,
    ) -> Result<(), PortEventPublishError> {
        let domain_event_payload = TransactionEventDto::from(event.clone());
        let payload_json = domain_event_payload.to_json().map_err(|e| {
            PortEventPublishError::SerializationError(e.to_string())
        })?;

        let domain_event = DomainEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: "TransactionCreated".to_string(),
            aggregate_id: event.transaction_id.to_string(),
            aggregate_type: "transaction".to_string(),
            payload: payload_json,
            metadata: std::collections::HashMap::new(),
        };

        self.publish(topic, &domain_event).await.map_err(|e| match e {
            EventPublishError::PublishFailed(msg) => PortEventPublishError::PublishFailed(msg),
            EventPublishError::ConnectionError(msg) => PortEventPublishError::ConnectionError(msg),
        })
    }
}

impl Drop for KafkaEventPublisher {
    fn drop(&mut self) {
        tracing::info!("flushing Kafka producer on drop");
        let _ = self.producer.flush(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_event() -> DomainEvent {
        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), "test".to_string());

        DomainEvent {
            event_id: "test-123".to_string(),
            event_type: "TestEvent".to_string(),
            aggregate_id: "agg-456".to_string(),
            aggregate_type: "TestAggregate".to_string(),
            payload: r#"{"key":"value"}"#.to_string(),
            metadata,
        }
    }

    #[test]
    fn test_create_key() {
        let event = create_test_event();
        let key = KafkaEventPublisher::create_key(&event);
        assert!(key.contains("TestAggregate"));
        assert!(key.contains("agg-456"));
        assert!(key.contains("test-123"));
    }

    #[test]
    fn test_build_headers() {
        let event = create_test_event();
        let _headers = KafkaEventPublisher::build_headers(&event);
    }

    #[test]
    fn test_calculate_backoff_exponential_growth() {
        let base_delay = Duration::from_millis(100);
        let max_delay = Duration::from_secs(30);

        let delay0 = calculate_backoff_with_jitter(0, base_delay, max_delay);
        assert!(delay0 >= Duration::from_millis(100));
        assert!(delay0 <= Duration::from_millis(125));

        let delay1 = calculate_backoff_with_jitter(1, base_delay, max_delay);
        assert!(delay1 >= Duration::from_millis(200));
        assert!(delay1 <= Duration::from_millis(250));

        let delay2 = calculate_backoff_with_jitter(2, base_delay, max_delay);
        assert!(delay2 >= Duration::from_millis(400));
        assert!(delay2 <= Duration::from_millis(500));

        let delay5 = calculate_backoff_with_jitter(5, base_delay, max_delay);
        assert!(delay5 >= Duration::from_millis(3200));
        assert!(delay5 <= Duration::from_millis(4000));
    }

    #[test]
    fn test_calculate_backoff_max_delay_cap() {
        let base_delay = Duration::from_millis(1000);
        let max_delay = Duration::from_millis(5000);

        let delay_high = calculate_backoff_with_jitter(10, base_delay, max_delay);
        assert!(delay_high >= max_delay);
        assert!(delay_high <= Duration::from_millis(6250));
    }

    #[test]
    fn test_calculate_backoff_jitter_variance() {
        let base_delay = Duration::from_millis(100);
        let max_delay = Duration::from_secs(30);

        let mut delays = Vec::new();
        for _ in 0..10 {
            delays.push(calculate_backoff_with_jitter(0, base_delay, max_delay));
        }

        let unique_delays: std::collections::HashSet<_> = delays.iter().collect();
        assert!(
            unique_delays.len() > 1,
            "jitter should create variance in delays"
        );
    }

    #[test]
    fn test_is_non_retryable_error_serialization() {
        assert!(KafkaEventPublisher::is_non_retryable_error(
            "serialization failed"
        ));
        assert!(KafkaEventPublisher::is_non_retryable_error(
            "deserialization error"
        ));
    }

    #[test]
    fn test_is_non_retryable_error_config() {
        assert!(KafkaEventPublisher::is_non_retryable_error(
            "invalid configuration"
        ));
    }

    #[test]
    fn test_is_non_retryable_error_topic() {
        assert!(KafkaEventPublisher::is_non_retryable_error("unknown topic"));
        assert!(KafkaEventPublisher::is_non_retryable_error(
            "invalid topic name"
        ));
    }

    #[test]
    fn test_is_retryable_network_error() {
        assert!(!KafkaEventPublisher::is_non_retryable_error(
            "connection timeout"
        ));
        assert!(!KafkaEventPublisher::is_non_retryable_error(
            "broker not available"
        ));
        assert!(!KafkaEventPublisher::is_non_retryable_error(
            "network unreachable"
        ));
    }
}
