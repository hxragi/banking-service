use std::collections::HashMap;
use std::time::Duration;

use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use thiserror::Error;

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
    pub async fn publish(&self, topic: &str, event: &DomainEvent) -> Result<(), EventPublishError> {
        let key = Self::create_key(event);
        let headers = Self::build_headers(event);

        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(&event.payload)
            .headers(headers);

        let delivery = self
            .producer
            .send(record, self.timeout)
            .await
            .map_err(|(err, _)| EventPublishError::PublishFailed(err.to_string()))?;

        tracing::info!(
            topic,
            partition = delivery.partition,
            offset = delivery.offset,
            event_id = %event.event_id,
            event_type = %event.event_type,
            "event published to Kafka"
        );

        Ok(())
    }
}

use crate::application::ports::{
    EventPublishError as PortEventPublishError, EventPublisher as EventPublisherPort,
};
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
        let payload_json = domain_event_payload
            .to_json()
            .map_err(|e| PortEventPublishError::SerializationError(e.to_string()))?;

        let domain_event = DomainEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: "TransactionCreated".to_string(),
            aggregate_id: event.transaction_id.to_string(),
            aggregate_type: "transaction".to_string(),
            payload: payload_json,
            metadata: std::collections::HashMap::new(),
        };

        self.publish(topic, &domain_event)
            .await
            .map_err(|e| match e {
                EventPublishError::PublishFailed(msg) => PortEventPublishError::PublishFailed(msg),
                EventPublishError::ConnectionError(msg) => {
                    PortEventPublishError::ConnectionError(msg)
                }
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
}
