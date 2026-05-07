use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::messaging::{
    kafka_external_events::{DonateTopupEvent, GovFineCreatedEvent, MarketOrderPaidEvent},
    kafka_tracing::extract_trace_context,
};
use application::external_event_processor::{
    ExternalEventError, ExternalEventProcessor, ProcessDonateTopupInput, ProcessGovFineInput,
    ProcessMarketOrderInput,
};

use async_trait::async_trait;
use base64::Engine;
use opentelemetry::trace::TraceContextExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::BorrowedMessage,
    producer::{FutureProducer, Producer},
};
use redis::aio::MultiplexedConnection;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const MAX_RETRIES: u32 = 3;
const DLQ_TOPIC: &str = "bank.transaction.dlq";
const RETRY_KEY_PREFIX: &str = "kafka:retry:";
const RETRY_KEY_TTL_SECS: u64 = 86400;

#[derive(Debug, Error)]
pub enum RetryTrackerError {
    #[error("redis unavailable: {0}")]
    RedisUnavailable(String),
}

impl RetryTrackerError {
    pub fn is_connection_error(&self) -> bool {
        matches!(self, RetryTrackerError::RedisUnavailable(_))
    }
}

#[derive(Clone)]
pub struct RetryTracker {
    redis: MultiplexedConnection,
    memory: Arc<Mutex<HashMap<String, u32>>>,
}

impl RetryTracker {
    pub async fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let redis = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            redis,
            memory: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn health_check(&self) -> Result<(), RetryTrackerError> {
        let mut conn = self.redis.clone();
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map(|_| ())
            .map_err(|e| RetryTrackerError::RedisUnavailable(e.to_string()))
    }

    fn retry_key(&self, topic: &str, partition: i32, offset: i64) -> String {
        format!("{}{}:{}:{}", RETRY_KEY_PREFIX, topic, partition, offset)
    }

    pub async fn get_retry_count(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<u32, RetryTrackerError> {
        let key = self.retry_key(topic, partition, offset);
        let mut conn = self.redis.clone();

        match redis::AsyncCommands::get::<_, Option<u32>>(&mut conn, &key).await {
            Ok(opt) => Ok(opt.unwrap_or(0)),
            Err(e) => {
                let err = RetryTrackerError::RedisUnavailable(e.to_string());
                if err.is_connection_error() {
                    let memory = self.memory.lock().await;
                    Ok(*memory.get(&key).unwrap_or(&0))
                } else {
                    tracing::error!(error = %e, topic, partition, offset, "redis error while getting retry count");
                    Err(err)
                }
            }
        }
    }

    pub async fn increment_retry(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), RetryTrackerError> {
        let key = self.retry_key(topic, partition, offset);
        let mut conn = self.redis.clone();

        async move {
            match redis::cmd("INCR")
                .arg(&key)
                .query_async::<u32>(&mut conn)
                .await
            {
                Ok(new_count) => {
                    if new_count == 1 {
                        redis::cmd("EXPIRE")
                            .arg(&key)
                            .arg(RETRY_KEY_TTL_SECS)
                            .query_async::<()>(&mut conn)
                            .await
                            .map_err(|e| {
                                tracing::warn!(error = %e, key = %key, "failed to set ttl on retry key - key may accumulate");
                            })
                            .ok();
                    }
                    Ok(())
                }
                Err(e) => {
                    let err = RetryTrackerError::RedisUnavailable(e.to_string());
                    if err.is_connection_error() {
                        let mut memory = self.memory.lock().await;
                        *memory.entry(key).or_insert(0) += 1;
                        Ok(())
                    } else {
                        Err(err)
                    }
                }
            }
        }
        .await
    }

    pub async fn clear_retry(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), RetryTrackerError> {
        let key = self.retry_key(topic, partition, offset);
        let mut conn = self.redis.clone();
        match redis::AsyncCommands::del(&mut conn, &key).await {
            Ok(()) => {
                let mut memory = self.memory.lock().await;
                memory.remove(&key);
                Ok(())
            }
            Err(e) => {
                let err = RetryTrackerError::RedisUnavailable(e.to_string());
                if err.is_connection_error() {
                    let mut memory = self.memory.lock().await;
                    memory.remove(&key);
                    Ok(())
                } else {
                    tracing::error!(error = %e, topic, offset, "redis error while clearing retry count");
                    Err(err)
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct DlqProducer {
    producer: FutureProducer,
}

impl DlqProducer {
    pub fn new(bootstrap_servers: &str) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| anyhow::anyhow!("failed to create DLQ producer: {}", e))?;

        Ok(Self { producer })
    }

    pub async fn send_to_dlq(
        &self,
        original_topic: &str,
        partition: i32,
        offset: i64,
        payload: &[u8],
        retry_count: u32,
        error_reason: &str,
    ) {
        let key = format!("{}:{}:{}", original_topic, partition, offset);

        let now = time::OffsetDateTime::now_utc();
        let timestamp = now.to_string();

        let dlq_message = serde_json::json!({
            "original_topic": original_topic,
            "partition": partition,
            "offset": offset,
            "retry_count": retry_count,
            "error_reason": error_reason,
            "payload": match std::str::from_utf8(payload) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
                    serde_json::json!({"base64": encoded})
                }
            },
            "timestamp": timestamp,
        });

        let payload_str = dlq_message.to_string();

        match self
            .producer
            .send(
                rdkafka::producer::FutureRecord::to(DLQ_TOPIC)
                    .key(&key)
                    .payload(&payload_str),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(_) => {
                tracing::error!(
                    original_topic = %original_topic,
                    partition = partition,
                    offset = offset,
                    retry_count = retry_count,
                    dlq_topic = DLQ_TOPIC,
                    "message sent to DLQ after {} failed processing attempts",
                    retry_count
                );
            }
            Err(e) => {
                tracing::error!(
                    error = %e.0,
                    original_topic = %original_topic,
                    partition = partition,
                    offset = offset,
                    "failed to send message to DLQ - message will be redelivered"
                );
            }
        }
    }
}

#[async_trait]
pub trait ExternalEventHandler: Send + Sync {
    async fn handle_fine_created(
        &self,
        event: GovFineCreatedEvent,
    ) -> Result<(), ExternalEventError>;

    async fn handle_order_paid(
        &self,
        event: MarketOrderPaidEvent,
    ) -> Result<(), ExternalEventError>;

    async fn handle_donate_topup(&self, event: DonateTopupEvent) -> Result<(), ExternalEventError>;
}

pub struct ExternalEventHandlerImpl {
    processor: Arc<ExternalEventProcessor>,
}

impl ExternalEventHandlerImpl {
    pub fn new(processor: Arc<ExternalEventProcessor>) -> Self {
        Self { processor }
    }
}

#[async_trait]
impl ExternalEventHandler for ExternalEventHandlerImpl {
    #[tracing::instrument(
        skip(self),
        fields(fine_id = %event.fine_id, user_id = %event.user_id, amount = %event.amount)
    )]
    async fn handle_fine_created(
        &self,
        event: GovFineCreatedEvent,
    ) -> Result<(), ExternalEventError> {
        tracing::info!("processing gov.fine.created event");

        self.processor
            .process_gov_fine(ProcessGovFineInput {
                fine_id: event.fine_id,
                user_id: event.user_id,
                account_number: event.account_number,
                amount: event.amount,
            })
            .await
    }

    #[tracing::instrument(
        skip(self),
        fields(order_id = %event.order_id, buyer_id = %event.buyer_id, amount = %event.total_amount)
    )]
    async fn handle_order_paid(
        &self,
        event: MarketOrderPaidEvent,
    ) -> Result<(), ExternalEventError> {
        tracing::info!("processing market.order.paid event");

        self.processor
            .process_market_order(ProcessMarketOrderInput {
                order_id: event.order_id,
                buyer_id: event.buyer_id,
                buyer_account_number: event.buyer_account_number,
                total_amount: event.total_amount,
                seller_id: event.seller_id,
            })
            .await
    }

    #[tracing::instrument(
        skip(self),
        fields(donation_id = %event.donation_id, recipient_id = %event.recipient_id, amount = %event.amount)
    )]
    async fn handle_donate_topup(&self, event: DonateTopupEvent) -> Result<(), ExternalEventError> {
        tracing::info!("processing donate.topup event");

        self.processor
            .process_donate_topup(ProcessDonateTopupInput {
                donation_id: event.donation_id,
                recipient_id: event.recipient_id,
                recipient_account_number: event.recipient_account_number,
                amount: event.amount,
                is_anonymous: event.is_anonymous,
            })
            .await
    }
}

pub struct KafkaConsumerConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub session_timeout_ms: u64,
    pub auto_offset_reset: String,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "bank-service-consumer".to_string(),
            topics: vec![
                "gov.fine.created".to_string(),
                "market.order.paid".to_string(),
                "donate.topup".to_string(),
            ],
            session_timeout_ms: 10000,
            auto_offset_reset: "earliest".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum ConsumerCommand {
    Shutdown,
}

pub struct KafkaEventConsumer {
    consumer: StreamConsumer,
    handler: Arc<dyn ExternalEventHandler>,
    command_rx: mpsc::Receiver<ConsumerCommand>,
    retry_tracker: Arc<RetryTracker>,
    dlq_producer: Arc<DlqProducer>,
}

impl KafkaEventConsumer {
    pub fn new(
        config: &KafkaConsumerConfig,
        handler: Arc<dyn ExternalEventHandler>,
        command_rx: mpsc::Receiver<ConsumerCommand>,
        retry_tracker: Arc<RetryTracker>,
        dlq_producer: Arc<DlqProducer>,
    ) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("enable.auto.commit", "false")
            .create()
            .map_err(|e| anyhow::anyhow!("failed to create Kafka consumer: {}", e))?;

        let topics: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topics)
            .map_err(|e| anyhow::anyhow!("failed to subscribe to topics: {}", e))?;

        Ok(Self {
            consumer,
            handler,
            command_rx,
            retry_tracker,
            dlq_producer,
        })
    }

    pub async fn run(mut self) {
        tracing::info!(
            "kafka consumer started with DLQ support (max_retries={})",
            MAX_RETRIES
        );

        let consumer = &self.consumer;
        let handler = &self.handler;
        let command_rx = &mut self.command_rx;
        let retry_tracker = &self.retry_tracker;
        let dlq_producer = &self.dlq_producer;

        loop {
            tokio::select! {
                command = command_rx.recv() => {
                    if let Some(ConsumerCommand::Shutdown) = command {
                        tracing::info!("received shutdown command, stopping consumer");
                        break;
                    }
                }

                result = async {
                    match consumer.recv().timeout(Duration::from_secs(1)).await {
                        Ok(Ok(msg)) => {
                            let topic = msg.topic().to_string();
                            let partition = msg.partition();
                            let offset = msg.offset();

                            match Self::process_message_with_handler(handler, &msg).await {
                                Ok(()) => {
                                    match retry_tracker.clear_retry(&topic, partition, offset).await {
                                        Ok(()) => {
                                            if let Err(e) = consumer.commit_message(&msg, CommitMode::Sync) {
                                                tracing::error!(error = %e, topic, partition, offset, "failed to commit offset after successful processing - message may be redelivered on restart");
                                            } else {
                                                tracing::debug!(topic, partition, offset, "offset committed successfully after processing");
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                error = %e,
                                                topic,
                                                partition,
                                                offset,
                                                "redis error after successful processing - offset not committed. message will be redelivered"
                                            );
                                        }
                                    }
                                    Ok(())
                                }
                                Err(error_reason) => {
                                    let retry_count = match retry_tracker.get_retry_count(&topic, partition, offset).await {
                                        Ok(count) => count,
                                        Err(e) => {
                                            tracing::error!(
                                                error = %e,
                                                topic,
                                                partition,
                                                offset,
                                                "redis error during retry tracking - offset not committed"
                                            );
                                            return Ok(());
                                        }
                                    };

                                    if retry_count >= MAX_RETRIES {
                                        tracing::warn!(topic, partition, offset, retry_count, "max retries exceeded, sending to dlq");

                                        if let Some(payload) = msg.payload() {
                                            dlq_producer.send_to_dlq(
                                                &topic,
                                                partition,
                                                offset,
                                                payload,
                                                retry_count + 1,
                                                &error_reason,
                                            ).await;
                                        }

                                        if let Err(e) = consumer.commit_message(&msg, CommitMode::Sync) {
                                            tracing::error!(error = %e, topic, partition, offset, "failed to commit offset after dlq");
                                        } else {
                                            tracing::info!(topic, partition, offset, "offset committed after sending to dlq");
                                        }

                                        if let Err(e) = retry_tracker.clear_retry(&topic, partition, offset).await {
                                            tracing::warn!(error = %e, topic, partition, offset, "failed to clear retry count after dlq - key will expire via ttl");
                                        }
                                    } else {
                                        match retry_tracker.increment_retry(&topic, partition, offset).await {
                                            Ok(()) => {
                                                tracing::warn!(topic, partition, offset, retry_count = retry_count + 1, "message processing failed, offset not committed - will be redelivered for retry");
                                            }
                                            Err(e) => {
                                                tracing::error!(error = %e, topic, partition, offset, "redis error during retry increment - offset not committed");
                                            }
                                        }
                                    }
                                    Ok(())
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::error!(error = %e, "kafka message error");
                            Err(e)
                        }
                        Err(_) => {
                            Ok(())
                        }
                    }
                } => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "consumer error");
                    }
                }
            }
        }

        tracing::info!("kafka consumer stopped");
    }

    async fn process_message_with_handler(
        handler: &Arc<dyn ExternalEventHandler>,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), String> {
        let topic = msg.topic();
        let partition = msg.partition();
        let offset = msg.offset();

        let parent_span_context = extract_trace_context(msg);

        let span = tracing::info_span!(
            "kafka.consume",
            otel.name = format!("consume {}", topic),
            topic = topic,
            partition = partition,
            offset = offset,
            messaging.system = "kafka",
            messaging.operation = "consume",
            messaging.destination = topic,
            messaging.destination_kind = "topic",
        );

        if let Some(parent_ctx) = parent_span_context {
            let parent_cx = opentelemetry::Context::new().with_remote_span_context(parent_ctx);
            let _ = span.set_parent(parent_cx);
            tracing::debug!("trace context extracted from Kafka headers");
        } else {
            tracing::debug!("no trace context found in Kafka headers, creating new trace");
        }

        async move {
            let payload = match msg.payload() {
                Some(p) => p,
                None => {
                    tracing::warn!("empty message payload");
                    return Ok(());
                }
            };

            let payload_str = match std::str::from_utf8(payload) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "invalid UTF-8 in message payload");
                    return Ok(());
                }
            };

            tracing::debug!("processing message");

            let result = match topic {
                "gov.fine.created" => {
                    match serde_json::from_str::<GovFineCreatedEvent>(payload_str) {
                        Ok(event) => handler.handle_fine_created(event).await,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to parse gov.fine.created event");
                            return Ok(());
                        }
                    }
                }
                "market.order.paid" => {
                    match serde_json::from_str::<MarketOrderPaidEvent>(payload_str) {
                        Ok(event) => handler.handle_order_paid(event).await,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to parse market.order.paid event");
                            return Ok(());
                        }
                    }
                }
                "donate.topup" => match serde_json::from_str::<DonateTopupEvent>(payload_str) {
                    Ok(event) => handler.handle_donate_topup(event).await,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to parse donate.topup event");
                        return Ok(());
                    }
                },
                _ => {
                    tracing::warn!("unknown topic, skipping");
                    return Ok(());
                }
            };

            match result {
                Ok(()) => {
                    tracing::info!("event processed successfully");
                    Ok(())
                }
                Err(e) => {
                    let error_reason = e.to_string();
                    tracing::error!(
                        error = %error_reason,
                        "failed to process event - will be redelivered"
                    );
                    Err(error_reason)
                }
            }
        }
        .instrument(span)
        .await
    }
}

trait TimeoutExt {
    async fn timeout(self, duration: Duration) -> Result<Self::Output, tokio::time::error::Elapsed>
    where
        Self: std::future::Future;
}

impl<F> TimeoutExt for F
where
    F: std::future::Future,
{
    async fn timeout(self, duration: Duration) -> Result<F::Output, tokio::time::error::Elapsed> {
        tokio::time::timeout(duration, self).await
    }
}

pub async fn check_kafka_connectivity(
    bootstrap_servers: &str,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(timeout_secs);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("request.timeout.ms", "5000")
        .set("socket.timeout.ms", "5000")
        .create()
        .map_err(|e| anyhow::anyhow!("failed to create connectivity check client: {}", e))?;

    tokio::time::timeout(timeout, async {
        producer
            .client()
            .fetch_metadata(None, Duration::from_secs(5))
    })
    .await
    .map_err(|_| anyhow::anyhow!("kafka connectivity check timed out"))?
    .map_err(|e| anyhow::anyhow!("kafka broker unavailable: {}", e))?;

    Ok(())
}

pub async fn start_consumer_with_retry(
    config: KafkaConsumerConfig,
    handler: Arc<dyn ExternalEventHandler>,
    mut shutdown_rx: mpsc::Receiver<ConsumerCommand>,
    check_interval_secs: u64,
    max_connect_retries: u32,
    connect_timeout_secs: u64,
    retry_tracker: Arc<RetryTracker>,
    dlq_producer: Arc<DlqProducer>,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    if let Err(e) = retry_tracker.health_check().await {
        tracing::warn!(
            error = %e,
            "redis unavailable at startup - consumer will start in degraded mode with in-memory retry tracking"
        );
    }

    match check_kafka_connectivity(&config.bootstrap_servers, 10).await {
        Ok(()) => {
            tracing::info!("kafka broker is available, starting consumer immediately");
            let consumer = KafkaEventConsumer::new(
                &config,
                handler,
                shutdown_rx,
                retry_tracker,
                dlq_producer,
            )?;
            let handle = tokio::spawn(async move {
                consumer.run().await;
            });
            return Ok(Some(handle));
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                bootstrap_servers = %config.bootstrap_servers,
                "kafka broker unavailable at startup, starting in degraded mode"
            );
        }
    }

    let bootstrap_servers = config.bootstrap_servers.clone();
    let retry_handler = handler.clone();
    let (consumer_started_tx, mut consumer_started_rx) = mpsc::channel::<()>(1);
    let retry_tracker_clone = retry_tracker.clone();
    let dlq_producer_clone = dlq_producer.clone();

    let retry_handle = tokio::spawn(async move {
        let started_at = tokio::time::Instant::now();
        let mut attempts: u32 = 0;

        loop {
            if connect_timeout_secs > 0
                && started_at.elapsed() >= Duration::from_secs(connect_timeout_secs)
            {
                tracing::error!(
                    elapsed_secs = started_at.elapsed().as_secs(),
                    connect_timeout_secs,
                    "kafka reconnection timed out - service continues in degraded mode, external event processing disabled"
                );
                break;
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(check_interval_secs)) => {}
                cmd = shutdown_rx.recv() => {
                    match cmd {
                        Some(ConsumerCommand::Shutdown) | None => {
                            tracing::info!("received shutdown command during kafka retry loop");
                            break;
                        }
                    }
                }
            }

            attempts += 1;
            let kafka_ok = check_kafka_connectivity(&bootstrap_servers, 10)
                .await
                .is_ok();

            if kafka_ok {
                let redis_ok = retry_tracker_clone.health_check().await.is_ok();
                if redis_ok {
                    tracing::info!(
                        attempt = attempts,
                        "kafka broker and redis are now available, starting consumer"
                    );
                } else {
                    tracing::warn!(
                        attempt = attempts,
                        "kafka broker available but redis is not - starting consumer with in-memory retry tracking"
                    );
                }

                let (_new_shutdown_tx, new_shutdown_rx) = mpsc::channel::<ConsumerCommand>(1);

                let _ = consumer_started_tx.send(()).await;

                match KafkaEventConsumer::new(
                    &config,
                    retry_handler.clone(),
                    new_shutdown_rx,
                    retry_tracker_clone.clone(),
                    dlq_producer_clone.clone(),
                ) {
                    Ok(consumer) => {
                        consumer.run().await;
                        tracing::info!("kafka consumer stopped");
                        break;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to create kafka consumer even after connectivity check succeeded");
                        continue;
                    }
                }
            } else {
                tracing::debug!(
                    attempt = attempts,
                    max_connect_retries,
                    "kafka broker still unavailable",
                );
            }
        }
    });

    tokio::spawn(async move {
        if consumer_started_rx.recv().await.is_some() {
            tracing::info!("consumer started successfully in background after retry");
        }
    });

    Ok(Some(retry_handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use std::sync::{Arc, Mutex};

    struct MockEventHandler {
        fine_events: Mutex<Vec<GovFineCreatedEvent>>,
        order_events: Mutex<Vec<MarketOrderPaidEvent>>,
        donate_events: Mutex<Vec<DonateTopupEvent>>,
        should_fail: bool,
    }

    impl MockEventHandler {
        fn new() -> Self {
            Self {
                fine_events: Mutex::new(Vec::new()),
                order_events: Mutex::new(Vec::new()),
                donate_events: Mutex::new(Vec::new()),
                should_fail: false,
            }
        }

        fn with_failure() -> Self {
            Self {
                fine_events: Mutex::new(Vec::new()),
                order_events: Mutex::new(Vec::new()),
                donate_events: Mutex::new(Vec::new()),
                should_fail: true,
            }
        }

        fn get_fine_count(&self) -> usize {
            self.fine_events.lock().unwrap().len()
        }

        fn get_order_count(&self) -> usize {
            self.order_events.lock().unwrap().len()
        }

        fn get_donate_count(&self) -> usize {
            self.donate_events.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl ExternalEventHandler for MockEventHandler {
        async fn handle_fine_created(
            &self,
            event: GovFineCreatedEvent,
        ) -> Result<(), ExternalEventError> {
            if self.should_fail {
                return Err(ExternalEventError::AccountNotFound {
                    user_id: event.user_id.clone(),
                });
            }
            self.fine_events.lock().unwrap().push(event);
            Ok(())
        }

        async fn handle_order_paid(
            &self,
            event: MarketOrderPaidEvent,
        ) -> Result<(), ExternalEventError> {
            if self.should_fail {
                return Err(ExternalEventError::AccountNotFound {
                    user_id: event.buyer_id.clone(),
                });
            }
            self.order_events.lock().unwrap().push(event);
            Ok(())
        }

        async fn handle_donate_topup(
            &self,
            event: DonateTopupEvent,
        ) -> Result<(), ExternalEventError> {
            if self.should_fail {
                return Err(ExternalEventError::AccountNotFound {
                    user_id: event.recipient_id.clone(),
                });
            }
            self.donate_events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[test]
    fn kafka_consumer_config_default_values() {
        let config = KafkaConsumerConfig::default();

        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.group_id, "bank-service-consumer");
        assert_eq!(config.topics.len(), 3);
        assert!(config.topics.contains(&"gov.fine.created".to_string()));
        assert!(config.topics.contains(&"market.order.paid".to_string()));
        assert!(config.topics.contains(&"donate.topup".to_string()));
        assert_eq!(config.session_timeout_ms, 10000);
        assert_eq!(config.auto_offset_reset, "earliest");
    }

    #[tokio::test]
    async fn mock_handler_processes_fine_event() {
        let handler = Arc::new(MockEventHandler::new());
        let event = GovFineCreatedEvent::new("fine-123", "user-456", 5000, "Speeding", "idem-789");

        let result = handler.handle_fine_created(event).await;

        assert!(result.is_ok());
        assert_eq!(handler.get_fine_count(), 1);
    }

    #[tokio::test]
    async fn mock_handler_processes_order_event() {
        let handler = Arc::new(MockEventHandler::new());
        let event =
            MarketOrderPaidEvent::new("order-456", "buyer-789", 15000, "seller-123", "idem-abc");

        let result = handler.handle_order_paid(event).await;

        assert!(result.is_ok());
        assert_eq!(handler.get_order_count(), 1);
    }

    #[tokio::test]
    async fn mock_handler_processes_donate_event() {
        let handler = Arc::new(MockEventHandler::new());
        let event = DonateTopupEvent::new("donation-789", "streamer-123", 1000, "idem-def");

        let result = handler.handle_donate_topup(event).await;

        assert!(result.is_ok());
        assert_eq!(handler.get_donate_count(), 1);
    }

    #[tokio::test]
    async fn mock_handler_returns_error_on_failure() {
        let handler = Arc::new(MockEventHandler::with_failure());
        let event = GovFineCreatedEvent::new("fine-001", "user-001", 1000, "Test", "idem-001");

        let result = handler.handle_fine_created(event).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ExternalEventError::AccountNotFound { .. }
        ));
    }

    #[test]
    fn non_utf8_payload_uses_base64_in_dlq_message() {
        let non_utf8: Vec<u8> = vec![0x80, 0x90, 0xa0];
        let json = serde_json::json!({
            "payload": match std::str::from_utf8(&non_utf8) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&non_utf8);
                    serde_json::json!({"base64": encoded})
                }
            }
        });
        let payload_obj = json.get("payload").unwrap();
        assert!(payload_obj.get("base64").is_some());
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(payload_obj.get("base64").unwrap().as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, non_utf8);
    }

    #[tokio::test]
    async fn shutdown_signal_responds_immediately() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ConsumerCommand>(1);
        tx.send(ConsumerCommand::Shutdown).await.unwrap();

        let received = tokio::select! {
            cmd = rx.recv() => cmd,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => None,
        };
        assert!(matches!(received, Some(ConsumerCommand::Shutdown)));
    }

    #[test]
    fn external_event_error_display() {
        let err = ExternalEventError::AccountNotFound {
            user_id: "user-123".to_string(),
        };
        assert!(err.to_string().contains("user-123"));

        let err = ExternalEventError::InsufficientFunds {
            user_id: "user-456".to_string(),
        };
        assert!(err.to_string().contains("user-456"));

        let err = ExternalEventError::InvalidAmount("invalid".to_string());
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn invalid_account_format_error_display() {
        let err = ExternalEventError::InvalidAccountFormat("bad-number".to_string());
        assert!(err.to_string().contains("bad-number"));
    }

    #[test]
    fn retry_tracker_error_is_connection_error() {
        let err = RetryTrackerError::RedisUnavailable("connection refused".to_string());
        assert!(err.is_connection_error());
    }

    #[test]
    fn retry_tracker_error_display() {
        let err = RetryTrackerError::RedisUnavailable("timeout".to_string());
        assert!(err.to_string().contains("redis unavailable"));
        assert!(err.to_string().contains("timeout"));
    }
}
