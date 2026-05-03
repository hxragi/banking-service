use config::{Config, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_secs: u64,
    #[serde(default = "default_statement_timeout")]
    pub default_statement_timeout_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CacheConfig {
    #[serde(default = "default_balance_cache_ttl_secs")]
    pub balance_cache_ttl_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DragonflyConfig {
    #[serde(default = "default_dragonfly_url")]
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_grpc_host")]
    pub grpc_host: String,
    #[serde(default = "default_http_host")]
    pub http_host: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    #[serde(default = "default_kafka_producer_timeout")]
    pub producer_timeout_ms: u64,
    #[serde(default = "default_kafka_compression")]
    pub compression_type: String,
    #[serde(default = "default_kafka_max_retries")]
    pub max_retries: i32,
    #[serde(default = "default_kafka_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_kafka_app_max_retries")]
    pub app_max_retries: u32,
    #[serde(default = "default_kafka_app_retry_base_delay_ms")]
    pub app_retry_base_delay_ms: u64,
    #[serde(default = "default_kafka_app_retry_max_delay_ms")]
    pub app_retry_max_delay_ms: u64,
    #[serde(default = "default_kafka_consumer_connect_max_retries")]
    pub consumer_connect_max_retries: u32,
    #[serde(default = "default_kafka_consumer_connect_timeout_secs")]
    pub consumer_connect_timeout_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TransactionRetryConfig {
    #[serde(default = "default_tx_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_tx_retry_base_delay_ms")]
    pub base_delay_ms: u64,
    #[serde(default = "default_tx_retry_max_delay_ms")]
    pub max_delay_ms: u64,
}

impl Default for TransactionRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_tx_retry_max_attempts(),
            base_delay_ms: default_tx_retry_base_delay_ms(),
            max_delay_ms: default_tx_retry_max_delay_ms(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SentryConfig {
    #[serde(default = "default_sentry_dsn")]
    pub dsn: String,
    #[serde(default = "default_sentry_environment")]
    pub environment: String,
    #[serde(default = "default_sentry_sample_rate")]
    pub sample_rate: f32,
}

fn default_sentry_dsn() -> String {
    String::new()
}

#[derive(Debug, Deserialize, Clone)]
pub struct TelemetryConfig {
    #[serde(default = "default_otel_endpoint")]
    pub otel_endpoint: String,
    #[serde(default = "default_otel_timeout_secs")]
    pub otel_timeout_secs: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otel_endpoint: default_otel_endpoint(),
            otel_timeout_secs: default_otel_timeout_secs(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub cache: CacheConfig,
    pub dragonfly: DragonflyConfig,
    pub server: ServerConfig,
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub transaction_retry: TransactionRetryConfig,
    pub sentry: Option<SentryConfig>,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    pub internal_api_key: String,
    pub jwt_secret: String,
}

fn default_max_connections() -> u32 {
    5
}
fn default_connection_timeout() -> u64 {
    30
}
fn default_statement_timeout() -> u64 {
    30
}
fn default_balance_cache_ttl_secs() -> u64 {
    60
}
fn default_dragonfly_url() -> String {
    "redis://localhost:6379".to_string()
}
fn default_grpc_port() -> u16 {
    50051
}
fn default_http_port() -> u16 {
    8080
}
fn default_grpc_host() -> String {
    "[::1]".to_string()
}
fn default_http_host() -> String {
    "0.0.0.0".to_string()
}
fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}
fn default_kafka_producer_timeout() -> u64 {
    5000
}
fn default_kafka_compression() -> String {
    "snappy".to_string()
}
fn default_kafka_max_retries() -> i32 {
    3
}
fn default_kafka_retry_backoff_ms() -> u64 {
    100
}
fn default_kafka_app_max_retries() -> u32 {
    5
}
fn default_kafka_app_retry_base_delay_ms() -> u64 {
    100
}
fn default_kafka_app_retry_max_delay_ms() -> u64 {
    30000
}
fn default_sentry_environment() -> String {
    "production".to_string()
}
fn default_tx_retry_max_attempts() -> u32 {
    3
}
fn default_tx_retry_base_delay_ms() -> u64 {
    10
}
fn default_tx_retry_max_delay_ms() -> u64 {
    500
}
fn default_sentry_sample_rate() -> f32 {
    1.0
}
fn default_otel_endpoint() -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://jaeger:4318/v1/traces".to_string())
}
fn default_otel_timeout_secs() -> u64 {
    3
}
fn default_kafka_consumer_connect_max_retries() -> u32 {
    10
}
fn default_kafka_consumer_connect_timeout_secs() -> u64 {
    300
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<AppConfig> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(dotenvy::Error::Io(e)) => {
                tracing::debug!("No .env file found: {}", e);
            }
            Err(e) => {
                tracing::warn!("Error reading .env file: {}", e);
            }
        }
        let config = Config::builder()
            .add_source(Environment::default().separator("__"))
            .build()?;

        let app_config: AppConfig = config.try_deserialize()?;
        Ok(app_config)
    }
}
