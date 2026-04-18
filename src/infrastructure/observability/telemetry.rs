use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};

use crate::infrastructure::config::config::AppConfig;

pub fn init_telemetry(
    service_name: &str,
    config: &AppConfig,
) -> anyhow::Result<opentelemetry_sdk::trace::TracerProvider> {
    let endpoint = config.telemetry.otel_endpoint.clone();
    let timeout_secs = config.telemetry.otel_timeout_secs;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(timeout_secs))
        .build()?;

    let resource = Resource::new([
        KeyValue::new(SERVICE_NAME, service_name.to_string()),
        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
    ]);

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    Ok(provider)
}
