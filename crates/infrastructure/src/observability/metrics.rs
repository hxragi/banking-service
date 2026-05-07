use std::sync::Arc;

use application::ports::MetricsPort;

use axum::{Router, body::Body, http::StatusCode, response::Response, routing::get};
use prometheus::{Encoder, IntCounterVec, Opts, Registry};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub operations_total: IntCounterVec,
    pub errors_total: IntCounterVec,
    pub cache_hits: IntCounterVec,
    pub cache_misses: IntCounterVec,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();

        let operations_total = IntCounterVec::new(
            Opts::new(
                "operations_total",
                "total number of operations by type and status",
            ),
            &["operation", "status"],
        )?;
        registry.register(Box::new(operations_total.clone()))?;

        let errors_total = IntCounterVec::new(
            Opts::new("errors_total", "total number of errors by type"),
            &["error_type", "operation"],
        )?;
        registry.register(Box::new(errors_total.clone()))?;

        let cache_hits = IntCounterVec::new(
            Opts::new("cache_hits_total", "total cache hits by type"),
            &["cache_type"],
        )?;
        registry.register(Box::new(cache_hits.clone()))?;

        let cache_misses = IntCounterVec::new(
            Opts::new("cache_misses_total", "total cache misses by type"),
            &["cache_type"],
        )?;
        registry.register(Box::new(cache_misses.clone()))?;

        Ok(Self {
            registry,
            operations_total,
            errors_total,
            cache_hits,
            cache_misses,
        })
    }

    pub fn increment_operation(&self, operation: &str, status: &str) {
        self.operations_total
            .with_label_values(&[operation, status])
            .inc();
    }

    pub fn record_error(&self, error_type: &str, operation: &str) {
        self.errors_total
            .with_label_values(&[error_type, operation])
            .inc();
    }

    pub fn increment_cache_hit(&self, cache_type: &str) {
        self.cache_hits.with_label_values(&[cache_type]).inc();
    }

    pub fn increment_cache_miss(&self, cache_type: &str) {
        self.cache_misses.with_label_values(&[cache_type]).inc();
    }
}

pub fn setup_metrics() -> anyhow::Result<Arc<Metrics>> {
    let metrics = Metrics::new()?;
    Ok(Arc::new(metrics))
}

pub fn create_metrics_router(registry: Registry) -> Router {
    Router::new().route(
        "/metrics",
        get(move || async move {
            let encoder = prometheus::TextEncoder::new();
            let metric_families = registry.gather();
            let mut buffer = Vec::new();
            match encoder.encode(&metric_families, &mut buffer) {
                Ok(_) => Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(Body::from(buffer))
                    .unwrap(),
                Err(err) => Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(format!("Failed to encode metrics: {}", err)))
                    .unwrap(),
            }
        }),
    )
}

impl MetricsPort for Metrics {
    fn increment_operation(&self, operation: &str, status: &str) {
        self.operations_total
            .with_label_values(&[operation, status])
            .inc();
    }

    fn record_error(&self, error_type: &str, operation: &str) {
        self.errors_total
            .with_label_values(&[error_type, operation])
            .inc();
    }
}
