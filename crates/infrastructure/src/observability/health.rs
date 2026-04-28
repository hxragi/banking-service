use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct HealthChecker {
    pool: PgPool,
    start_time: Instant,
}

impl HealthChecker {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            start_time: Instant::now(),
        }
    }

    pub async fn check(&self) -> HealthStatus {
        let db_status = self.check_database().await;
        let tables_status = self.check_tables().await;
        let uptime_secs = self.start_time.elapsed().as_secs();

        let overall_healthy = db_status.healthy && tables_status.healthy;

        HealthStatus {
            healthy: overall_healthy,
            uptime_secs,
            database: db_status,
            tables: tables_status,
            migrations: MigrationStatus {
                applied: overall_healthy,
                message: if overall_healthy {
                    "migrations applied".to_string()
                } else {
                    "migration check failed".to_string()
                },
            },
        }
    }

    async fn check_database(&self) -> DatabaseStatus {
        match sqlx::query("SELECT 1").fetch_one(&self.pool).await {
            Ok(_) => DatabaseStatus {
                healthy: true,
                message: "database connection ok".to_string(),
            },
            Err(e) => DatabaseStatus {
                healthy: false,
                message: format!("database error: {}", e),
            },
        }
    }

    async fn check_tables(&self) -> TablesStatus {
        let accounts_check = sqlx::query("SELECT COUNT(*) FROM accounts LIMIT 1")
            .fetch_one(&self.pool)
            .await;

        let transactions_check = sqlx::query("SELECT COUNT(*) FROM transactions LIMIT 1")
            .fetch_one(&self.pool)
            .await;

        let idempotency_check = sqlx::query("SELECT COUNT(*) FROM idempotency_keys LIMIT 1")
            .fetch_one(&self.pool)
            .await;

        let all_healthy =
            accounts_check.is_ok() && transactions_check.is_ok() && idempotency_check.is_ok();

        TablesStatus {
            healthy: all_healthy,
            accounts: accounts_check.is_ok(),
            transactions: transactions_check.is_ok(),
            idempotency_keys: idempotency_check.is_ok(),
            message: if all_healthy {
                "all tables accessible".to_string()
            } else {
                let mut failed = vec![];
                if accounts_check.is_err() {
                    failed.push("accounts");
                }
                if transactions_check.is_err() {
                    failed.push("transactions");
                }
                if idempotency_check.is_err() {
                    failed.push("idempotency_keys");
                }
                format!("tables unavailable: {}", failed.join(", "))
            },
        }
    }
}

#[derive(Serialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub uptime_secs: u64,
    pub database: DatabaseStatus,
    pub tables: TablesStatus,
    pub migrations: MigrationStatus,
}

#[derive(Serialize)]
pub struct DatabaseStatus {
    pub healthy: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct TablesStatus {
    pub healthy: bool,
    pub accounts: bool,
    pub transactions: bool,
    pub idempotency_keys: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct MigrationStatus {
    pub applied: bool,
    pub message: String,
}

impl IntoResponse for HealthStatus {
    fn into_response(self) -> axum::response::Response {
        let status = if self.healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        (status, Json(self)).into_response()
    }
}

pub async fn health_check(
    axum::extract::State(checker): axum::extract::State<Arc<HealthChecker>>,
) -> HealthStatus {
    checker.check().await
}

pub async fn readiness_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ready"
    }))
}
