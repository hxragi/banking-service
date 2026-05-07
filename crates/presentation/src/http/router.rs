use std::sync::Arc;

use super::handlers::accounts::{
    AccountHttpHandler, create_account, deposit, get_account, get_accounts, get_transactions,
    transfer, withdraw,
};
use application::ports::HealthPort;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;
use tracing::info_span;

pub fn create_router<H>(handler: Arc<AccountHttpHandler>, health: Arc<H>) -> Router<()>
where
    H: HealthPort + 'static,
{
    let account_routes = Router::new()
        .route("/accounts", post(create_account).get(get_accounts))
        .route("/accounts/{account_number}", get(get_account))
        .route("/accounts/{account_number}/deposit", post(deposit))
        .route("/accounts/{account_number}/withdraw", post(withdraw))
        .route(
            "/accounts/{account_number}/transactions",
            get(get_transactions),
        )
        .route("/transfers", post(transfer))
        .with_state((*handler).clone());

    let health_routes = Router::new()
        .route("/health", get(health_check::<H>))
        .route("/ready", get(readiness_check::<H>))
        .with_state(health);

    Router::new()
        .merge(account_routes)
        .merge(health_routes)
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::extract::Request| {
                info_span!(
                    "http.request",
                    http.route = %request.uri().path(),
                )
            }),
        )
}

async fn health_check<H: HealthPort>(State(health): State<Arc<H>>) -> impl IntoResponse {
    let status = health.health().await;
    let code = if status.healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(status))
}

async fn readiness_check<H: HealthPort>(State(health): State<Arc<H>>) -> impl IntoResponse {
    let status = health.readiness().await;
    Json(status)
}
