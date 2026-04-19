use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;
use tracing::info_span;

use super::handlers::accounts::AccountHttpHandler;

pub fn create_router(handler: Arc<AccountHttpHandler>) -> Router {
    Router::new()
        .route(
            "/accounts",
            post(super::handlers::accounts::create_account)
                .get(super::handlers::accounts::get_accounts),
        )
        .route(
            "/accounts/{account_number}",
            get(super::handlers::accounts::get_account),
        )
        .route(
            "/accounts/{account_number}/deposit",
            post(super::handlers::accounts::deposit),
        )
        .route(
            "/accounts/{account_number}/withdraw",
            post(super::handlers::accounts::withdraw),
        )
        .route(
            "/accounts/{account_number}/transactions",
            get(super::handlers::accounts::get_transactions),
        )
        .route(
            "/transfers",
            post(super::handlers::accounts::transfer),
        )
        .with_state((*handler).clone())
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::extract::Request| {
                info_span!(
                    "http.request",
                    http.route = %request.uri().path(),
                )
            }),
        )
}
