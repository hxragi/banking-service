use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;
use tracing::info_span;

use crate::infrastructure::http::accounts::AccountHttpHandler;

pub fn create_router(handler: Arc<AccountHttpHandler>) -> Router {
    Router::new()
        .route(
            "/accounts",
            post(crate::infrastructure::http::accounts::create_account)
                .get(crate::infrastructure::http::accounts::get_accounts),
        )
        .route(
            "/accounts/{account_number}",
            get(crate::infrastructure::http::accounts::get_account),
        )
        .route(
            "/accounts/{account_number}/deposit",
            post(crate::infrastructure::http::accounts::deposit),
        )
        .route(
            "/accounts/{account_number}/withdraw",
            post(crate::infrastructure::http::accounts::withdraw),
        )
        .route(
            "/accounts/{account_number}/transactions",
            get(crate::infrastructure::http::accounts::get_transactions),
        )
        .route(
            "/transfers",
            post(crate::infrastructure::http::accounts::transfer),
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
