use std::sync::Arc;

use axum::{Router, routing::post};

use crate::application::create_account::CreateAccountUseCase;

pub mod accounts;

pub fn create_router(create_account_use_case: Arc<CreateAccountUseCase>) -> Router {
    let handler = accounts::AccountHttpHandler::new(create_account_use_case);

    Router::new()
        .route("/accounts", post(accounts::create_account))
        .with_state(handler)
}
