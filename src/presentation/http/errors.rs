use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::application::ports::OperationError;
use crate::infrastructure::services::owner_extractor::OwnerExtractionError;

#[derive(Debug)]
pub enum HttpError {
    InvalidInput(String),
    ResourceConflict(String),
    SystemFailure(String),
    ServiceUnavailable(String),
}

impl From<OperationError> for HttpError {
    fn from(err: OperationError) -> Self {
        match err {
            OperationError::NotFound { resource } => {
                HttpError::InvalidInput(format!("{} not found", resource))
            }
            OperationError::Unavailable { reason } => HttpError::ServiceUnavailable(reason),
            OperationError::InsufficientFunds => {
                HttpError::InvalidInput("insufficient funds".into())
            }
            OperationError::InvalidInput { field, reason } => {
                HttpError::InvalidInput(format!("invalid {}: {}", field, reason))
            }
            OperationError::RepositoryError { operation } => {
                tracing::error!(operation = ?operation, "repository error");
                HttpError::SystemFailure("internal server error".into())
            }
            OperationError::TierLimitExceeded => {
                HttpError::ResourceConflict("account limit exceeded for this tier".into())
            }
            OperationError::TierDowngradeNotAllowed { reason } => {
                HttpError::ResourceConflict(format!("tier downgrade not allowed: {}", reason))
            }
            OperationError::LockTimeout => {
                HttpError::ResourceConflict("lock timeout - please retry".into())
            }
            OperationError::UniqueConstraintViolation(msg) => {
                HttpError::ResourceConflict(format!("conflict: {}", msg))
            }
            OperationError::ConnectionError(msg) => {
                tracing::error!(message = %msg, "connection error");
                HttpError::ServiceUnavailable(
                    "database temporarily unavailable - please retry".into(),
                )
            }
            OperationError::IdempotencyError => {
                HttpError::SystemFailure("idempotency error".into())
            }
        }
    }
}

impl From<OwnerExtractionError> for HttpError {
    fn from(err: OwnerExtractionError) -> Self {
        match err {
            OwnerExtractionError::MissingOwner => {
                HttpError::InvalidInput("must specify user_id or org_id".into())
            }
            OwnerExtractionError::InvalidUserId => {
                HttpError::InvalidInput("invalid user_id".into())
            }
            OwnerExtractionError::InvalidOrgId => HttpError::InvalidInput("invalid org_id".into()),
            OwnerExtractionError::BothUserAndOrgProvided => {
                HttpError::InvalidInput("cannot specify both user_id and org_id".into())
            }
            OwnerExtractionError::OrgNotAllowed => {
                HttpError::InvalidInput("org operations can only be done via internal API".into())
            }
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            HttpError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            HttpError::ResourceConflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            HttpError::SystemFailure(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            HttpError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
        };

        #[derive(Serialize)]
        struct ErrorResponse {
            error: String,
        }

        (status, Json(ErrorResponse { error: message })).into_response()
    }
}
