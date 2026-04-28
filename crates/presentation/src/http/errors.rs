use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::http::extractors::owner_extractor::OwnerExtractionError;
use application::ports::OperationError;

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
            OperationError::RepositoryError { operation, reason } => {
                tracing::error!(operation = ?operation, reason = %reason, "repository error");
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
            OperationError::Deadlock => {
                HttpError::ResourceConflict("deadlock detected - please retry".into())
            }
            OperationError::SerializationFailure => {
                HttpError::ResourceConflict("serialization failure - please retry".into())
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
            OperationError::IdempotencyError { reason } => {
                HttpError::ResourceConflict(format!("idempotency conflict: {}", reason))
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

#[cfg(test)]
mod tests {
    use super::*;
    use application::ports::OperationError;

    #[test]
    fn idempotency_error_maps_to_conflict() {
        let http_err: HttpError = OperationError::IdempotencyError {
            reason: "test".to_string(),
        }
        .into();
        assert!(matches!(http_err, HttpError::ResourceConflict(_)));
    }

    #[test]
    fn not_found_maps_to_invalid_input() {
        let http_err: HttpError = OperationError::NotFound {
            resource: "account".to_string(),
        }
        .into();
        assert!(matches!(http_err, HttpError::InvalidInput(_)));
    }

    #[test]
    fn insufficient_funds_maps_to_invalid_input() {
        let http_err: HttpError = OperationError::InsufficientFunds.into();
        assert!(matches!(http_err, HttpError::InvalidInput(_)));
    }
}
