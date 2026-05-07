use crate::http::extractors::owner_extractor::OwnerExtractionError;
use application::ports::{OperationError, OperationErrorKind};

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug)]
pub enum HttpError {
    InvalidInput(String),
    ResourceConflict(String),
    SystemFailure(String),
    ServiceUnavailable(String),
}

impl From<OperationError> for HttpError {
    fn from(err: OperationError) -> Self {
        if let OperationError::RepositoryError { operation, reason } = &err {
            tracing::error!(operation = ?operation, reason = %reason, "repository error");
        }
        if let OperationError::ConnectionError(msg) = &err {
            tracing::error!(message = %msg, "connection error");
        }

        let msg = err.to_string();
        match err.kind() {
            OperationErrorKind::NotFound
            | OperationErrorKind::InvalidInput
            | OperationErrorKind::InsufficientFunds => HttpError::InvalidInput(msg),
            OperationErrorKind::TierLimitExceeded
            | OperationErrorKind::TierDowngradeNotAllowed
            | OperationErrorKind::UniqueConstraintViolation
            | OperationErrorKind::IdempotencyError
            | OperationErrorKind::LockTimeout
            | OperationErrorKind::Deadlock
            | OperationErrorKind::SerializationFailure => HttpError::ResourceConflict(msg),
            OperationErrorKind::Unavailable | OperationErrorKind::ConnectionError => {
                HttpError::ServiceUnavailable(msg)
            }
            OperationErrorKind::RepositoryError => {
                HttpError::SystemFailure("internal server error".into())
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
