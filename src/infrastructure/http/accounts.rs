use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::application::create_account::{
    CreateAccountError, CreateAccountInput, CreateAccountUseCase,
};
use crate::domain::owner::Owner;
use crate::domain::tier::Tier;
use crate::domain::user_id::UserId;

#[derive(Clone)]
pub struct AccountHttpHandler {
    create_account_use_case: Arc<CreateAccountUseCase>,
}

impl AccountHttpHandler {
    pub fn new(create_account_use_case: Arc<CreateAccountUseCase>) -> Self {
        Self {
            create_account_use_case,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateAccountBody {
    pub tier: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct AccountResponse {
    pub id: String,
    pub number: String,
    pub owner: OwnerResponse,
    pub balance: u64,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "id")]
pub enum OwnerResponse {
    #[serde(rename = "user")]
    User { id: String },
    #[serde(rename = "org")]
    Org { id: String },
}

fn parse_tier(value: Option<u32>) -> Result<Tier, HttpError> {
    match value.unwrap_or(1) {
        1 => Ok(Tier::Basic),
        2 => Ok(Tier::Premium),
        3 => Ok(Tier::Elite),
        _ => Err(HttpError::BadRequest("tier must be 1, 2, or 3".into())),
    }
}

fn domain_to_http_account(account: &crate::domain::account::Account) -> AccountResponse {
    let owner = match account.owner() {
        Owner::User(user_id) => OwnerResponse::User {
            id: user_id.as_str().to_owned(),
        },
        Owner::Org(org_id) => OwnerResponse::Org {
            id: org_id.as_str().to_owned(),
        },
    };

    AccountResponse {
        id: account.id().to_string(),
        number: account.number().as_str().to_owned(),
        owner,
        balance: account.balance().as_u64(),
        created_at: account
            .created_at()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| account.created_at().to_string()),
    }
}

#[derive(Debug)]
pub(crate) enum HttpError {
    BadRequest(String),
    Conflict(String),
    Internal(String),
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            HttpError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            HttpError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            HttpError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<CreateAccountError> for HttpError {
    fn from(err: CreateAccountError) -> Self {
        match err {
            CreateAccountError::TierLimitExceeded => {
                HttpError::Conflict("account limit exceeded for this tier".into())
            }
            CreateAccountError::AccountRepository(_) => {
                HttpError::Internal("internal server error".into())
            }
            CreateAccountError::AccountNumberGenerator(_) => {
                HttpError::Internal("internal server error".into())
            }
        }
    }
}

pub async fn create_account(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Option<CreateAccountBody>>,
) -> Result<(StatusCode, Json<AccountResponse>), HttpError> {
    let user_id_str = headers
        .get("X-USER-ID")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| HttpError::BadRequest("missing X-USER-ID header".into()))?;

    let user_id = UserId::new(user_id_str)
        .map_err(|_| HttpError::BadRequest("invalid X-USER-ID header".into()))?;

    let tier = parse_tier(body.and_then(|b| b.tier))?;

    let input = CreateAccountInput {
        owner: Owner::User(user_id),
        tier,
    };

    let account = handler.create_account_use_case.execute(input).await?;

    let response = domain_to_http_account(&account);

    Ok((StatusCode::CREATED, Json(response)))
}
