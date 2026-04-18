use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::application::{
    create_account::{CreateAccountInput, CreateAccountUseCase},
    deposit::{DepositInput, DepositUseCase},
    get_account::{GetAccountInput, GetAccountUseCase},
    get_accounts::{GetAccountsInput, GetAccountsUseCase},
    get_transactions::{GetTransactionsInput, GetTransactionsUseCase},
    ports::OperationError,
    transfer::{TransferInput, TransferUseCase},
    withdraw::{WithdrawInput, WithdrawUseCase},
};
use crate::domain::account::Account;
use crate::domain::account_number::AccountNumber;
use crate::domain::amount::Amount;
use crate::domain::owner::Owner;
use crate::domain::transaction_kind::TransactionKind;
use crate::domain::user_id::UserId;
use crate::infrastructure::services::idempotency_service::IdempotencyService;
use crate::infrastructure::services::owner_extractor::{OwnerExtractionError, OwnerExtractor};

#[derive(Clone)]
pub struct AccountHttpHandler {
    create_account_use_case: Arc<CreateAccountUseCase>,
    get_account_use_case: Arc<GetAccountUseCase>,
    get_accounts_use_case: Arc<GetAccountsUseCase>,
    deposit_use_case: Arc<DepositUseCase>,
    withdraw_use_case: Arc<WithdrawUseCase>,
    transfer_use_case: Arc<TransferUseCase>,
    get_transactions_use_case: Arc<GetTransactionsUseCase>,
    idempotency_service: Arc<IdempotencyService>,
}

impl AccountHttpHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        create_account_use_case: Arc<CreateAccountUseCase>,
        get_account_use_case: Arc<GetAccountUseCase>,
        get_accounts_use_case: Arc<GetAccountsUseCase>,
        deposit_use_case: Arc<DepositUseCase>,
        withdraw_use_case: Arc<WithdrawUseCase>,
        transfer_use_case: Arc<TransferUseCase>,
        get_transactions_use_case: Arc<GetTransactionsUseCase>,
        idempotency_service: Arc<IdempotencyService>,
    ) -> Self {
        Self {
            create_account_use_case,
            get_account_use_case,
            get_accounts_use_case,
            deposit_use_case,
            withdraw_use_case,
            transfer_use_case,
            get_transactions_use_case,
            idempotency_service,
        }
    }

    async fn verify_account_ownership(
        &self,
        account_number: &AccountNumber,
        user_id: &UserId,
        has_internal_api_key: bool,
    ) -> Result<(), HttpError> {
        let input = GetAccountInput {
            account_number: account_number.clone(),
        };
        let account = self.get_account_use_case.execute(input).await?;

        match account.owner() {
            Owner::User(account_user_id) => {
                if account_user_id.as_str() != user_id.as_str() {
                    return Err(HttpError::InvalidInput("account not found".into()));
                }
            }
            Owner::Org(_) => {
                if !has_internal_api_key {
                    return Err(HttpError::InvalidInput("account not found".into()));
                }
            }
        }

        Ok(())
    }

    async fn check_idempotency(
        &self,
        idempotency_key: &Option<String>,
    ) -> Result<(Option<String>, Option<String>), HttpError> {
        let key = match idempotency_key {
            Some(k) => k,
            None => return Ok((None, None)),
        };

        let check = self
            .idempotency_service
            .check_or_acquire(key)
            .await
            .map_err(|_| HttpError::SystemFailure("idempotency check failed".into()))?;

        if let Some(cached) = check.cached_response {
            return Ok((Some(key.clone()), Some(cached)));
        }

        Ok((Some(key.clone()), None))
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateAccountBody {}

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

fn extract_user_id(headers: &axum::http::HeaderMap) -> Result<UserId, HttpError> {
    let user_id_str = headers
        .get("X-USER-ID")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| HttpError::InvalidInput("missing X-USER-ID header".into()))?;

    UserId::new(user_id_str).map_err(|_| HttpError::InvalidInput("invalid X-USER-ID header".into()))
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

fn domain_to_http_account(account: &Account) -> AccountResponse {
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
pub enum HttpError {
    InvalidInput(String),
    ResourceConflict(String),
    SystemFailure(String),
    ServiceUnavailable(String),
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

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

#[derive(Debug, Deserialize)]
pub struct DepositBody {
    pub amount: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DepositResponse {
    pub account: AccountResponse,
    pub idempotency_key: String,
}

#[derive(Debug, Deserialize)]
pub struct WithdrawBody {
    pub amount: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WithdrawResponse {
    pub account: AccountResponse,
    pub idempotency_key: String,
}

#[derive(Debug, Deserialize)]
pub struct TransferBody {
    pub from_account_number: String,
    pub to_account_number: String,
    pub amount: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TransactionResponse {
    pub id: String,
    pub kind: String,
    pub amount: u64,
    pub from_account_number: Option<String>,
    pub to_account_number: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct TransferResponse {
    pub transaction: Option<TransactionResponse>,
    pub idempotency_key: String,
}

pub async fn create_account(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Json(_body): Json<Option<CreateAccountBody>>,
) -> Result<(StatusCode, Json<AccountResponse>), HttpError> {
    let user_id = extract_user_id(&headers)?;

    let input = CreateAccountInput {
        owner: Owner::User(user_id),
    };

    let account = handler.create_account_use_case.execute(input).await?;

    let response = domain_to_http_account(&account);

    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn deposit(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Path(account_number): Path<String>,
    Json(body): Json<DepositBody>,
) -> Result<(StatusCode, Json<DepositResponse>), HttpError> {
    let user_id = extract_user_id(&headers)?;

    let (idempotency_key, cached) = handler.check_idempotency(&body.idempotency_key).await?;

    if let Some(cached_key) = cached {
        let account_number_parsed = AccountNumber::new(&account_number)
            .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;
        let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
        handler
            .verify_account_ownership(&account_number_parsed, &user_id, has_internal_key)
            .await?;
        let input = GetAccountInput {
            account_number: account_number_parsed,
        };
        let account = handler.get_account_use_case.execute(input).await?;
        return Ok((
            StatusCode::OK,
            Json(DepositResponse {
                account: domain_to_http_account(&account),
                idempotency_key: cached_key,
            }),
        ));
    }

    let account_number = AccountNumber::new(&account_number)
        .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;

    let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
    handler
        .verify_account_ownership(&account_number, &user_id, has_internal_key)
        .await?;

    let amount =
        Amount::new(body.amount).map_err(|_| HttpError::InvalidInput("invalid amount".into()))?;

    let input = DepositInput {
        account_number,
        amount,
        idempotency_key: idempotency_key.clone(),
    };
    let account = handler.deposit_use_case.execute(input).await?;

    let response_key = idempotency_key
        .clone()
        .unwrap_or_else(IdempotencyService::generate_key);

    if let Some(key) = idempotency_key {
        let _ = handler
            .idempotency_service
            .save_response(&key, &response_key)
            .await;
    }

    Ok((
        StatusCode::OK,
        Json(DepositResponse {
            account: domain_to_http_account(&account),
            idempotency_key: response_key,
        }),
    ))
}

pub async fn withdraw(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Path(account_number): Path<String>,
    Json(body): Json<WithdrawBody>,
) -> Result<(StatusCode, Json<WithdrawResponse>), HttpError> {
    let user_id = extract_user_id(&headers)?;

    let (idempotency_key, cached) = handler.check_idempotency(&body.idempotency_key).await?;

    if let Some(cached_key) = cached {
        let account_number_parsed = AccountNumber::new(&account_number)
            .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;
        let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
        handler
            .verify_account_ownership(&account_number_parsed, &user_id, has_internal_key)
            .await?;
        let input = GetAccountInput {
            account_number: account_number_parsed,
        };
        let account = handler.get_account_use_case.execute(input).await?;
        return Ok((
            StatusCode::OK,
            Json(WithdrawResponse {
                account: domain_to_http_account(&account),
                idempotency_key: cached_key,
            }),
        ));
    }

    let account_number = AccountNumber::new(&account_number)
        .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;

    let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
    handler
        .verify_account_ownership(&account_number, &user_id, has_internal_key)
        .await?;

    let amount =
        Amount::new(body.amount).map_err(|_| HttpError::InvalidInput("invalid amount".into()))?;

    let input = WithdrawInput {
        account_number,
        amount,
        idempotency_key: idempotency_key.clone(),
    };
    let account = handler.withdraw_use_case.execute(input).await?;

    let response_key = idempotency_key
        .clone()
        .unwrap_or_else(IdempotencyService::generate_key);

    if let Some(key) = idempotency_key {
        let _ = handler
            .idempotency_service
            .save_response(&key, &response_key)
            .await;
    }

    Ok((
        StatusCode::OK,
        Json(WithdrawResponse {
            account: domain_to_http_account(&account),
            idempotency_key: response_key,
        }),
    ))
}

pub async fn transfer(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Json(body): Json<TransferBody>,
) -> Result<(StatusCode, Json<TransferResponse>), HttpError> {
    let user_id = extract_user_id(&headers)?;

    let (idempotency_key, cached) = handler.check_idempotency(&body.idempotency_key).await?;

    if let Some(cached_key) = cached {
        return Ok((
            StatusCode::OK,
            Json(TransferResponse {
                transaction: None,
                idempotency_key: cached_key,
            }),
        ));
    }

    let from_account_number = AccountNumber::new(&body.from_account_number)
        .map_err(|_| HttpError::InvalidInput("invalid from account number".into()))?;
    let to_account_number = AccountNumber::new(&body.to_account_number)
        .map_err(|_| HttpError::InvalidInput("invalid to account number".into()))?;

    let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
    handler
        .verify_account_ownership(&from_account_number, &user_id, has_internal_key)
        .await?;

    let amount =
        Amount::new(body.amount).map_err(|_| HttpError::InvalidInput("invalid amount".into()))?;

    let input = TransferInput {
        from_account_number: from_account_number.clone(),
        to_account_number: to_account_number.clone(),
        amount,
        idempotency_key: idempotency_key.clone(),
    };

    let result = handler.transfer_use_case.execute(input).await?;

    let response_key = idempotency_key
        .clone()
        .unwrap_or_else(IdempotencyService::generate_key);

    if let Some(key) = idempotency_key {
        let _ = handler
            .idempotency_service
            .save_response(&key, &response_key)
            .await;
    }

    let transaction = TransactionResponse {
        id: result.transaction.id().to_string(),
        kind: "transfer".to_string(),
        amount: result.transaction.amount().as_u64(),
        from_account_number: Some(body.from_account_number.clone()),
        to_account_number: Some(body.to_account_number.clone()),
        created_at: result
            .transaction
            .created_at()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| result.transaction.created_at().to_string()),
    };

    Ok((
        StatusCode::OK,
        Json(TransferResponse {
            transaction: Some(transaction),
            idempotency_key: response_key,
        }),
    ))
}

pub async fn get_account(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Path(account_number): Path<String>,
) -> Result<(StatusCode, Json<AccountResponse>), HttpError> {
    let user_id = extract_user_id(&headers)?;

    let account_number = AccountNumber::new(&account_number)
        .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;

    let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
    handler
        .verify_account_ownership(&account_number, &user_id, has_internal_key)
        .await?;

    let input = GetAccountInput { account_number };
    let account = handler.get_account_use_case.execute(input).await?;

    Ok((StatusCode::OK, Json(domain_to_http_account(&account))))
}

#[derive(Debug, Deserialize)]
pub struct GetAccountsQuery {
    user_id: Option<String>,
    org_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GetAccountsResponse {
    accounts: Vec<AccountResponse>,
}

pub async fn get_accounts(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(query): axum::extract::Query<GetAccountsQuery>,
) -> Result<(StatusCode, Json<GetAccountsResponse>), HttpError> {
    let owner = OwnerExtractor::extract_from_header_and_params(
        headers.get("X-USER-ID").and_then(|v| v.to_str().ok()),
        query.user_id,
        query.org_id,
        headers.get("X-Internal-Api-Key").is_some(),
    )
    .map_err(HttpError::from)?;

    let input = GetAccountsInput { owner };
    let accounts = handler.get_accounts_use_case.execute(input).await?;

    let response = GetAccountsResponse {
        accounts: accounts.iter().map(domain_to_http_account).collect(),
    };

    Ok((StatusCode::OK, Json(response)))
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionsQuery {
    page: Option<u32>,
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct GetTransactionsResponse {
    transactions: Vec<TransactionResponse>,
    total_count: u64,
    page: u32,
    page_size: u32,
    has_more: bool,
}

pub async fn get_transactions(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Path(account_number): Path<String>,
    axum::extract::Query(query): axum::extract::Query<GetTransactionsQuery>,
) -> Result<(StatusCode, Json<GetTransactionsResponse>), HttpError> {
    let user_id = extract_user_id(&headers)?;

    let account_number = AccountNumber::new(&account_number)
        .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;

    let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
    handler
        .verify_account_ownership(&account_number, &user_id, has_internal_key)
        .await?;

    let page = query.page.unwrap_or(0);
    let page_size = query.page_size.unwrap_or(20).min(100);

    let input = GetTransactionsInput {
        account_number,
        page,
        page_size,
    };
    let result = handler.get_transactions_use_case.execute(input).await?;

    let transactions: Vec<TransactionResponse> = result
        .transactions
        .iter()
        .map(|t| TransactionResponse {
            id: t.transaction.id().to_string(),
            kind: match t.transaction.kind() {
                TransactionKind::Deposit => "deposit".to_string(),
                TransactionKind::Withdraw => "withdraw".to_string(),
                TransactionKind::Transfer => "transfer".to_string(),
            },
            amount: t.transaction.amount().as_u64(),
            from_account_number: t.from_account_number.clone(),
            to_account_number: t.to_account_number.clone(),
            created_at: t
                .transaction
                .created_at()
                .format(&time::format_description::well_known::Rfc3339)
                .expect("RFC3339 formatting should never fail for valid timestamps"),
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(GetTransactionsResponse {
            transactions,
            total_count: result.total_count,
            page: result.page,
            page_size: result.page_size,
            has_more: result.has_more,
        }),
    ))
}
