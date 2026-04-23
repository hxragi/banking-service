use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use uuid::Uuid;

use crate::application::{
    create_account::{CreateAccountInput, CreateAccountUseCase},
    deposit::{DepositInput, DepositPort},
    get_account::{GetAccountInput, GetAccountUseCase},
    get_accounts::{GetAccountsInput, GetAccountsUseCase},
    get_transactions::{GetTransactionsInput, GetTransactionsUseCase},
    transfer::{TransferInput, TransferPort},
    withdraw::{WithdrawInput, WithdrawPort},
};
use crate::domain::account_number::AccountNumber;
use crate::domain::amount::Amount;
use crate::domain::owner::Owner;
use crate::domain::transaction_kind::TransactionKind;
use crate::domain::user_id::UserId;
use crate::infrastructure::services::owner_extractor::OwnerExtractor;

use super::super::dto::requests::{
    CreateAccountBody, DepositBody, GetAccountsQuery, GetTransactionsQuery, TransferBody,
    WithdrawBody,
};
use super::super::dto::responses::{
    AccountResponse, DepositResponse, GetAccountsResponse, GetTransactionsResponse,
    TransactionResponse, TransferResponse, WithdrawResponse,
};
use super::super::errors::HttpError;
use super::super::mappers::domain_to_http_account;

#[derive(Clone)]
pub struct AccountHttpHandler {
    create_account_use_case: Arc<CreateAccountUseCase>,
    get_account_use_case: Arc<GetAccountUseCase>,
    get_accounts_use_case: Arc<GetAccountsUseCase>,
    deposit_use_case: Arc<dyn DepositPort>,
    withdraw_use_case: Arc<dyn WithdrawPort>,
    transfer_use_case: Arc<dyn TransferPort>,
    get_transactions_use_case: Arc<GetTransactionsUseCase>,
}

impl AccountHttpHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        create_account_use_case: Arc<CreateAccountUseCase>,
        get_account_use_case: Arc<GetAccountUseCase>,
        get_accounts_use_case: Arc<GetAccountsUseCase>,
        deposit_use_case: Arc<dyn DepositPort>,
        withdraw_use_case: Arc<dyn WithdrawPort>,
        transfer_use_case: Arc<dyn TransferPort>,
        get_transactions_use_case: Arc<GetTransactionsUseCase>,
    ) -> Self {
        Self {
            create_account_use_case,
            get_account_use_case,
            get_accounts_use_case,
            deposit_use_case,
            withdraw_use_case,
            transfer_use_case,
            get_transactions_use_case,
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
}

fn extract_user_id(headers: &axum::http::HeaderMap) -> Result<UserId, HttpError> {
    let user_id_str = headers
        .get("X-USER-ID")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| HttpError::InvalidInput("missing X-USER-ID header".into()))?;

    UserId::new(user_id_str).map_err(|_| HttpError::InvalidInput("invalid X-USER-ID header".into()))
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
        idempotency_key: body.idempotency_key.clone(),
    };
    let account = handler.deposit_use_case.execute(input).await?;

    let response_key = body
        .idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

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
        idempotency_key: body.idempotency_key.clone(),
    };
    let account = handler.withdraw_use_case.execute(input).await?;

    let response_key = body
        .idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

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
        idempotency_key: body.idempotency_key.clone(),
    };

    let result = handler.transfer_use_case.execute(input).await?;

    let response_key = body
        .idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

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

pub async fn get_accounts(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Query(query): Query<GetAccountsQuery>,
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

pub async fn get_transactions(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Path(account_number): Path<String>,
    Query(query): Query<GetTransactionsQuery>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_user_id_from_valid_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("X-USER-ID", "user-123".parse().unwrap());
        let result = extract_user_id(&headers);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), "user-123");
    }

    #[test]
    fn extract_user_id_missing_header_returns_error() {
        let headers = axum::http::HeaderMap::new();
        let result = extract_user_id(&headers);
        assert!(result.is_err());
    }

    #[test]
    fn extract_user_id_empty_header_returns_error() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("X-USER-ID", "".parse().unwrap());
        let result = extract_user_id(&headers);
        assert!(result.is_err());
    }
}
