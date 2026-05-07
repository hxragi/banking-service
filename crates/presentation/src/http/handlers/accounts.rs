use std::sync::Arc;

use super::super::{
    dto::{
        requests::{
            CreateAccountBody, DepositBody, GetAccountsQuery, GetTransactionsQuery, TransferBody,
            WithdrawBody,
        },
        responses::{
            AccountResponse, DepositResponse, GetAccountsResponse, GetTransactionsResponse,
            TransactionResponse, TransferResponse, WithdrawResponse,
        },
    },
    errors::HttpError,
    mappers::domain_to_http_account,
};
use crate::http::extractors::owner_extractor::OwnerExtractor;
use application::{
    create_account::{CreateAccountInput, CreateAccountUseCase},
    deposit::{DepositInput, DepositPort},
    get_account::{GetAccountInput, GetAccountPort},
    get_accounts::{GetAccountsInput, GetAccountsPort},
    get_transactions::{GetTransactionsInput, GetTransactionsUseCase},
    transfer::{TransferInput, TransferPort},
    withdraw::{WithdrawInput, WithdrawPort},
};
use domain::{
    account_number::AccountNumber, amount::Amount, owner::Owner, transaction_kind::TransactionKind,
    user_id::UserId,
};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
}

#[derive(Clone)]
pub struct JwtDecoder {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtDecoder {
    pub fn new(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        validation.required_spec_claims.remove("exp");
        Self {
            decoding_key: DecodingKey::from_secret(secret),
            validation,
        }
    }

    pub fn decode(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        decode::<Claims>(token, &self.decoding_key, &self.validation).map(|data| data.claims)
    }
}

#[derive(Clone)]
pub struct AccountHttpHandler {
    create_account_use_case: Arc<CreateAccountUseCase>,
    get_account_use_case: Arc<dyn GetAccountPort>,
    get_accounts_use_case: Arc<dyn GetAccountsPort>,
    deposit_use_case: Arc<dyn DepositPort>,
    withdraw_use_case: Arc<dyn WithdrawPort>,
    transfer_use_case: Arc<dyn TransferPort>,
    get_transactions_use_case: Arc<GetTransactionsUseCase>,
    jwt_decoder: Arc<JwtDecoder>,
}

impl AccountHttpHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        create_account_use_case: Arc<CreateAccountUseCase>,
        get_account_use_case: Arc<dyn GetAccountPort>,
        get_accounts_use_case: Arc<dyn GetAccountsPort>,
        deposit_use_case: Arc<dyn DepositPort>,
        withdraw_use_case: Arc<dyn WithdrawPort>,
        transfer_use_case: Arc<dyn TransferPort>,
        get_transactions_use_case: Arc<GetTransactionsUseCase>,
        jwt_decoder: Arc<JwtDecoder>,
    ) -> Self {
        Self {
            create_account_use_case,
            get_account_use_case,
            get_accounts_use_case,
            deposit_use_case,
            withdraw_use_case,
            transfer_use_case,
            get_transactions_use_case,
            jwt_decoder,
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

fn extract_user_id_from_jwt(
    headers: &axum::http::HeaderMap,
    jwt_decoder: &JwtDecoder,
) -> Result<UserId, HttpError> {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| HttpError::InvalidInput("missing authorization header".into()))?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or_else(|| HttpError::InvalidInput("invalid authorization header format".into()))?;

    let claims = jwt_decoder
        .decode(token)
        .map_err(|e| HttpError::InvalidInput(format!("invalid token: {e}")))?;

    UserId::new(&claims.sub).map_err(|_| HttpError::InvalidInput("invalid user id in token".into()))
}

pub async fn create_account(
    State(handler): State<AccountHttpHandler>,
    headers: axum::http::HeaderMap,
    Json(_body): Json<Option<CreateAccountBody>>,
) -> Result<(StatusCode, Json<AccountResponse>), HttpError> {
    let user_id = extract_user_id_from_jwt(&headers, &handler.jwt_decoder)?;

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
    let user_id = extract_user_id_from_jwt(&headers, &handler.jwt_decoder)?;

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
    let user_id = extract_user_id_from_jwt(&headers, &handler.jwt_decoder)?;

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
    let user_id = extract_user_id_from_jwt(&headers, &handler.jwt_decoder)?;

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
    let user_id = extract_user_id_from_jwt(&headers, &handler.jwt_decoder)?;

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
    let user_id = extract_user_id_from_jwt(&headers, &handler.jwt_decoder)?;

    let account_number = AccountNumber::new(&account_number)
        .map_err(|_| HttpError::InvalidInput("invalid account number".into()))?;

    let has_internal_key = headers.get("X-Internal-Api-Key").is_some();
    handler
        .verify_account_ownership(&account_number, &user_id, has_internal_key)
        .await?;

    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).clamp(1, 100);

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
                .unwrap_or_else(|_| t.transaction.created_at().to_string()),
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
    use std::sync::Once;

    use jsonwebtoken::{EncodingKey, Header, encode};

    use super::*;

    const TEST_SECRET: &[u8] = b"test-secret-key";
    static INIT_CRYPTO: Once = Once::new();

    fn init_crypto() {
        INIT_CRYPTO.call_once(|| {
            jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER
                .install_default()
                .expect("crypto provider should install once")
        });
    }

    fn test_decoder() -> JwtDecoder {
        init_crypto();
        JwtDecoder::new(TEST_SECRET)
    }

    fn test_token(sub: &str) -> String {
        init_crypto();
        let claims = Claims {
            sub: sub.to_string(),
        };

        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(TEST_SECRET),
        )
        .expect("test token encoding should not fail")
    }

    #[test]
    fn jwt_decoder_decodes_valid_token() {
        let token = test_token("user-123");
        let decoder = test_decoder();
        let claims = decoder.decode(&token).expect("valid token should decode");
        assert_eq!(claims.sub, "user-123");
    }

    #[test]
    fn jwt_decoder_fails_on_invalid_signature() {
        let claims = Claims {
            sub: "user-123".to_string(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"wrong-secret"),
        )
        .expect("test token encoding should not fail");
        let decoder = test_decoder();
        assert!(decoder.decode(&token).is_err());
    }

    #[test]
    fn extract_user_id_from_valid_bearer_token() {
        let token = test_token("user-123");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
        let result = extract_user_id_from_jwt(&headers, &test_decoder());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), "user-123");
    }

    #[test]
    fn extract_user_id_missing_authorization_header() {
        let headers = axum::http::HeaderMap::new();
        let result = extract_user_id_from_jwt(&headers, &test_decoder());
        assert!(result.is_err());
    }

    #[test]
    fn extract_user_id_invalid_header_format() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Basic dXNlcjoxMjM=".parse().unwrap(),
        );
        let result = extract_user_id_from_jwt(&headers, &test_decoder());
        assert!(result.is_err());
    }

    #[test]
    fn extract_user_id_invalid_token() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer invalid.token".parse().unwrap(),
        );
        let result = extract_user_id_from_jwt(&headers, &test_decoder());
        assert!(result.is_err());
    }

    #[test]
    fn extract_user_id_invalid_user_id_in_claims() {
        let token = test_token("     ");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
        let result = extract_user_id_from_jwt(&headers, &test_decoder());
        assert!(result.is_err());
    }
}
