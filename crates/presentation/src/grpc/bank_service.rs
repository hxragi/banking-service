use super::{interceptor::InternalRequestExt, mappers::domain_to_proto_account};
use crate::{
    grpc::bank_service::bank::{
        ChangeTierRequest, ChangeTierResponse, CreateAccountRequest, CreateAccountResponse,
        DepositRequest, DepositResponse, GetAccountRequest, GetAccountResponse, GetAccountsRequest,
        GetAccountsResponse, GetTransactionsRequest, GetTransactionsResponse, TransferRequest,
        TransferResponse, WithdrawRequest, WithdrawResponse, bank_service_server::BankService,
    },
    http::extractors::owner_extractor::{OwnerExtractionError, OwnerExtractor},
};
use application::{
    change_tier::{ChangeTierInput, ChangeTierUseCase},
    create_account::{CreateAccountInput, CreateAccountUseCase},
    deposit::{DepositInput, DepositPort},
    get_account::{GetAccountInput, GetAccountUseCase},
    get_accounts::{GetAccountsInput, GetAccountsUseCase},
    get_transactions::{GetTransactionsInput, GetTransactionsUseCase},
    ports::{MetricsPort, OperationError, OperationErrorKind},
    transfer::{TransferInput, TransferPort},
    withdraw::{WithdrawInput, WithdrawPort},
};
use domain::{
    account_number::AccountNumber, amount::Amount, owner::Owner, tier::Tier,
    transaction_kind::TransactionKind,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

tonic_include_protos::include_protos!();

fn map_extraction_error(err: OwnerExtractionError) -> Status {
    match err {
        OwnerExtractionError::MissingOwner => {
            Status::invalid_argument("owner must be either user_id or org_id")
        }
        OwnerExtractionError::InvalidUserId => Status::invalid_argument("invalid user_id"),
        OwnerExtractionError::InvalidOrgId => Status::invalid_argument("invalid org_id"),
        OwnerExtractionError::BothUserAndOrgProvided => {
            Status::invalid_argument("cannot specify both user_id and org_id")
        }
        OwnerExtractionError::OrgNotAllowed => {
            Status::permission_denied("org operations can only be done via internal API")
        }
    }
}

#[derive(Clone)]
pub struct BankGrpcService {
    create_account_use_case: Arc<CreateAccountUseCase>,
    get_account_use_case: Arc<GetAccountUseCase>,
    get_accounts_use_case: Arc<GetAccountsUseCase>,
    deposit_use_case: Arc<dyn DepositPort>,
    withdraw_use_case: Arc<dyn WithdrawPort>,
    transfer_use_case: Arc<dyn TransferPort>,
    get_transactions_use_case: Arc<GetTransactionsUseCase>,
    change_tier_use_case: Arc<ChangeTierUseCase>,
    metrics: Arc<dyn MetricsPort>,
}

impl BankGrpcService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        create_account_use_case: Arc<CreateAccountUseCase>,
        get_account_use_case: Arc<GetAccountUseCase>,
        get_accounts_use_case: Arc<GetAccountsUseCase>,
        deposit_use_case: Arc<dyn DepositPort>,
        withdraw_use_case: Arc<dyn WithdrawPort>,
        transfer_use_case: Arc<dyn TransferPort>,
        get_transactions_use_case: Arc<GetTransactionsUseCase>,
        change_tier_use_case: Arc<ChangeTierUseCase>,
        metrics: Arc<dyn MetricsPort>,
    ) -> Self {
        Self {
            create_account_use_case,
            get_account_use_case,
            get_accounts_use_case,
            deposit_use_case,
            withdraw_use_case,
            transfer_use_case,
            get_transactions_use_case,
            change_tier_use_case,
            metrics,
        }
    }
}

fn map_operation_error(err: OperationError) -> Status {
    if let OperationError::RepositoryError { operation, reason } = &err {
        tracing::error!(operation = ?operation, reason = %reason, "repository error");
    }
    if let OperationError::ConnectionError(msg) = &err {
        tracing::error!(message = %msg, "connection error");
    }

    let msg = err.to_string();
    match err.kind() {
        OperationErrorKind::NotFound => Status::not_found(msg),
        OperationErrorKind::InvalidInput => Status::invalid_argument(msg),
        OperationErrorKind::InsufficientFunds
        | OperationErrorKind::TierLimitExceeded
        | OperationErrorKind::TierDowngradeNotAllowed => Status::failed_precondition(msg),
        OperationErrorKind::UniqueConstraintViolation | OperationErrorKind::IdempotencyError => {
            Status::already_exists(msg)
        }
        OperationErrorKind::LockTimeout
        | OperationErrorKind::Deadlock
        | OperationErrorKind::SerializationFailure => Status::aborted(msg),
        OperationErrorKind::Unavailable | OperationErrorKind::ConnectionError => {
            Status::unavailable(msg)
        }
        OperationErrorKind::RepositoryError => Status::internal("internal server error"),
    }
}

fn parse_tier(value: u32) -> Result<Tier, Status> {
    match value {
        0 | 1 => Ok(Tier::Basic),
        2 => Ok(Tier::Premium),
        3 => Ok(Tier::Elite),
        _ => Err(Status::invalid_argument(
            "tier must be 1 (Basic), 2 (Premium), or 3 (Elite)",
        )),
    }
}

fn parse_owner(
    owner: Option<bank::create_account_request::Owner>,
    is_internal: bool,
) -> Result<Owner, Status> {
    let (user_id, org_id) = match owner {
        Some(bank::create_account_request::Owner::UserId(s)) => (Some(s), None),
        Some(bank::create_account_request::Owner::OrgId(s)) => (None, Some(s)),
        None => (None, None),
    };

    OwnerExtractor::extract_from_params(user_id, org_id, is_internal).map_err(map_extraction_error)
}

fn parse_get_accounts_owner(
    owner: Option<bank::get_accounts_request::Owner>,
    is_internal: bool,
) -> Result<Owner, Status> {
    let (user_id, org_id) = match owner {
        Some(bank::get_accounts_request::Owner::UserId(s)) => (Some(s), None),
        Some(bank::get_accounts_request::Owner::OrgId(s)) => (None, Some(s)),
        None => (None, None),
    };

    OwnerExtractor::extract_from_params(user_id, org_id, is_internal).map_err(map_extraction_error)
}

fn parse_change_tier_owner(
    owner: Option<bank::change_tier_request::Owner>,
    is_internal: bool,
) -> Result<Owner, Status> {
    let (user_id, org_id) = match owner {
        Some(bank::change_tier_request::Owner::UserId(s)) => (Some(s), None),
        Some(bank::change_tier_request::Owner::OrgId(s)) => (None, Some(s)),
        None => (None, None),
    };

    OwnerExtractor::extract_from_params(user_id, org_id, is_internal).map_err(map_extraction_error)
}

#[tonic::async_trait]
impl BankService for BankGrpcService {
    async fn create_account(
        &self,
        request: Request<CreateAccountRequest>,
    ) -> Result<Response<CreateAccountResponse>, Status> {
        let is_internal = request.is_internal();
        let req = request.into_inner();
        let owner = parse_owner(req.owner, is_internal)?;

        let input = CreateAccountInput { owner };
        let result = self.create_account_use_case.execute(input).await;

        match result {
            Ok(account) => {
                self.metrics
                    .increment_operation("create_account", "success");
                Ok(Response::new(CreateAccountResponse {
                    account: Some(domain_to_proto_account(&account)),
                }))
            }
            Err(err) => {
                self.metrics
                    .record_error("create_account_failed", "create_account");
                Err(map_operation_error(err))
            }
        }
    }

    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> Result<Response<GetAccountResponse>, Status> {
        let req = request.into_inner();
        let account_number = AccountNumber::new(&req.account_number)
            .map_err(|_| Status::invalid_argument("invalid account number"))?;

        let input = GetAccountInput { account_number };
        let result = self.get_account_use_case.execute(input).await;

        match result {
            Ok(account) => {
                self.metrics.increment_operation("get_account", "success");
                Ok(Response::new(GetAccountResponse {
                    account: Some(domain_to_proto_account(&account)),
                }))
            }
            Err(err) => {
                self.metrics
                    .record_error("get_account_failed", "get_account");
                Err(map_operation_error(err))
            }
        }
    }

    async fn get_accounts(
        &self,
        request: Request<GetAccountsRequest>,
    ) -> Result<Response<GetAccountsResponse>, Status> {
        let is_internal = request.is_internal();
        let req = request.into_inner();
        let owner = parse_get_accounts_owner(req.owner, is_internal)?;

        let input = GetAccountsInput { owner };
        let result = self.get_accounts_use_case.execute(input).await;

        match result {
            Ok(accounts) => {
                self.metrics.increment_operation("get_accounts", "success");
                let proto_accounts: Vec<bank::Account> =
                    accounts.iter().map(domain_to_proto_account).collect();
                Ok(Response::new(GetAccountsResponse {
                    accounts: proto_accounts,
                }))
            }
            Err(err) => {
                self.metrics
                    .record_error("get_accounts_failed", "get_accounts");
                Err(map_operation_error(err))
            }
        }
    }

    async fn transfer(
        &self,
        request: Request<TransferRequest>,
    ) -> Result<Response<TransferResponse>, Status> {
        let req = request.into_inner();

        let idempotency_key =
            (!req.idempotency_key.is_empty()).then(|| req.idempotency_key.clone());

        let from_account_number = AccountNumber::new(&req.from_account_number)
            .map_err(|_| Status::invalid_argument("invalid from account number"))?;
        let to_account_number = AccountNumber::new(&req.to_account_number)
            .map_err(|_| Status::invalid_argument("invalid to account number"))?;
        let amount =
            Amount::new(req.amount).map_err(|_| Status::invalid_argument("invalid amount"))?;

        let input = TransferInput {
            from_account_number,
            to_account_number,
            amount,
            idempotency_key: idempotency_key.clone(),
        };

        let result = self.transfer_use_case.execute(input).await;

        match result {
            Ok(tx_result) => {
                self.metrics.increment_operation("transfer", "success");
                let transaction = bank::Transaction {
                    id: tx_result.transaction.id().to_string(),
                    kind: "transfer".to_string(),
                    amount: tx_result.transaction.amount().as_u64(),
                    from_account_number: Some(req.from_account_number.clone()),
                    to_account_number: Some(req.to_account_number.clone()),
                    created_at: tx_result.transaction.created_at().to_string(),
                };

                let response_key = idempotency_key.unwrap_or_else(|| Uuid::new_v4().to_string());

                Ok(Response::new(TransferResponse {
                    transaction: Some(transaction),
                    idempotency_key: response_key,
                }))
            }
            Err(err) => {
                self.metrics.record_error("transfer_failed", "transfer");
                Err(map_operation_error(err))
            }
        }
    }

    async fn deposit(
        &self,
        request: Request<DepositRequest>,
    ) -> Result<Response<DepositResponse>, Status> {
        let req = request.into_inner();

        let idempotency_key =
            (!req.idempotency_key.is_empty()).then(|| req.idempotency_key.clone());

        let account_number = AccountNumber::new(&req.account_number)
            .map_err(|_| Status::invalid_argument("invalid account number"))?;
        let amount =
            Amount::new(req.amount).map_err(|_| Status::invalid_argument("invalid amount"))?;

        let input = DepositInput {
            account_number,
            amount,
            idempotency_key: idempotency_key.clone(),
        };
        let result = self.deposit_use_case.execute(input).await;

        match result {
            Ok(deposit_result) => {
                let response_key = idempotency_key.unwrap_or_else(|| Uuid::new_v4().to_string());

                self.metrics.increment_operation("deposit", "success");
                Ok(Response::new(DepositResponse {
                    account: Some(domain_to_proto_account(&deposit_result)),
                    idempotency_key: response_key,
                }))
            }
            Err(err) => {
                self.metrics.record_error("deposit_failed", "deposit");
                Err(map_operation_error(err))
            }
        }
    }

    async fn withdraw(
        &self,
        request: Request<WithdrawRequest>,
    ) -> Result<Response<WithdrawResponse>, Status> {
        let req = request.into_inner();

        let idempotency_key =
            (!req.idempotency_key.is_empty()).then(|| req.idempotency_key.clone());

        let account_number = AccountNumber::new(&req.account_number)
            .map_err(|_| Status::invalid_argument("invalid account number"))?;
        let amount =
            Amount::new(req.amount).map_err(|_| Status::invalid_argument("invalid amount"))?;

        let input = WithdrawInput {
            account_number,
            amount,
            idempotency_key: idempotency_key.clone(),
        };
        let result = self.withdraw_use_case.execute(input).await;

        match result {
            Ok(withdraw_result) => {
                let response_key = idempotency_key.unwrap_or_else(|| Uuid::new_v4().to_string());

                self.metrics.increment_operation("withdraw", "success");
                Ok(Response::new(WithdrawResponse {
                    account: Some(domain_to_proto_account(&withdraw_result)),
                    idempotency_key: response_key,
                }))
            }
            Err(err) => {
                self.metrics.record_error("withdraw_failed", "withdraw");
                Err(map_operation_error(err))
            }
        }
    }

    async fn get_transactions(
        &self,
        request: Request<GetTransactionsRequest>,
    ) -> Result<Response<GetTransactionsResponse>, Status> {
        let req = request.into_inner();
        let account_number = AccountNumber::new(&req.account_number)
            .map_err(|_| Status::invalid_argument("invalid account number"))?;

        let page = req.page.max(1);
        let page_size = req.page_size.clamp(1, 100);

        let input = GetTransactionsInput {
            account_number,
            page,
            page_size,
        };

        let result = self.get_transactions_use_case.execute(input).await;

        match result {
            Ok(transactions) => {
                self.metrics
                    .increment_operation("get_transactions", "success");
                let proto_transactions: Vec<bank::Transaction> = transactions
                    .transactions
                    .iter()
                    .map(|t| bank::Transaction {
                        id: t.transaction.id().to_string(),
                        kind: match t.transaction.kind() {
                            TransactionKind::Deposit => "deposit".to_string(),
                            TransactionKind::Withdraw => "withdraw".to_string(),
                            TransactionKind::Transfer => "transfer".to_string(),
                        },
                        amount: t.transaction.amount().as_u64(),
                        from_account_number: t.from_account_number.clone(),
                        to_account_number: t.to_account_number.clone(),
                        created_at: t.transaction.created_at().to_string(),
                    })
                    .collect();

                Ok(Response::new(GetTransactionsResponse {
                    transactions: proto_transactions,
                    total_count: transactions.total_count,
                    page: transactions.page,
                    page_size: transactions.page_size,
                    has_more: transactions.has_more,
                }))
            }
            Err(err) => {
                self.metrics
                    .record_error("get_transactions_failed", "get_transactions");
                Err(map_operation_error(err))
            }
        }
    }

    async fn change_tier(
        &self,
        request: Request<ChangeTierRequest>,
    ) -> Result<Response<ChangeTierResponse>, Status> {
        let is_internal = request.is_internal();
        let req = request.into_inner();
        let owner = parse_change_tier_owner(req.owner, is_internal)?;
        let new_tier =
            parse_tier(req.new_tier).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let input = ChangeTierInput { owner, new_tier };

        let result = self.change_tier_use_case.execute(input).await;

        match result {
            Ok(tier) => {
                self.metrics.increment_operation("change_tier", "success");
                Ok(Response::new(ChangeTierResponse {
                    tier: tier.as_i32() as u32,
                }))
            }
            Err(err) => {
                self.metrics
                    .record_error("change_tier_failed", "change_tier");
                Err(map_operation_error(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::bank_service::bank::get_accounts_request;
    use tonic::metadata::MetadataValue;

    fn create_test_request_with_org() -> Request<GetAccountsRequest> {
        let mut req = Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::OrgId("org-123".to_string())),
        });
        req.metadata_mut().insert(
            "x-internal-api-key",
            MetadataValue::from_static("internal-secret"),
        );
        req
    }

    fn create_test_request_with_org_no_header() -> Request<GetAccountsRequest> {
        Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::OrgId("org-123".to_string())),
        })
    }

    fn create_test_request_with_user() -> Request<GetAccountsRequest> {
        Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::UserId("user-123".to_string())),
        })
    }

    fn mark_request_internal<T>(req: &mut Request<T>) {
        req.metadata_mut()
            .insert("x-is-internal-request", MetadataValue::from_static("true"));
    }

    #[test]
    fn test_is_internal_request_with_header() {
        let mut req = create_test_request_with_org();
        mark_request_internal(&mut req);
        assert!(req.is_internal());
    }

    #[test]
    fn test_is_internal_request_without_header() {
        let req = create_test_request_with_org_no_header();
        assert!(!req.is_internal());
    }

    #[test]
    fn test_parse_get_accounts_owner_user_no_header() {
        let req = create_test_request_with_user();
        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_ok());

        match result.unwrap() {
            Owner::User(user_id) => assert_eq!(user_id.as_str(), "user-123"),
            _ => panic!("Expected User owner"),
        }
    }

    #[test]
    fn test_parse_get_accounts_owner_org_with_header() {
        let mut req = create_test_request_with_org();
        mark_request_internal(&mut req);
        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_ok());

        match result.unwrap() {
            Owner::Org(org_id) => assert_eq!(org_id.as_str(), "org-123"),
            _ => panic!("Expected Org owner"),
        }
    }

    #[test]
    fn test_parse_get_accounts_owner_org_without_header() {
        let req = create_test_request_with_org_no_header();
        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(
            status
                .message()
                .contains("org operations can only be done via internal API")
        );
    }

    #[test]
    fn test_parse_get_accounts_owner_empty_user_id() {
        let req = Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::UserId("".to_string())),
        });
        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("invalid user_id"));
    }

    #[test]
    fn test_parse_get_accounts_owner_whitespace_only_user_id() {
        let req = Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::UserId("   ".to_string())),
        });
        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("invalid user_id"));
    }

    #[test]
    fn test_parse_get_accounts_owner_empty_org_id() {
        let mut req = Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::OrgId("".to_string())),
        });
        req.metadata_mut()
            .insert("x-is-internal-request", MetadataValue::from_static("true"));

        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("invalid org_id"));
    }

    #[test]
    fn test_parse_get_accounts_owner_whitespace_only_org_id() {
        let mut req = Request::new(GetAccountsRequest {
            owner: Some(get_accounts_request::Owner::OrgId("   ".to_string())),
        });
        req.metadata_mut()
            .insert("x-is-internal-request", MetadataValue::from_static("true"));

        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("invalid org_id"));
    }

    #[test]
    fn test_parse_get_accounts_owner_none() {
        let req = Request::new(GetAccountsRequest { owner: None });
        let is_internal = req.is_internal();
        let req_inner = req.into_inner();

        let result = parse_get_accounts_owner(req_inner.owner, is_internal);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status
                .message()
                .contains("owner must be either user_id or org_id")
        );
    }
}
