use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::application::create_account::{
    CreateAccountError, CreateAccountInput, CreateAccountUseCase,
};
use crate::domain::account::Account;
use crate::domain::org_id::OrgId;
use crate::domain::owner::Owner;
use crate::domain::tier::Tier;
use crate::domain::user_id::UserId;
use crate::infrastructure::grpc::bank::bank_service_server::BankService;
use crate::infrastructure::grpc::bank::{CreateAccountRequest, CreateAccountResponse};

tonic_include_protos::include_protos!();

pub struct BankGrpcService {
    create_account_use_case: Arc<CreateAccountUseCase>,
}

impl BankGrpcService {
    pub fn new(create_account_use_case: Arc<CreateAccountUseCase>) -> Self {
        Self {
            create_account_use_case,
        }
    }
}

fn parse_tier(value: u32) -> Result<Tier, Status> {
    match value {
        1 => Ok(Tier::Basic),
        2 => Ok(Tier::Premium),
        3 => Ok(Tier::Elite),
        _ => Err(Status::invalid_argument(
            "tier must be 1 (Basic), 2 (Premium), or 3 (Elite)",
        )),
    }
}

fn parse_owner(owner: Option<bank::create_account_request::Owner>) -> Result<Owner, Status> {
    match owner {
        Some(bank::create_account_request::Owner::UserId(s)) => UserId::new(&s)
            .map(Owner::User)
            .map_err(|_| Status::invalid_argument("invalid user_id")),
        Some(bank::create_account_request::Owner::OrgId(s)) => OrgId::new(&s)
            .map(Owner::Org)
            .map_err(|_| Status::invalid_argument("invalid org_id")),
        None => Err(Status::invalid_argument(
            "owner must be either user_id or org_id",
        )),
    }
}

fn map_create_account_error(err: CreateAccountError) -> Status {
    match err {
        CreateAccountError::TierLimitExceeded => {
            Status::failed_precondition("account limit exceeded for this tier")
        }
        CreateAccountError::AccountRepository(_) => Status::internal("internal server error"),
        CreateAccountError::AccountNumberGenerator(_) => Status::internal("internal server error"),
    }
}

fn domain_to_proto_account(account: &Account) -> bank::Account {
    let owner = match account.owner() {
        Owner::User(user_id) => Some(bank::account::Owner::UserId(user_id.as_str().to_owned())),
        Owner::Org(org_id) => Some(bank::account::Owner::OrgId(org_id.as_str().to_owned())),
    };

    bank::Account {
        id: account.id().to_string(),
        number: account.number().as_str().to_owned(),
        owner,
        balance: account.balance().as_u64(),
        created_at: account.created_at().to_string(),
    }
}

#[tonic::async_trait]
impl BankService for BankGrpcService {
    async fn create_account(
        &self,
        request: Request<CreateAccountRequest>,
    ) -> Result<Response<CreateAccountResponse>, Status> {
        let req = request.into_inner();

        let owner = parse_owner(req.owner)?;
        let tier = parse_tier(req.tier)?;

        let input = CreateAccountInput { owner, tier };
        let account = self
            .create_account_use_case
            .execute(input)
            .await
            .map_err(map_create_account_error)?;

        let account_proto = domain_to_proto_account(&account);

        Ok(Response::new(bank::CreateAccountResponse {
            account: Some(account_proto),
        }))
    }

    async fn get_account(
        &self,
        request: Request<bank::GetAccountRequest>,
    ) -> Result<Response<bank::GetAccountResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn get_accounts(
        &self,
        request: Request<bank::GetAccountsRequest>,
    ) -> Result<Response<bank::GetAccountsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn transfer(
        &self,
        request: Request<bank::TransferRequest>,
    ) -> Result<Response<bank::TransferResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn deposit(
        &self,
        request: Request<bank::DepositRequest>,
    ) -> Result<Response<bank::DepositResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn withdraw(
        &self,
        request: Request<bank::WithdrawRequest>,
    ) -> Result<Response<bank::WithdrawResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn get_transactions(
        &self,
        request: Request<bank::GetTransactionsRequest>,
    ) -> Result<Response<bank::GetTransactionsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }
}
