use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CreateAccountBody {}

#[derive(Debug, Deserialize)]
pub struct DepositBody {
    pub amount: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WithdrawBody {
    pub amount: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransferBody {
    pub from_account_number: String,
    pub to_account_number: String,
    pub amount: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetAccountsQuery {
    pub user_id: Option<String>,
    pub org_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionsQuery {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
}
