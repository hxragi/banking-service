use serde::Serialize;

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

#[derive(Debug, Serialize)]
pub struct DepositResponse {
    pub account: AccountResponse,
    pub idempotency_key: String,
}

#[derive(Debug, Serialize)]
pub struct WithdrawResponse {
    pub account: AccountResponse,
    pub idempotency_key: String,
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

#[derive(Debug, Serialize)]
pub struct GetAccountsResponse {
    pub accounts: Vec<AccountResponse>,
}

#[derive(Debug, Serialize)]
pub struct GetTransactionsResponse {
    pub transactions: Vec<TransactionResponse>,
    pub total_count: u64,
    pub page: u32,
    pub page_size: u32,
    pub has_more: bool,
}
