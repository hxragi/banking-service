use domain::account::Account;
use domain::owner::Owner;

use super::dto::responses::{AccountResponse, OwnerResponse};

pub fn domain_to_http_account(account: &Account) -> AccountResponse {
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
