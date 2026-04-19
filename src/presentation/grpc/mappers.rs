use crate::domain::account::Account;
use crate::domain::owner::Owner;

use super::bank::account;

pub fn domain_to_proto_account(account: &Account) -> super::bank::Account {
    let owner = match account.owner() {
        Owner::User(user_id) => Some(account::Owner::UserId(user_id.as_str().to_owned())),
        Owner::Org(org_id) => Some(account::Owner::OrgId(org_id.as_str().to_owned())),
    };

    super::bank::Account {
        id: account.id().to_string(),
        number: account.number().as_str().to_owned(),
        owner,
        balance: account.balance().as_u64(),
        created_at: account.created_at().to_string(),
        tier: 0,
    }
}
