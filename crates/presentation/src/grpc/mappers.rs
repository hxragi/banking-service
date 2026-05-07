use domain::{account::Account, owner::Owner};

use crate::grpc::bank_service::bank::account;

pub fn domain_to_proto_account(account: &Account) -> crate::grpc::bank_service::bank::Account {
    let owner = match account.owner() {
        Owner::User(user_id) => Some(account::Owner::UserId(user_id.as_str().to_owned())),
        Owner::Org(org_id) => Some(account::Owner::OrgId(org_id.as_str().to_owned())),
    };

    crate::grpc::bank_service::bank::Account {
        id: account.id().to_string(),
        number: account.number().as_str().to_owned(),
        owner,
        balance: account.balance().as_u64(),
        created_at: account.created_at().to_string(),
        tier: 0,
    }
}
