use sqlx::PgPool;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    account::Account, account_number::AccountNumber, errors::DomainError, org_id::OrgId, owner::Owner, user_id::{self, UserId}
};

pub struct SqlxAccountRepository {
    pool: PgPool,
}

pub struct AccountRow {
    id: Uuid,
    number: String,
    user_id: Option<String>,
    org_id: Option<String>,
    balance: i64,
    created_at: OffsetDateTime,
}

#[derive(Debug, Error)]
pub enum SqlxAccountRepositoryError {
    #[error("account conversation error")]
    AccountConversationError,
    #[error("failed to identify owner")]
    FailedIdentifyOwner
}

impl SqlxAccountRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl TryFrom<AccountRow> for Account {
    type Error =
        crate::infrastructure::repositories::sqlx_account_repository::SqlxAccountRepositoryError;

    fn try_from(row: AccountRow) -> Result<Account, Self::Error> {
        let AccountRow {
            id,
            number,
            user_id,
            org_id,
            balance,
            created_at,
        } = row;

        let account_number = AccountNumber::new(&number.as_str())?;

        let owner = match (user_id, org_id) {
            (Some(user_id), None) => Owner::User(UserId::new(&user_id)?),
            (None, Some(org_id)) => Owner::Org(OrgId::new(&org_id)?),
            _ => 
        }
    }
}
