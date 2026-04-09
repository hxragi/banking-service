use sqlx::{PgPool, prelude::FromRow};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{AccountRepository, AccountRepositoryError},
    domain::{
        account::Account, account_number::AccountNumber, balance::Balance, errors::DomainError,
        org_id::OrgId, owner::Owner, user_id::UserId,
    },
};

pub struct SqlxAccountRepository {
    pool: PgPool,
}

#[derive(Debug, FromRow)]
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
    #[error("account conversation error: {0}")]
    AccountConversionError(#[from] DomainError),
    #[error("failed to identify owner")]
    FailedIdentifyOwner,
    #[error("invalid balance data")]
    InvalidBalanceData,
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
}

impl SqlxAccountRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_number(
        &self,
        number: &AccountNumber,
    ) -> Result<Option<Account>, SqlxAccountRepositoryError> {
        let row: Option<AccountRow> = sqlx::query_as!(
            AccountRow,
            r#"
            SELECT id, number, user_id, org_id, balance, created_at
            FROM accounts
            WHERE number = $1
            "#,
            number.as_str()
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => {
                let account = Account::try_from(r)?;
                Ok(Some(account))
            }
            None => Ok(None),
        }
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

        let owner = match (user_id, org_id) {
            (Some(user_id), None) => Owner::User(UserId::new(&user_id)?),
            (None, Some(org_id)) => Owner::Org(OrgId::new(&org_id)?),
            _ => return Err(SqlxAccountRepositoryError::FailedIdentifyOwner),
        };

        let balance =
            u64::try_from(balance).map_err(|_| SqlxAccountRepositoryError::InvalidBalanceData)?;

        let account = Account::new(
            id,
            AccountNumber::new(&number)?,
            owner,
            Balance::new(balance),
            created_at,
        );

        Ok(account)
    }
}

#[async_trait::async_trait]
impl AccountRepository for SqlxAccountRepository {
    async fn find_by_number(
        &self,
        number: &AccountNumber,
    ) -> Result<Option<Account>, AccountRepositoryError> {
        self.find_by_number(number).await.map_err(|e| {
            tracing::error!(err = %e, "failed to find account by number");
            AccountRepositoryError::OperationFailed
        })
    }

    async fn count_by_owner(&self, owner: &Owner) -> Result<u64, AccountRepositoryError> {
        let count = match owner {
            Owner::User(user_id) => {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) FROM accounts WHERE user_id = $1"#,
                    user_id.as_str()
                )
                .fetch_one(&self.pool)
                .await
            }
            Owner::Org(org_id) => {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) FROM accounts WHERE org_id = $1"#,
                    org_id.as_str()
                )
                .fetch_one(&self.pool)
                .await
            }
        }
        .map_err(|e| {
            tracing::error!(err = %e, "failed to count accounts by owner");
            AccountRepositoryError::OperationFailed
        })?;

        Ok(count.unwrap_or(0) as u64)
    }

    async fn create(&self, account: &Account) -> Result<(), AccountRepositoryError> {
        let id = account.id();
        let number = account.number().as_str();
        let owner = account.owner();
        let balance = account.balance().as_u64();
        let created_at = account.created_at();

        let (user_id, org_id) = match owner {
            Owner::User(user_id) => (Some(user_id.as_str().to_owned()), None),
            Owner::Org(org_id) => (None, Some(org_id.as_str().to_owned())),
        };

        let balance =
            i64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed)?;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, number, user_id, org_id, balance, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            id,
            number,
            user_id,
            org_id,
            balance,
            created_at,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(err = %e, "failed to create account");
            AccountRepositoryError::OperationFailed
        })?;

        Ok(())
    }

    async fn update(&self, account: &Account) -> Result<(), AccountRepositoryError> {
        let id = account.id();
        let balance = account.balance().as_u64();

        let balance =
            i64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed)?;

        sqlx::query!(
            r#"UPDATE accounts SET balance = $1 WHERE id = $2"#,
            balance,
            id,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(err = %e, "failed to update account");
            AccountRepositoryError::OperationFailed
        })?;

        Ok(())
    }

    async fn find_by_owner(&self, owner: &Owner) -> Result<Vec<Account>, AccountRepositoryError> {
        let rows = match owner {
            Owner::User(user_id) => {
                sqlx::query_as!(
                    AccountRow,
                    r#"
                    SELECT id, number, user_id, org_id, balance, created_at
                    FROM accounts
                    WHERE user_id = $1
                    "#,
                    user_id.as_str()
                )
                .fetch_all(&self.pool)
                .await
            }
            Owner::Org(org_id) => {
                sqlx::query_as!(
                    AccountRow,
                    r#"
                    SELECT id, number, user_id, org_id, balance, created_at
                    FROM accounts
                    WHERE org_id = $1
                    "#,
                    org_id.as_str()
                )
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| {
            tracing::error!(err = %e, "failed to find accounts by owner");
            AccountRepositoryError::OperationFailed
        })?;

        let accounts: Vec<Account> = rows
            .into_iter()
            .map(Account::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                tracing::error!(err = %e, "failed to convert account row");
                AccountRepositoryError::OperationFailed
            })?;

        Ok(accounts)
    }
}
