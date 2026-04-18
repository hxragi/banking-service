use sqlx::{PgPool, Row, postgres::PgRow};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{AccountRepository, AccountRepositoryError},
    domain::{
        account::Account, account_number::AccountNumber, balance::Balance, errors::DomainError,
        org_id::OrgId, owner::Owner, user_id::UserId,
    },
    infrastructure::database::error::classify,
};

pub struct SqlxAccountRepository {
    pool: PgPool,
}

#[derive(Debug, Error)]
pub enum SqlxAccountRepositoryError {
    #[error("account conversion error: {0}")]
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

    fn row_to_account(&self, row: &PgRow) -> Result<Account, SqlxAccountRepositoryError> {
        let id: Uuid = row
            .try_get("id")
            .map_err(|_| SqlxAccountRepositoryError::InvalidBalanceData)?;
        let number: String = row
            .try_get("number")
            .map_err(|_| SqlxAccountRepositoryError::InvalidBalanceData)?;
        let user_id: Option<String> = row
            .try_get("user_id")
            .map_err(|_| SqlxAccountRepositoryError::FailedIdentifyOwner)?;
        let org_id: Option<String> = row
            .try_get("org_id")
            .map_err(|_| SqlxAccountRepositoryError::FailedIdentifyOwner)?;
        let balance: i64 = row
            .try_get("balance")
            .map_err(|_| SqlxAccountRepositoryError::InvalidBalanceData)?;
        let created_at: OffsetDateTime = row
            .try_get("created_at")
            .map_err(|_| SqlxAccountRepositoryError::InvalidBalanceData)?;

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
        let row = sqlx::query(
            r#"
            SELECT id, number, user_id, org_id, balance, created_at
            FROM accounts
            WHERE number = $1
            "#
        )
        .bind(number.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            let context = classify(&e, "find_by_number");
            tracing::warn!(err = %context, account_number = %number, "failed to find account by number");
            AccountRepositoryError::OperationFailed {
                operation: "find_by_number".to_string(),
                reason: context,
            }
        })?;

        match row {
            Some(row) => self.row_to_account(&row)
                .map_err(|e| {
                    tracing::error!(err = %e, account_number = %number, "failed to convert account row");
                    AccountRepositoryError::OperationFailed {
                        operation: "row_conversion".to_string(),
                        reason: e.to_string(),
                    }
                })
                .map(Some),
            None => Ok(None),
        }
    }

    async fn count_by_owner(&self, owner: &Owner) -> Result<u64, AccountRepositoryError> {
        let count: i64 = match owner {
            Owner::User(user_id) => {
                sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM accounts WHERE user_id = $1"#)
                    .bind(user_id.as_str())
                    .fetch_one(&self.pool)
                    .await
            }
            Owner::Org(org_id) => {
                sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM accounts WHERE org_id = $1"#)
                    .bind(org_id.as_str())
                    .fetch_one(&self.pool)
                    .await
            }
        }
        .map_err(|e| {
            let context = classify(&e, "count_by_owner");
            tracing::warn!(err = %context, owner = ?owner, "failed to count accounts by owner");
            AccountRepositoryError::OperationFailed {
                operation: "count_by_owner".to_string(),
                reason: context,
            }
        })?;

        Ok(count as u64)
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
            i64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "balance_conversion".to_string(),
                reason: "balance value out of range".to_string(),
            })?;

        sqlx::query(
            r#"
            INSERT INTO accounts (id, number, user_id, org_id, balance, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(id)
        .bind(number)
        .bind(user_id)
        .bind(org_id)
        .bind(balance)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            let context = classify(&e, "create");
            tracing::error!(err = %context, account_number = %number, "failed to create account");

            if let sqlx::Error::Database(db_err) = &e
                && (db_err.message().contains("duplicate key")
                    || db_err.message().contains("unique constraint"))
            {
                return AccountRepositoryError::UniqueConstraintViolation(format!(
                    "account number {} already exists",
                    number
                ));
            }
            AccountRepositoryError::OperationFailed {
                operation: "create".to_string(),
                reason: context,
            }
        })?;

        Ok(())
    }

    async fn find_by_owner(&self, owner: &Owner) -> Result<Vec<Account>, AccountRepositoryError> {
        let rows = match owner {
            Owner::User(user_id) => {
                sqlx::query(
                    r#"
                    SELECT id, number, user_id, org_id, balance, created_at
                    FROM accounts
                    WHERE user_id = $1
                    "#,
                )
                .bind(user_id.as_str())
                .fetch_all(&self.pool)
                .await
            }
            Owner::Org(org_id) => {
                sqlx::query(
                    r#"
                    SELECT id, number, user_id, org_id, balance, created_at
                    FROM accounts
                    WHERE org_id = $1
                    "#,
                )
                .bind(org_id.as_str())
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| {
            let context = classify(&e, "find_by_owner");
            tracing::error!(err = %context, owner = ?owner, "failed to find accounts by owner");
            AccountRepositoryError::OperationFailed {
                operation: "find_by_owner".to_string(),
                reason: context,
            }
        })?;

        let accounts: Vec<Account> = rows
            .iter()
            .map(|row| self.row_to_account(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                tracing::error!(err = %e, "failed to convert account row");
                AccountRepositoryError::OperationFailed {
                    operation: "row_conversion".to_string(),
                    reason: e.to_string(),
                }
            })?;

        Ok(accounts)
    }
}
