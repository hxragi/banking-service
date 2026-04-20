use sqlx::PgPool;

use crate::{
    application::ports::{AccountRepository, AccountRepositoryError},
    domain::{account::Account, account_number::AccountNumber, owner::Owner},
    infrastructure::database::{error::classify, row_mapping::row_to_account},
};

pub struct SqlxAccountRepository {
    pool: PgPool,
}

impl SqlxAccountRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
            Some(row) => {
                let account = row_to_account(&row).map_err(|e| {
                    tracing::error!(err = %e, account_number = %number, "failed to convert account row");
                    e
                })?;
                Ok(Some(account))
            }
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
            .map(|row| row_to_account(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                tracing::error!(err = %e, "failed to convert account row");
                e
            })?;

        Ok(accounts)
    }
}
