use crate::database::{error::from_sqlx, row_mapping::row_to_account};
use application::ports::{AccountRepository, AccountRepositoryError};
use domain::{account::Account, account_number::AccountNumber, owner::Owner};

use sqlx::PgPool;

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
            "#,
        )
        .bind(number.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("find by number");
            tracing::warn!(err = %e, account_number = %number, "failed to find account by number");
            AccountRepositoryError::from(err)
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
            let err = from_sqlx(&e).with_operation("count_by_owner");
            tracing::warn!(err = %e, owner = ?owner, "failed to count accounts by owner");
            AccountRepositoryError::from(err)
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
            let err = from_sqlx(&e).with_operation("create");
            tracing::error!(err = %err, account_number = %number, "failed to create account");
            AccountRepositoryError::from(err)
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
            let err = from_sqlx(&e).with_operation("find_by_owner");
            tracing::error!(err = %err, owner = ?owner, "failed to find accounts by owner");
            AccountRepositoryError::from(err)
        })?;

        let accounts: Vec<Account> = rows
            .iter()
            .map(row_to_account)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                tracing::error!(err = %e, "failed to convert account row");
                e
            })?;

        Ok(accounts)
    }

    async fn create_within_limit(
        &self,
        account: &Account,
        limit: Option<u64>,
    ) -> Result<(), AccountRepositoryError> {
        let owner = account.owner();

        let mut tx = self.pool.begin().await.map_err(|e| {
            let err = from_sqlx(&e).with_operation("create_within_limit");
            AccountRepositoryError::from(err)
        })?;

        let (owner_type, owner_id) = match owner {
            Owner::User(user_id) => ("user", user_id.as_str()),
            Owner::Org(org_id) => ("org", org_id.as_str()),
        };

        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
            .bind(owner_type)
            .bind(owner_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                let err = from_sqlx(&e).with_operation("create_within_limit");
                AccountRepositoryError::from(err)
            })?;

        let count: i64 = match owner {
            Owner::User(user_id) => {
                sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM accounts WHERE user_id = $1"#)
                    .bind(user_id.as_str())
                    .fetch_one(&mut *tx)
                    .await
            }
            Owner::Org(org_id) => {
                sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM accounts WHERE org_id = $1"#)
                    .bind(org_id.as_str())
                    .fetch_one(&mut *tx)
                    .await
            }
        }
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("count_by_owner");
            AccountRepositoryError::from(err)
        })?;

        if let Some(limit) = limit
            && (count as u64) >= limit
        {
            return Err(AccountRepositoryError::LimitExceeded);
        }

        let id = account.id();
        let number = account.number().as_str();
        let balance = account.balance().as_u64();
        let created_at = account.created_at();

        let (user_id, org_id) = match owner {
            Owner::User(user_id) => (Some(user_id.as_str()), None),
            Owner::Org(org_id) => (None, Some(org_id.as_str())),
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
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("create");
            tracing::error!(err = %err, account_number = %number, "failed to create account");
            AccountRepositoryError::from(err)
        })?;

        tx.commit().await.map_err(|e| {
            let err = from_sqlx(&e).with_operation("create_within_limit");
            AccountRepositoryError::from(err)
        })?;

        Ok(())
    }
}
