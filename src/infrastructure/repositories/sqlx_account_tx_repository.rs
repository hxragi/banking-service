use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use crate::{
    application::ports::{AccountTxRepository, AccountRepositoryError},
    domain::{
        account::Account, account_number::AccountNumber, balance::Balance, org_id::OrgId,
        owner::Owner, user_id::UserId,
    },
    infrastructure::database::error::{classify, map_sqlx_to_account_error},
};

fn row_to_account(row: &sqlx::postgres::PgRow) -> Result<Account, AccountRepositoryError> {
    let id: Uuid = row.try_get("id").map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "missing id".to_string(),
    })?;
    let number: String = row.try_get("number").map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "missing number".to_string(),
    })?;
    let user_id: Option<String> = row.try_get("user_id").map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "missing user_id".to_string(),
    })?;
    let org_id: Option<String> = row.try_get("org_id").map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "missing org_id".to_string(),
    })?;
    let balance: i64 = row.try_get("balance").map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "missing balance".to_string(),
    })?;
    let created_at: time::OffsetDateTime = row.try_get("created_at").map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "missing created_at".to_string(),
    })?;

    let owner = match (user_id, org_id) {
        (Some(uid), None) => Owner::User(UserId::new(&uid).map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "invalid user_id".to_string(),
        })?),
        (None, Some(oid)) => Owner::Org(OrgId::new(&oid).map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "invalid org_id".to_string(),
        })?),
        _ => return Err(AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "ambiguous owner".to_string(),
        }),
    };

    let balance_u64 = u64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed {
        operation: "row_conversion".to_string(),
        reason: "negative balance".to_string(),
    })?;

    Ok(Account::new(
        id,
        AccountNumber::new(&number).map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "invalid account number".to_string(),
        })?,
        owner,
        Balance::new(balance_u64),
        created_at,
    ))
}

pub struct SqlxAccountTxRepository;

#[async_trait::async_trait]
impl AccountTxRepository<Transaction<'static, Postgres>> for SqlxAccountTxRepository {
    async fn find_by_number_for_update(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        account_number: &str,
    ) -> Result<Option<Account>, AccountRepositoryError> {
        let row = sqlx::query(
            "SELECT id, number, user_id, org_id, balance, created_at FROM accounts WHERE number = $1 FOR UPDATE"
        )
        .bind(account_number)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_to_account_error)?;

        match row {
            Some(row) => Ok(Some(row_to_account(&row)?)),
            None => Ok(None),
        }
    }

    async fn update_balance(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        account_id: Uuid,
        balance: u64,
    ) -> Result<(), AccountRepositoryError> {
        let balance_i64 = i64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "update_balance".to_string(),
            reason: "balance overflow".to_string(),
        })?;

        sqlx::query("UPDATE accounts SET balance = $1 WHERE id = $2")
            .bind(balance_i64)
            .bind(account_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                let context = classify(&e, "update_balance");
                tracing::error!(error = %context, "failed to update account balance");
                map_sqlx_to_account_error(e)
            })?;

        Ok(())
    }

    async fn lock_for_update_by_numbers(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        first_number: &str,
        second_number: &str,
    ) -> Result<Vec<Account>, AccountRepositoryError> {
        let rows = sqlx::query(
            "SELECT id, number, user_id, org_id, balance, created_at FROM accounts WHERE number IN ($1, $2) ORDER BY number FOR UPDATE"
        )
        .bind(first_number)
        .bind(second_number)
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_to_account_error)?;

        rows.iter()
            .map(|row| row_to_account(row))
            .collect()
    }
}
