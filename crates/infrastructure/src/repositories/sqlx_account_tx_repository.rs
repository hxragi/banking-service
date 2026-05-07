use crate::database::{error::from_sqlx, row_mapping::row_to_account, transaction::DbTransaction};
use application::ports::{AccountRepositoryError, AccountTxRepository};
use domain::account::Account;

use uuid::Uuid;

pub struct SqlxAccountTxRepository;

#[async_trait::async_trait]
impl AccountTxRepository<DbTransaction> for SqlxAccountTxRepository {
    async fn find_by_number_for_update(
        &self,
        tx: &mut DbTransaction,
        account_number: &str,
    ) -> Result<Option<Account>, AccountRepositoryError> {
        let row = sqlx::query(
            "SELECT id, number, user_id, org_id, balance, created_at 
             FROM accounts WHERE number = $1 FOR UPDATE",
        )
        .bind(account_number)
        .fetch_optional(tx.as_sqlx())
        .await
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("find by number for update");
            AccountRepositoryError::from(err)
        })?;

        match row {
            Some(row) => Ok(Some(row_to_account(&row)?)),
            None => Ok(None),
        }
    }

    async fn update_balance(
        &self,
        tx: &mut DbTransaction,
        account_id: Uuid,
        balance: u64,
    ) -> Result<(), AccountRepositoryError> {
        let balance_i64 =
            i64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "update_balance".to_string(),
                reason: "balance overflow".to_string(),
            })?;

        sqlx::query("UPDATE accounts SET balance = $1 WHERE id = $2")
            .bind(balance_i64)
            .bind(account_id)
            .execute(tx.as_sqlx())
            .await
            .map_err(|e| {
                let err = from_sqlx(&e).with_operation("update_balance");
                tracing::error!(error = %err, "failed to update account balance");
                AccountRepositoryError::from(err)
            })?;

        Ok(())
    }

    async fn lock_for_update_by_numbers(
        &self,
        tx: &mut DbTransaction,
        first_number: &str,
        second_number: &str,
    ) -> Result<Vec<Account>, AccountRepositoryError> {
        let rows = sqlx::query(
            "SELECT id, number, user_id, org_id, balance, created_at 
             FROM accounts WHERE number IN ($1, $2) ORDER BY number FOR UPDATE",
        )
        .bind(first_number)
        .bind(second_number)
        .fetch_all(tx.as_sqlx())
        .await
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("lock for update by numbers");
            AccountRepositoryError::from(err)
        })?;

        rows.iter().map(row_to_account).collect()
    }
}
