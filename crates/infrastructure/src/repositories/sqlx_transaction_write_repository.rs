use crate::database::{error::from_sqlx, transaction::DbTransaction};
use application::ports::{TransactionRepositoryError, TransactionWriteRepository};
use domain::transaction::Transaction;

pub struct SqlxTransactionWriteRepository;

#[async_trait::async_trait]
impl TransactionWriteRepository<DbTransaction> for SqlxTransactionWriteRepository {
    async fn create(
        &self,
        tx: &mut DbTransaction,
        transaction: &Transaction,
    ) -> Result<(), TransactionRepositoryError> {
        let id = transaction.id();
        let kind = transaction.kind().as_str();
        let amount = transaction.amount().as_u64() as i64;
        let from_account_id = transaction.source_account_id();
        let to_account_id = transaction.destination_account_id();

        let account_id = from_account_id.or(to_account_id).ok_or_else(|| {
            TransactionRepositoryError::TransactionFailed(
                "transaction has no source or destination account".into(),
            )
        })?;

        sqlx::query(
            "INSERT INTO transactions
             (id, kind, amount, from_account_id, to_account_id, account_id)
             VALUES ($1, $2::transaction_kind, $3, $4, $5, $6)",
        )
        .bind(id)
        .bind(kind)
        .bind(amount)
        .bind(from_account_id)
        .bind(to_account_id)
        .bind(account_id)
        .execute(tx.as_sqlx())
        .await
        .map_err(|e| {
            let err = from_sqlx(&e).with_operation("create transaction");
            tracing::error!(err = %err, "failed to create transaction record");
            TransactionRepositoryError::from(err)
        })?;

        Ok(())
    }
}
