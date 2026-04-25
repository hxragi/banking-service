use std::str::FromStr;

use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{
        PaginatedTransactions, TransactionRepository, TransactionRepositoryError,
        TransactionWithAccounts,
    },
    domain::{amount::Amount, transaction::Transaction, transaction_kind::TransactionKind},
    infrastructure::database::error::classify,
};

#[derive(Clone)]
pub struct SqlxTransactionRepository {
    pool: PgPool,
}

impl SqlxTransactionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn row_to_transaction(
        id: Uuid,
        kind_str: String,
        amount: i64,
        from_account_id: Option<Uuid>,
        to_account_id: Option<Uuid>,
        created_at: OffsetDateTime,
    ) -> Result<Transaction, TransactionRepositoryError> {
        let kind = TransactionKind::from_str(&kind_str).map_err(|_| {
            TransactionRepositoryError::TransactionFailed(format!(
                "invalid transaction kind in database: {}",
                kind_str
            ))
        })?;
        let amount = u64::try_from(amount).map_err(|_| {
            TransactionRepositoryError::TransactionFailed("amount overflow in database row".into())
        })?;
        let amount = Amount::new(amount).map_err(|_| {
            TransactionRepositoryError::TransactionFailed("invalid amount in database row".into())
        })?;

        match kind {
            TransactionKind::Deposit => {
                if from_account_id.is_some() || to_account_id.is_none() {
                    return Err(TransactionRepositoryError::TransactionFailed(
                        "deposit row has invalid account references".into(),
                    ));
                }
                Ok(Transaction::deposit(
                    id,
                    amount,
                    to_account_id.unwrap(),
                    created_at,
                ))
            }
            TransactionKind::Withdraw => {
                if from_account_id.is_none() || to_account_id.is_some() {
                    return Err(TransactionRepositoryError::TransactionFailed(
                        "withdraw row has invalid account references".into(),
                    ));
                }
                Ok(Transaction::withdraw(
                    id,
                    amount,
                    from_account_id.unwrap(),
                    created_at,
                ))
            }
            TransactionKind::Transfer => {
                if from_account_id.is_none() || to_account_id.is_none() {
                    return Err(TransactionRepositoryError::TransactionFailed(
                        "transfer row has invalid account references".into(),
                    ));
                }
                Transaction::transfer(
                    id,
                    amount,
                    from_account_id.unwrap(),
                    to_account_id.unwrap(),
                    created_at,
                )
                .map_err(|e| TransactionRepositoryError::TransactionFailed(e.to_string()))
            }
        }
    }
}

#[async_trait::async_trait]
impl TransactionRepository for SqlxTransactionRepository {
    async fn find_by_account_id_paginated(
        &self,
        account_id: Uuid,
        page: u32,
        page_size: u32,
    ) -> Result<PaginatedTransactions, TransactionRepositoryError> {
        let offset = ((page.saturating_sub(1)) as i64) * (page_size as i64);
        let limit = page_size as i64;

        let total_count = self.count_by_account_id(account_id).await?;

        let query = r#"
            SELECT
                t.id, t.kind::text, t.amount, t.from_account_id, t.to_account_id, t.created_at,
                from_acc.number as from_account_number,
                to_acc.number as to_account_number
            FROM transactions t
            LEFT JOIN accounts from_acc ON t.from_account_id = from_acc.id
            LEFT JOIN accounts to_acc ON t.to_account_id = to_acc.id
            WHERE t.from_account_id = $1 OR t.to_account_id = $1
            ORDER BY t.created_at DESC
            LIMIT $2 OFFSET $3
        "#;

        let rows: Vec<(Uuid, String, i64, Option<Uuid>, Option<Uuid>, OffsetDateTime, Option<String>, Option<String>)> = sqlx::query_as(query)
            .bind(account_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                let context = classify(&e, "find_transactions_paginated");
                tracing::error!(err = %context, account_id = %account_id, "failed to find transactions");
                TransactionRepositoryError::TransactionFailed(context)
            })?;

        let transactions: Vec<TransactionWithAccounts> = rows
            .into_iter()
            .map(
                |(
                    id,
                    kind_str,
                    amount,
                    from_account_id,
                    to_account_id,
                    created_at,
                    from_account_number,
                    to_account_number,
                )| {
                    let transaction = Self::row_to_transaction(
                        id,
                        kind_str,
                        amount,
                        from_account_id,
                        to_account_id,
                        created_at,
                    )?;
                    Ok(TransactionWithAccounts {
                        transaction,
                        from_account_number,
                        to_account_number,
                    })
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = (offset + limit) < (total_count as i64);

        Ok(PaginatedTransactions {
            transactions,
            total_count,
            page,
            page_size,
            has_more,
        })
    }

    async fn count_by_account_id(
        &self,
        account_id: Uuid,
    ) -> Result<u64, TransactionRepositoryError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM transactions WHERE from_account_id = $1 OR to_account_id = $1"
        )
        .bind(account_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            let context = classify(&e, "count_transactions");
            tracing::error!(err = %context, account_id = %account_id, "failed to count transactions");
            TransactionRepositoryError::TransactionFailed(context)
        })?;

        Ok(count as u64)
    }
}
