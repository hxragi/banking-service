use std::sync::Arc;

use crate::{
    application::{
        ports::{BalanceCachePort, OperationError, Transaction as TxTrait, TransactionPort},
        transaction_manager::{
            DepositInput as TxDepositInput, FinancialTransactionManager, TransactionOperation,
        },
    },
    domain::{account::Account, account_number::AccountNumber, amount::Amount},
};

#[derive(Debug)]
pub struct DepositInput {
    pub account_number: AccountNumber,
    pub amount: Amount,
    pub idempotency_key: Option<String>,
}

#[async_trait::async_trait]
pub trait DepositPort: Send + Sync {
    async fn execute(&self, input: DepositInput) -> Result<Account, OperationError>;
}

pub struct DepositUseCase<M, Tx> {
    transaction_manager: Arc<FinancialTransactionManager<M, Tx>>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

#[async_trait::async_trait]
impl<M, Tx> DepositPort for DepositUseCase<M, Tx>
where
    M: TransactionPort<Transaction = Tx>,
    Tx: TxTrait + Send,
{
    #[tracing::instrument(
        skip(self),
        fields(
            account_number = %input.account_number,
            amount = %input.amount,
            idempotency_key = ?input.idempotency_key
        )
    )]
    async fn execute(&self, input: DepositInput) -> Result<Account, OperationError> {
        let account_number_str = input.account_number.to_string();
        let DepositInput {
            account_number,
            amount,
            idempotency_key,
        } = input;

        let operation = TransactionOperation::Deposit(TxDepositInput {
            account_number,
            amount,
        });

        let result = self
            .transaction_manager
            .execute(operation, idempotency_key)
            .await;

        match result {
            Ok(output) => {
                let account = output
                    .into_account()
                    .ok_or_else(|| OperationError::Unavailable {
                        reason: "invalid operation result".to_string(),
                    })?;

                self.balance_cache
                    .set(account.id(), account.balance().as_u64())
                    .await;

                tracing::info!(account_number = %account.number(), new_balance = %account.balance(), "deposit completed");
                Ok(account)
            }
            Err(e) => {
                tracing::warn!(account_number = %account_number_str, error = %e, "deposit failed");
                Err(OperationError::from(e))
            }
        }
    }
}

impl<M, Tx> DepositUseCase<M, Tx>
where
    M: TransactionPort<Transaction = Tx>,
    Tx: TxTrait + Send,
{
    pub fn new(
        transaction_manager: Arc<FinancialTransactionManager<M, Tx>>,
        balance_cache: Arc<dyn BalanceCachePort>,
    ) -> Self {
        Self {
            transaction_manager,
            balance_cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::AccountRepositoryError;
    use crate::application::transaction_manager::TransactionError;

    #[test]
    fn deposit_error_from_transaction_error_account_not_found() {
        let tx_err = TransactionError::AccountNotFound;
        let deposit_err: OperationError = tx_err.into();
        assert!(matches!(deposit_err, OperationError::NotFound { .. }));
    }

    #[test]
    fn deposit_error_from_transaction_error_account_unavailable() {
        let tx_err = TransactionError::AccountUnavailable;
        let deposit_err: OperationError = tx_err.into();
        assert!(matches!(deposit_err, OperationError::Unavailable { .. }));
    }

    #[test]
    fn deposit_error_from_transaction_error_repository() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::OperationFailed {
            operation: "test".to_string(),
            reason: "test failure".to_string(),
        });
        let deposit_err: OperationError = tx_err.into();
        assert!(matches!(
            deposit_err,
            OperationError::RepositoryError { .. }
        ));
    }

    #[test]
    fn deposit_error_from_transaction_error_lock_timeout() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::LockTimeout);
        let deposit_err: OperationError = tx_err.into();
        assert!(matches!(deposit_err, OperationError::LockTimeout));
    }

    #[test]
    fn deposit_error_from_transaction_error_unique_violation() {
        let tx_err = TransactionError::AccountRepository(
            AccountRepositoryError::UniqueConstraintViolation("duplicate".to_string()),
        );
        let deposit_err: OperationError = tx_err.into();
        assert!(matches!(
            deposit_err,
            OperationError::UniqueConstraintViolation(_)
        ));
    }

    #[test]
    fn deposit_error_from_transaction_error_connection_error() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::ConnectionError(
            "db down".to_string(),
        ));
        let deposit_err: OperationError = tx_err.into();
        assert!(matches!(deposit_err, OperationError::ConnectionError(_)));
    }
}
