use std::sync::Arc;

use crate::{
    ports::{BalanceCachePort, OperationError},
    transaction_manager::{
        TransactionManagerPort, TransactionOperation, WithdrawInput as TxWithdrawInput,
    },
};
use domain::{account::Account, account_number::AccountNumber, amount::Amount};

#[derive(Debug)]
pub struct WithdrawInput {
    pub account_number: AccountNumber,
    pub amount: Amount,
    pub idempotency_key: Option<String>,
}

pub struct WithdrawUseCase {
    transaction_manager: Arc<dyn TransactionManagerPort>,
}

#[async_trait::async_trait]
pub trait WithdrawPort: Send + Sync {
    async fn execute(&self, input: WithdrawInput) -> Result<Account, OperationError>;
}

impl WithdrawUseCase {
    pub fn new(transaction_manager: Arc<dyn TransactionManagerPort>) -> Self {
        Self {
            transaction_manager,
        }
    }
}

#[async_trait::async_trait]
impl WithdrawPort for WithdrawUseCase {
    #[tracing::instrument(
        skip(self),
        fields(
            account_number = %input.account_number,
            amount = %input.amount,
            idempotency_key = ?input.idempotency_key
        )
    )]
    async fn execute(&self, input: WithdrawInput) -> Result<Account, OperationError> {
        let account_number_str = input.account_number.to_string();
        let WithdrawInput {
            account_number,
            amount,
            idempotency_key,
        } = input;

        let operation = TransactionOperation::Withdraw(TxWithdrawInput {
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

                tracing::info!(account_number = %account.number(), new_balance = %account.balance(), "withdraw completed");
                Ok(account)
            }
            Err(e) => {
                tracing::warn!(account_number = %account_number_str, error = %e, "withdraw failed");
                Err(OperationError::from(e))
            }
        }
    }
}

pub struct CachingWithdrawUseCase {
    inner: Arc<dyn WithdrawPort>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl CachingWithdrawUseCase {
    pub fn new(inner: Arc<dyn WithdrawPort>, balance_cache: Arc<dyn BalanceCachePort>) -> Self {
        Self {
            inner,
            balance_cache,
        }
    }
}

#[async_trait::async_trait]
impl WithdrawPort for CachingWithdrawUseCase {
    async fn execute(&self, input: WithdrawInput) -> Result<Account, OperationError> {
        let account = self.inner.execute(input).await?;

        if let Err(e) = self
            .balance_cache
            .set(account.id(), account.balance().as_u64())
            .await
        {
            tracing::warn!(error = %e, "failed to set balance in cache")
        };

        Ok(account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::AccountRepositoryError;
    use crate::transaction_manager::TransactionError;

    #[test]
    fn withdraw_error_from_transaction_error_account_not_found() {
        let tx_err = TransactionError::AccountNotFound;
        let withdraw_err: OperationError = tx_err.into();
        assert!(matches!(withdraw_err, OperationError::NotFound { .. }));
    }

    #[test]
    fn withdraw_error_from_transaction_error_insufficient_funds() {
        let tx_err = TransactionError::InsufficientFunds;
        let withdraw_err: OperationError = tx_err.into();
        assert!(matches!(withdraw_err, OperationError::InsufficientFunds));
    }

    #[test]
    fn withdraw_error_from_transaction_error_account_unavailable() {
        let tx_err = TransactionError::AccountUnavailable;
        let withdraw_err: OperationError = tx_err.into();
        assert!(matches!(withdraw_err, OperationError::Unavailable { .. }));
    }

    #[test]
    fn withdraw_error_from_transaction_error_lock_timeout() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::LockTimeout);
        let withdraw_err: OperationError = tx_err.into();
        assert!(matches!(withdraw_err, OperationError::LockTimeout));
    }

    #[test]
    fn withdraw_error_from_transaction_error_connection_error() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::ConnectionError(
            "db down".to_string(),
        ));
        let withdraw_err: OperationError = tx_err.into();
        assert!(matches!(withdraw_err, OperationError::ConnectionError(_)));
    }
}
