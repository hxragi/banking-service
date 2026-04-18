use std::sync::Arc;

use crate::{
    application::{
        ports::{BalanceCachePort, OperationError},
        transaction_manager::{
            TransactionManager, TransactionOperation, TransferInput as TxTransferInput,
        },
    },
    domain::{account_number::AccountNumber, amount::Amount, transaction::Transaction},
};

#[derive(Debug)]
pub struct TransferInput {
    pub from_account_number: AccountNumber,
    pub to_account_number: AccountNumber,
    pub amount: Amount,
    pub idempotency_key: Option<String>,
}

pub struct TransferOutput {
    pub transaction: Transaction,
}

pub struct TransferUseCase {
    transaction_manager: Arc<TransactionManager>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl TransferUseCase {
    pub fn new(
        transaction_manager: Arc<TransactionManager>,
        balance_cache: Arc<dyn BalanceCachePort>,
    ) -> Self {
        Self {
            transaction_manager,
            balance_cache,
        }
    }

    #[tracing::instrument(
        skip(self),
        fields(
            from_account_number = %input.from_account_number,
            to_account_number = %input.to_account_number,
            amount = %input.amount,
            idempotency_key = ?input.idempotency_key
        )
    )]
    pub async fn execute(&self, input: TransferInput) -> Result<TransferOutput, OperationError> {
        let TransferInput {
            from_account_number,
            to_account_number,
            amount,
            idempotency_key,
        } = input;

        let operation = TransactionOperation::Transfer(TxTransferInput {
            from_account_number,
            to_account_number,
            amount,
        });

        let result = self
            .transaction_manager
            .execute(operation, idempotency_key)
            .await;

        match result {
            Ok(output) => {
                let transfer_result = output.clone().into_transfer_result().ok_or_else(|| {
                    OperationError::Unavailable {
                        reason: "invalid operation result".to_string(),
                    }
                })?;
                self.balance_cache
                    .invalidate(&transfer_result.from_account.id())
                    .await;
                self.balance_cache
                    .invalidate(&transfer_result.to_account.id())
                    .await;
                tracing::info!(
                    from = %transfer_result.from_account.number(),
                    to = %transfer_result.to_account.number(),
                    from_balance = %transfer_result.from_account.balance(),
                    to_balance = %transfer_result.to_account.balance(),
                    "transfer completed"
                );
                Ok(TransferOutput {
                    transaction: output.transaction().clone(),
                })
            }
            Err(e) => {
                tracing::warn!(error = %e, "transfer failed");
                Err(OperationError::from(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::AccountRepositoryError;
    use crate::application::transaction_manager::TransactionError;

    #[test]
    fn transfer_error_from_transaction_error_from_account_not_found() {
        let tx_err = TransactionError::FromAccountNotFound;
        let transfer_err: OperationError = tx_err.into();
        assert!(
            matches!(transfer_err, OperationError::NotFound { resource } if resource == "from account")
        );
    }

    #[test]
    fn transfer_error_from_transaction_error_to_account_not_found() {
        let tx_err = TransactionError::ToAccountNotFound;
        let transfer_err: OperationError = tx_err.into();
        assert!(
            matches!(transfer_err, OperationError::NotFound { resource } if resource == "to account")
        );
    }

    #[test]
    fn transfer_error_from_transaction_error_same_account() {
        let tx_err = TransactionError::SameAccountTransfer;
        let transfer_err: OperationError = tx_err.into();
        assert!(matches!(transfer_err, OperationError::InvalidInput { .. }));
    }

    #[test]
    fn transfer_error_from_transaction_error_insufficient_funds() {
        let tx_err = TransactionError::InsufficientFunds;
        let transfer_err: OperationError = tx_err.into();
        assert!(matches!(transfer_err, OperationError::InsufficientFunds));
    }

    #[test]
    fn transfer_error_from_transaction_error_account_unavailable() {
        let tx_err = TransactionError::AccountUnavailable;
        let transfer_err: OperationError = tx_err.into();
        assert!(matches!(transfer_err, OperationError::Unavailable { .. }));
    }

    #[test]
    fn transfer_error_from_transaction_error_lock_timeout() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::LockTimeout);
        let transfer_err: OperationError = tx_err.into();
        assert!(matches!(transfer_err, OperationError::LockTimeout));
    }

    #[test]
    fn transfer_error_from_transaction_error_connection_error() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::ConnectionError(
            "db down".to_string(),
        ));
        let transfer_err: OperationError = tx_err.into();
        assert!(matches!(transfer_err, OperationError::ConnectionError(_)));
    }
}
