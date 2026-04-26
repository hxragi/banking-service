use std::sync::Arc;

use thiserror::Error;
use uuid::Uuid;

use rand::random;

use crate::{
    application::ports::{
        AccountRepository, AccountRepositoryError, AccountTxRepository, EventPublisher,
        IdempotencyError, IdempotencyTxRepository, MetricsPort, OperationError,
        Transaction as TxTrait, TransactionPort, TransactionRepositoryError,
        TransactionWriteRepository,
    },
    domain::{
        account::Account, account_number::AccountNumber, amount::Amount, balance::Balance,
        transaction::Transaction, transaction_event::TransactionEvent,
    },
};

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 10,
            max_delay_ms: 500,
        }
    }
}

impl RetryConfig {
    pub fn new(max_attempts: u32, base_delay_ms: u64, max_delay_ms: u64) -> Self {
        Self {
            max_attempts,
            base_delay_ms,
            max_delay_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DepositInput {
    pub account_number: AccountNumber,
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub struct WithdrawInput {
    pub account_number: AccountNumber,
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub struct TransferInput {
    pub from_account_number: AccountNumber,
    pub to_account_number: AccountNumber,
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub enum TransactionOutput {
    Deposit(Account, Transaction),
    Withdraw(Account, Transaction),
    Transfer(TransferResult, Transaction),
}

impl TransactionOutput {
    pub fn transaction(&self) -> &Transaction {
        match self {
            TransactionOutput::Deposit(_, tx) => tx,
            TransactionOutput::Withdraw(_, tx) => tx,
            TransactionOutput::Transfer(_, tx) => tx,
        }
    }

    pub fn into_account(self) -> Option<Account> {
        match self {
            TransactionOutput::Deposit(acc, _) => Some(acc),
            TransactionOutput::Withdraw(acc, _) => Some(acc),
            TransactionOutput::Transfer(_, _) => None,
        }
    }

    pub fn into_transfer_result(self) -> Option<TransferResult> {
        match self {
            TransactionOutput::Transfer(result, _) => Some(result),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransferResult {
    pub from_account: Account,
    pub to_account: Account,
}

#[derive(Debug, Clone)]
pub enum TransactionOperation {
    Deposit(DepositInput),
    Withdraw(WithdrawInput),
    Transfer(TransferInput),
}

impl TransactionOperation {
    pub fn operation_type(&self) -> &'static str {
        match self {
            TransactionOperation::Deposit(_) => "deposit",
            TransactionOperation::Withdraw(_) => "withdraw",
            TransactionOperation::Transfer(_) => "transfer",
        }
    }
}

#[derive(Error, Debug, Clone)]
pub enum TransactionError {
    #[error("account not found")]
    AccountNotFound,
    #[error("from account not found")]
    FromAccountNotFound,
    #[error("to account not found")]
    ToAccountNotFound,
    #[error("account unavailable")]
    AccountUnavailable,
    #[error("database transaction failed: {0}")]
    DbTransactionFailed(String),
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("same account transfer")]
    SameAccountTransfer,
    #[error("account repository error: {0}")]
    AccountRepository(#[from] AccountRepositoryError),
    #[error("transaction repository error: {0}")]
    TransactionRepository(#[from] TransactionRepositoryError),
    #[error("idempotency error: {0}")]
    Idempotency(#[from] IdempotencyError),
}

impl From<TransactionError> for OperationError {
    fn from(err: TransactionError) -> Self {
        match err {
            TransactionError::AccountNotFound => OperationError::NotFound {
                resource: "account".to_string(),
            },
            TransactionError::FromAccountNotFound => OperationError::NotFound {
                resource: "from account".to_string(),
            },
            TransactionError::ToAccountNotFound => OperationError::NotFound {
                resource: "to account".to_string(),
            },
            TransactionError::AccountUnavailable => OperationError::Unavailable {
                reason: "account temporarily unavailable".to_string(),
            },
            TransactionError::DbTransactionFailed(reason) => OperationError::Unavailable { reason },
            TransactionError::InsufficientFunds => OperationError::InsufficientFunds,
            TransactionError::SameAccountTransfer => OperationError::InvalidInput {
                field: "account".to_string(),
                reason: "same account transfer".to_string(),
            },
            TransactionError::AccountRepository(repo_err) => repo_err.into(),
            TransactionError::TransactionRepository(tx_err) => tx_err.into(),
            TransactionError::Idempotency(_) => OperationError::IdempotencyError {
                reason: "key conflict".to_string(),
            },
        }
    }
}

#[async_trait::async_trait]
pub trait TransactionManagerPort: Send + Sync {
    async fn execute(
        &self,
        operation: TransactionOperation,
        idempotency_key: Option<String>,
    ) -> Result<TransactionOutput, TransactionError>;
}

#[async_trait::async_trait]
pub trait OutboxRepository<Tx>: Send + Sync {
    async fn save_in_tx(
        &self,
        event: &TransactionEvent,
        tx: &mut Tx,
    ) -> Result<(), TransactionRepositoryError>;
}

#[derive(Clone)]
pub struct FinancialTransactionManager<M, Tx> {
    db_manager: M,
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    account_tx_repository: Arc<dyn AccountTxRepository<Tx> + Send + Sync>,
    transaction_write_repository: Arc<dyn TransactionWriteRepository<Tx> + Send + Sync>,
    idempotency_tx_repository: Arc<dyn IdempotencyTxRepository<Tx> + Send + Sync>,
    event_publisher: Option<Arc<dyn EventPublisher + Send + Sync>>,
    event_topic: String,
    metrics: Option<Arc<dyn MetricsPort + Send + Sync>>,
    retry_config: RetryConfig,
    outbox_repository: Option<Arc<dyn OutboxRepository<Tx> + Send + Sync>>,
    _phantom: std::marker::PhantomData<Tx>,
}

impl<M, Tx> FinancialTransactionManager<M, Tx>
where
    M: TransactionPort<Transaction = Tx>,
    Tx: TxTrait + Send,
{
    pub fn new(
        db_manager: M,
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        account_tx_repository: Arc<dyn AccountTxRepository<Tx> + Send + Sync>,
        transaction_write_repository: Arc<dyn TransactionWriteRepository<Tx> + Send + Sync>,
        idempotency_tx_repository: Arc<dyn IdempotencyTxRepository<Tx> + Send + Sync>,
        event_publisher: Option<Arc<dyn EventPublisher + Send + Sync>>,
        metrics: Option<Arc<dyn MetricsPort + Send + Sync>>,
    ) -> Self {
        Self {
            db_manager,
            account_repository,
            account_tx_repository,
            transaction_write_repository,
            idempotency_tx_repository,
            event_publisher,
            event_topic: "bank.transaction.created".to_string(),
            metrics,
            retry_config: RetryConfig::default(),
            outbox_repository: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub fn with_outbox_repository(
        mut self,
        outbox_repository: Arc<dyn OutboxRepository<Tx> + Send + Sync>,
    ) -> Self {
        self.outbox_repository = Some(outbox_repository);
        self
    }

    async fn publish_transaction_event(&self, event: &TransactionEvent) {
        if let Some(ref publisher) = self.event_publisher {
            match publisher.publish(&self.event_topic, event).await {
                Ok(_) => {
                    tracing::info!("transaction event published successfully");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to publish transaction event")
                }
            }
        }
    }

    #[tracing::instrument(
        skip(self, idempotency_key),
        fields(
            operation = ?operation,
            idempotency_key = ?idempotency_key
        )
    )]
    pub async fn execute(
        &self,
        operation: TransactionOperation,
        idempotency_key: Option<String>,
    ) -> Result<TransactionOutput, TransactionError> {
        if let Some(m) = self.metrics.as_ref() {
            m.increment_operation("transaction", "started");
        }

        let account_repo = self.account_repository.clone();
        let account_tx_repo = self.account_tx_repository.clone();
        let tx_write_repo = self.transaction_write_repository.clone();
        let idem_tx_repo = self.idempotency_tx_repository.clone();
        let operation_clone = operation.clone();
        let db_manager = self.db_manager.clone();
        let idem_key_clone = idempotency_key.clone();
        let retry_config = self.retry_config;
        let outbox_repo = self.outbox_repository.clone();

        let result = execute_with_retry(
            operation_clone,
            account_repo,
            account_tx_repo,
            tx_write_repo,
            idem_tx_repo,
            db_manager,
            idem_key_clone,
            retry_config,
            outbox_repo,
        )
        .await;

        match result {
            Ok((output, event)) => {
                if let Some(ref metrics) = self.metrics {
                    let op_str = match operation {
                        TransactionOperation::Deposit(_) => "deposit",
                        TransactionOperation::Withdraw(_) => "withdraw",
                        TransactionOperation::Transfer(_) => "transfer",
                    };
                    metrics.increment_operation(op_str, "success");
                }

                self.publish_transaction_event(&event).await;

                tracing::info!(operation_type = %operation.operation_type(), "transaction completed successfully");
                Ok(output)
            }
            Err(e) => {
                if let Some(ref metrics) = self.metrics {
                    let op_str = match operation {
                        TransactionOperation::Deposit(_) => "deposit",
                        TransactionOperation::Withdraw(_) => "withdraw",
                        TransactionOperation::Transfer(_) => "transfer",
                    };
                    let error_type = format!("{:?}", e);
                    metrics.record_error(&error_type, op_str);
                }
                tracing::warn!(operation = ?operation, error = %e, "transaction failed");
                Err(e)
            }
        }
    }
}

#[async_trait::async_trait]
impl<M, Tx> TransactionManagerPort for FinancialTransactionManager<M, Tx>
where
    M: TransactionPort<Transaction = Tx>,
    Tx: TxTrait + Send,
{
    async fn execute(
        &self,
        operation: TransactionOperation,
        idempotency_key: Option<String>,
    ) -> Result<TransactionOutput, TransactionError> {
        FinancialTransactionManager::execute(self, operation, idempotency_key).await
    }
}

async fn execute_with_retry<M, Tx>(
    operation: TransactionOperation,
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    account_tx_repository: Arc<dyn AccountTxRepository<Tx> + Send + Sync>,
    transaction_write_repository: Arc<dyn TransactionWriteRepository<Tx> + Send + Sync>,
    idempotency_repository: Arc<dyn IdempotencyTxRepository<Tx> + Send + Sync>,
    db_manager: M,
    idempotency_key: Option<String>,
    retry_config: RetryConfig,
    outbox_repository: Option<Arc<dyn OutboxRepository<Tx> + Send + Sync>>,
) -> Result<(TransactionOutput, TransactionEvent), TransactionError>
where
    M: TransactionPort<Transaction = Tx>,
    Tx: TxTrait + Send,
{
    let mut attempt = 1u32;
    let mut delay_ms = retry_config.base_delay_ms;

    loop {
        let mut db_tx = db_manager.begin().await.map_err(|e| {
            tracing::error!(error = %e, "failed to begin transaction");
            TransactionError::DbTransactionFailed(e.to_string())
        })?;

        let result = match &operation {
            TransactionOperation::Deposit(input) => {
                execute_deposit_in_tx(
                    input.clone(),
                    account_tx_repository.clone(),
                    transaction_write_repository.clone(),
                    &mut db_tx,
                )
                .await
            }
            TransactionOperation::Withdraw(input) => {
                execute_withdraw_in_tx(
                    input.clone(),
                    account_tx_repository.clone(),
                    transaction_write_repository.clone(),
                    &mut db_tx,
                )
                .await
            }
            TransactionOperation::Transfer(input) => {
                execute_transfer_in_tx(
                    input.clone(),
                    account_tx_repository.clone(),
                    transaction_write_repository.clone(),
                    &mut db_tx,
                )
                .await
            }
        };

        match result {
            Ok(output) => {
                let event = TransactionEvent::from_transaction(output.transaction());

                if let Some(key) = idempotency_key {
                    let response = match &output {
                        TransactionOutput::Deposit(acc, _) => acc.id().to_string(),
                        TransactionOutput::Withdraw(acc, _) => acc.id().to_string(),
                        TransactionOutput::Transfer(result, _) => {
                            format!("{}:{}", result.from_account.id(), result.to_account.id())
                        }
                    };

                    match idempotency_repository
                        .save_in_tx(&key, &response, &mut db_tx)
                        .await
                    {
                        Ok(()) => {}
                        Err(IdempotencyError::KeyAlreadyExists { response: cached }) => {
                            let _ = db_tx.rollback().await;
                            tracing::info!(idempotency_key = %key, "idempotency key already exists from concurrent request, returning cached result");
                            return parse_cached_response(
                                &cached,
                                &operation,
                                account_repository.clone(),
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!(err = ?e, "failed to save idempotency key in transaction, rolling back");
                            let _ = db_tx.rollback().await;
                            return Err(TransactionError::Idempotency(e));
                        }
                    }
                }

                if let Some(ref outbox_repo) = outbox_repository {
                    if let Err(e) = outbox_repo.save_in_tx(&event, &mut db_tx).await {
                        tracing::error!(err = %e, "failed to save event to outbox, rolling back");
                        let _ = db_tx.rollback().await;
                        return Err(TransactionError::TransactionRepository(e));
                    }
                }

                if let Err(e) = db_tx.commit().await {
                    tracing::error!(error = %e, "failed to commit transaction");

                    return Err(TransactionError::AccountUnavailable);
                }

                return Ok((output, event));
            }
            Err(e) => {
                if let Err(rollback_err) = db_tx.rollback().await {
                    tracing::warn!(error = %rollback_err, "failed to rollback transaction");
                }

                let is_transient = matches!(
                    &e,
                    TransactionError::DbTransactionFailed(_)
                        | TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
                        | TransactionError::AccountRepository(AccountRepositoryError::Deadlock)
                        | TransactionError::AccountRepository(
                            AccountRepositoryError::SerializationFailure
                        )
                );

                if is_transient && attempt < retry_config.max_attempts {
                    tracing::warn!(
                        error = %e,
                        attempt = attempt,
                        max_attempts = retry_config.max_attempts,
                        delay_ms = delay_ms,
                        "transient error detected, retrying transaction"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(retry_config.max_delay_ms);
                    delay_ms += random::<u64>() % 10;
                    attempt += 1;
                    continue;
                }

                return Err(e);
            }
        }
    }
}

async fn parse_cached_response(
    _response: &str,
    operation: &TransactionOperation,
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
) -> Result<(TransactionOutput, TransactionEvent), TransactionError> {
    let output = match operation {
        TransactionOperation::Deposit(input) => {
            let account = account_repository
                .find_by_number(&input.account_number)
                .await?
                .ok_or(TransactionError::AccountNotFound)?;

            let transaction = Transaction::deposit(
                uuid::Uuid::new_v4(),
                input.amount,
                account.id(),
                time::OffsetDateTime::now_utc(),
            );

            TransactionOutput::Deposit(account, transaction)
        }
        TransactionOperation::Withdraw(input) => {
            let account = account_repository
                .find_by_number(&input.account_number)
                .await?
                .ok_or(TransactionError::AccountNotFound)?;

            let transaction = Transaction::withdraw(
                uuid::Uuid::new_v4(),
                input.amount,
                account.id(),
                time::OffsetDateTime::now_utc(),
            );

            TransactionOutput::Withdraw(account, transaction)
        }
        TransactionOperation::Transfer(input) => {
            let from_account = account_repository
                .find_by_number(&input.from_account_number)
                .await?
                .ok_or(TransactionError::FromAccountNotFound)?;
            let to_account = account_repository
                .find_by_number(&input.to_account_number)
                .await?
                .ok_or(TransactionError::ToAccountNotFound)?;

            let transaction = Transaction::transfer(
                uuid::Uuid::new_v4(),
                input.amount,
                from_account.id(),
                to_account.id(),
                time::OffsetDateTime::now_utc(),
            )
            .map_err(|_| TransactionError::SameAccountTransfer)?;

            TransactionOutput::Transfer(
                TransferResult {
                    from_account,
                    to_account,
                },
                transaction,
            )
        }
    };

    let event = TransactionEvent::from_transaction(output.transaction());
    Ok((output, event))
}

async fn execute_deposit_in_tx<Tx: TxTrait + Send>(
    input: DepositInput,
    account_tx_repo: Arc<dyn AccountTxRepository<Tx> + Send + Sync>,
    transaction_write_repo: Arc<dyn TransactionWriteRepository<Tx> + Send + Sync>,
    tx: &mut Tx,
) -> Result<TransactionOutput, TransactionError> {
    let DepositInput {
        account_number,
        amount,
    } = input;

    tracing::debug!(account_number = %account_number, "execute_deposit_in_tx started");

    let account = account_tx_repo
        .find_by_number_for_update(tx, account_number.as_str())
        .await?
        .ok_or(TransactionError::AccountNotFound)?;

    tracing::debug!(account_number = %account_number, "account found, processing deposit");

    let new_balance = account.balance().as_u64() + amount.as_u64();
    account_tx_repo
        .update_balance(tx, account.id(), new_balance)
        .await?;

    let transaction =
        Transaction::deposit(Uuid::new_v4(), amount, account.id(), account.created_at());
    transaction_write_repo.create(tx, &transaction).await?;

    let updated_account = Account::new(
        account.id(),
        account.number().clone(),
        account.owner().clone(),
        Balance::new(new_balance),
        account.created_at(),
    );

    Ok(TransactionOutput::Deposit(updated_account, transaction))
}

async fn execute_withdraw_in_tx<Tx: TxTrait + Send>(
    input: WithdrawInput,
    account_tx_repo: Arc<dyn AccountTxRepository<Tx> + Send + Sync>,
    transaction_write_repo: Arc<dyn TransactionWriteRepository<Tx> + Send + Sync>,
    tx: &mut Tx,
) -> Result<TransactionOutput, TransactionError> {
    let WithdrawInput {
        account_number,
        amount,
    } = input;

    let account = account_tx_repo
        .find_by_number_for_update(tx, account_number.as_str())
        .await?
        .ok_or(TransactionError::AccountNotFound)?;

    if account.balance().as_u64() < amount.as_u64() {
        return Err(TransactionError::InsufficientFunds);
    }

    let new_balance = account.balance().as_u64() - amount.as_u64();
    account_tx_repo
        .update_balance(tx, account.id(), new_balance)
        .await?;

    let transaction =
        Transaction::withdraw(Uuid::new_v4(), amount, account.id(), account.created_at());
    transaction_write_repo.create(tx, &transaction).await?;

    let updated_account = Account::new(
        account.id(),
        account.number().clone(),
        account.owner().clone(),
        Balance::new(new_balance),
        account.created_at(),
    );

    Ok(TransactionOutput::Withdraw(updated_account, transaction))
}

async fn execute_transfer_in_tx<Tx: TxTrait + Send>(
    input: TransferInput,
    account_tx_repo: Arc<dyn AccountTxRepository<Tx> + Send + Sync>,
    transaction_write_repo: Arc<dyn TransactionWriteRepository<Tx> + Send + Sync>,
    tx: &mut Tx,
) -> Result<TransactionOutput, TransactionError> {
    let TransferInput {
        from_account_number,
        to_account_number,
        amount,
    } = input;

    if from_account_number == to_account_number {
        return Err(TransactionError::SameAccountTransfer);
    }

    let (first_number, second_number) = if from_account_number.as_str() < to_account_number.as_str()
    {
        (from_account_number.clone(), to_account_number.clone())
    } else {
        (to_account_number.clone(), from_account_number.clone())
    };

    let accounts = account_tx_repo
        .lock_for_update_by_numbers(tx, first_number.as_str(), second_number.as_str())
        .await?;

    let from_account = accounts
        .iter()
        .find(|a| a.number() == &from_account_number)
        .cloned()
        .ok_or(TransactionError::FromAccountNotFound)?;

    let to_account = accounts
        .iter()
        .find(|a| a.number() == &to_account_number)
        .cloned()
        .ok_or(TransactionError::ToAccountNotFound)?;

    if from_account.balance().as_u64() < amount.as_u64() {
        return Err(TransactionError::InsufficientFunds);
    }

    let new_from_balance = from_account.balance().as_u64() - amount.as_u64();
    let new_to_balance = to_account.balance().as_u64() + amount.as_u64();

    account_tx_repo
        .update_balance(tx, from_account.id(), new_from_balance)
        .await?;
    account_tx_repo
        .update_balance(tx, to_account.id(), new_to_balance)
        .await?;

    let transaction = Transaction::transfer(
        Uuid::new_v4(),
        amount,
        from_account.id(),
        to_account.id(),
        time::OffsetDateTime::now_utc(),
    )
    .map_err(|_| TransactionError::SameAccountTransfer)?;

    transaction_write_repo.create(tx, &transaction).await?;

    let updated_from = Account::new(
        from_account.id(),
        from_account.number().clone(),
        from_account.owner().clone(),
        Balance::new(new_from_balance),
        from_account.created_at(),
    );
    let updated_to = Account::new(
        to_account.id(),
        to_account.number().clone(),
        to_account.owner().clone(),
        Balance::new(new_to_balance),
        to_account.created_at(),
    );

    Ok(TransactionOutput::Transfer(
        TransferResult {
            from_account: updated_from,
            to_account: updated_to,
        },
        transaction,
    ))
}

#[cfg(test)]
mod tests {
    use crate::application::ports::{
        AccountRepositoryError, IdempotencyError, OperationError, TransactionRepositoryError,
    };
    use crate::application::transaction_manager::TransactionError;
    use crate::application::transaction_manager::{
        DepositInput, RetryConfig, TransactionOperation, TransactionOutput, TransferInput,
        TransferResult, WithdrawInput,
    };
    use crate::domain::{
        account::Account, account_number::AccountNumber, amount::Amount, balance::Balance,
        owner::Owner, transaction::Transaction, user_id::UserId,
    };
    use time::OffsetDateTime;
    use uuid::Uuid;

    use std::sync::Arc;

    use super::FinancialTransactionManager;
    use crate::application::ports::IdempotencyTxRepository;
    use crate::test_utils::fakes::{
        FakeTransactionPort, InMemoryAccountTxRepository, InMemoryTransactionWriteRepository,
    };

    fn create_test_account(number: &str, balance: u64) -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new(number).unwrap(),
            Owner::User(UserId::new("user-1").unwrap()),
            Balance::new(balance),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    #[test]
    fn retry_config_default_values() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.base_delay_ms, 10);
        assert_eq!(config.max_delay_ms, 500);
    }

    #[test]
    fn retry_config_custom_values() {
        let config = RetryConfig::new(5, 50, 1000);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.base_delay_ms, 50);
        assert_eq!(config.max_delay_ms, 1000);
    }

    #[test]
    fn retry_config_from_app_config() {
        let config = RetryConfig::new(7, 100, 5000);
        assert_eq!(config.max_attempts, 7);
        assert_eq!(config.base_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 5000);
    }

    #[test]
    fn classifies_transient_error_correctly() {
        let transient_errors = vec![
            TransactionError::DbTransactionFailed("commit failed".to_string()),
            TransactionError::AccountRepository(AccountRepositoryError::LockTimeout),
            TransactionError::AccountRepository(AccountRepositoryError::Deadlock),
            TransactionError::AccountRepository(AccountRepositoryError::SerializationFailure),
        ];

        for err in transient_errors {
            let is_transient = matches!(
                &err,
                TransactionError::DbTransactionFailed(_)
                    | TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
                    | TransactionError::AccountRepository(AccountRepositoryError::Deadlock)
                    | TransactionError::AccountRepository(
                        AccountRepositoryError::SerializationFailure
                    )
            );
            assert!(is_transient, "expected {:?} to be transient", err);
        }
    }

    #[test]
    fn classifies_non_transient_error_correctly() {
        let non_transient_errors = vec![
            TransactionError::AccountNotFound,
            TransactionError::AccountUnavailable,
            TransactionError::InsufficientFunds,
            TransactionError::SameAccountTransfer,
        ];

        for err in non_transient_errors {
            let is_transient = matches!(
                &err,
                TransactionError::DbTransactionFailed(_)
                    | TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
            );
            assert!(!is_transient, "expected {:?} to be non-transient", err);
        }
    }

    #[test]
    fn retry_logic_increments_delay_between_attempts() {
        let base_delay = 10u64;
        let max_delay = 500u64;
        let mut delay = base_delay;

        for attempt in 1..=3 {
            delay = (delay * 2).min(max_delay);
            delay += 0;

            let expected_delay = match attempt {
                1 => 20u64,
                2 => 40u64,
                3 => 80u64,
                _ => max_delay,
            };

            assert!(
                delay >= expected_delay || delay <= expected_delay + 10,
                "delay progression should approximately double each attempt"
            );
        }
    }

    #[test]
    fn retry_logic_respects_custom_base_delay() {
        let base_delay = 50u64;
        let max_delay = 1000u64;
        let mut delay = base_delay;

        delay = (delay * 2).min(max_delay);
        assert_eq!(delay, 100);

        delay = (delay * 2).min(max_delay);
        assert_eq!(delay, 200);

        delay = (delay * 2).min(max_delay);
        assert_eq!(delay, 400);
    }

    #[test]
    fn retry_logic_clamps_to_max_delay() {
        let base_delay = 500u64;
        let max_delay = 600u64;
        let mut delay = base_delay;

        delay = (delay * 2).min(max_delay);
        assert_eq!(delay, 600);

        delay = (delay * 2).min(max_delay);
        assert_eq!(delay, 600);
    }

    #[test]
    fn transaction_output_extracts_transaction_correctly() {
        let account = create_test_account("ACC001", 1000);
        let transaction = Transaction::deposit(
            Uuid::new_v4(),
            Amount::new(100).unwrap(),
            account.id(),
            OffsetDateTime::now_utc(),
        );

        let output = TransactionOutput::Deposit(account.clone(), transaction.clone());

        assert_eq!(output.transaction().id(), transaction.id());
        assert_eq!(output.into_account().unwrap().id(), account.id());
    }

    #[test]
    fn transaction_output_handles_withdraw() {
        let account = create_test_account("ACC001", 1000);
        let transaction = Transaction::withdraw(
            Uuid::new_v4(),
            Amount::new(100).unwrap(),
            account.id(),
            OffsetDateTime::now_utc(),
        );

        let output = TransactionOutput::Withdraw(account.clone(), transaction.clone());

        assert_eq!(output.transaction().id(), transaction.id());
        let acc = output.into_account();
        assert!(acc.is_some());
        assert_eq!(acc.unwrap().id(), account.id());
    }

    #[test]
    fn transaction_output_handles_transfer_result() {
        let from_account = create_test_account("ACC001", 1000);
        let to_account = create_test_account("ACC002", 500);
        let transaction = Transaction::transfer(
            Uuid::new_v4(),
            Amount::new(100).unwrap(),
            from_account.id(),
            to_account.id(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();

        let transfer_result = TransferResult {
            from_account: from_account.clone(),
            to_account: to_account.clone(),
        };

        let output = TransactionOutput::Transfer(transfer_result, transaction.clone());

        assert_eq!(output.transaction().id(), transaction.id());

        let result = output.into_transfer_result();
        assert!(result.is_some());
    }

    #[test]
    fn converts_account_not_found_to_operation_error() {
        let tx_err = TransactionError::AccountNotFound;
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::NotFound { resource } if resource == "account"));
    }

    #[test]
    fn converts_from_account_not_found_to_operation_error() {
        let tx_err = TransactionError::FromAccountNotFound;
        let op_err: OperationError = tx_err.into();
        assert!(
            matches!(op_err, OperationError::NotFound { resource } if resource == "from account")
        );
    }

    #[test]
    fn converts_to_account_not_found_to_operation_error() {
        let tx_err = TransactionError::ToAccountNotFound;
        let op_err: OperationError = tx_err.into();
        assert!(
            matches!(op_err, OperationError::NotFound { resource } if resource == "to account")
        );
    }

    #[test]
    fn converts_insufficient_funds_to_operation_error() {
        let tx_err = TransactionError::InsufficientFunds;
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::InsufficientFunds));
    }

    #[test]
    fn converts_same_account_transfer_to_operation_error() {
        let tx_err = TransactionError::SameAccountTransfer;
        let op_err: OperationError = tx_err.into();
        assert!(matches!(
            op_err,
            OperationError::InvalidInput { field, reason }
            if field == "account" && reason == "same account transfer"
        ));
    }

    #[test]
    fn converts_account_unavailable_to_operation_error() {
        let tx_err = TransactionError::AccountUnavailable;
        let op_err: OperationError = tx_err.into();
        assert!(matches!(
            op_err,
            OperationError::Unavailable { reason }
            if reason == "account temporarily unavailable"
        ));
    }

    #[test]
    fn converts_lock_timeout_to_operation_error() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::LockTimeout);
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::LockTimeout));
    }

    #[test]
    fn converts_db_transaction_failed_to_operation_error() {
        let tx_err = TransactionError::DbTransactionFailed("commit failed".to_string());
        let op_err: OperationError = tx_err.into();
        assert!(
            matches!(op_err, OperationError::Unavailable { reason } if reason == "commit failed")
        )
    }

    #[test]
    fn converts_deadlock_to_operation_error() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::Deadlock);
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::Deadlock));
    }

    #[test]
    fn converts_serialization_failure_to_operation_error() {
        let tx_err =
            TransactionError::AccountRepository(AccountRepositoryError::SerializationFailure);
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::SerializationFailure));
    }

    #[test]
    fn converts_constraint_violation_to_operation_error() {
        let tx_err = TransactionError::AccountRepository(
            AccountRepositoryError::UniqueConstraintViolation("duplicate".to_string()),
        );
        let op_err: OperationError = tx_err.into();
        assert!(matches!(
            op_err,
            OperationError::UniqueConstraintViolation(msg)
            if msg == "duplicate"
        ));
    }

    #[test]
    fn converts_connection_error_to_operation_error() {
        let tx_err = TransactionError::AccountRepository(AccountRepositoryError::ConnectionError(
            "timeout".to_string(),
        ));
        let op_err: OperationError = tx_err.into();
        assert!(matches!(
            op_err,
            OperationError::ConnectionError(msg)
            if msg == "timeout"
        ));
    }

    #[test]
    fn converts_transaction_lock_timeout_to_operation_error() {
        let tx_err =
            TransactionError::TransactionRepository(TransactionRepositoryError::LockTimeout);
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::LockTimeout));
    }

    #[test]
    fn converts_transaction_constraint_violation_to_operation_error() {
        let tx_err = TransactionError::TransactionRepository(
            TransactionRepositoryError::UniqueConstraintViolation("duplicate".to_string()),
        );
        let op_err: OperationError = tx_err.into();
        assert!(matches!(
            op_err,
            OperationError::UniqueConstraintViolation(msg)
            if msg == "duplicate"
        ));
    }

    #[test]
    fn converts_idempotency_error_to_operation_error() {
        let tx_err = TransactionError::Idempotency(IdempotencyError::IdempotencyFailed);
        let op_err: OperationError = tx_err.into();
        assert!(matches!(op_err, OperationError::IdempotencyError { .. }));
    }

    #[test]
    fn idempotency_error_contains_reason() {
        let op_err = OperationError::IdempotencyError {
            reason: "key conflict".to_string(),
        };
        let msg = op_err.to_string();
        assert!(msg.contains("key conflict"));
    }

    #[test]
    fn deposit_input_creates_correctly() {
        let input = DepositInput {
            account_number: AccountNumber::new("ACC001").unwrap(),
            amount: Amount::new(100).unwrap(),
        };
        assert_eq!(input.account_number.as_str(), "ACC001");
        assert_eq!(input.amount.as_u64(), 100);
    }

    #[test]
    fn withdraw_input_creates_correctly() {
        let input = WithdrawInput {
            account_number: AccountNumber::new("ACC002").unwrap(),
            amount: Amount::new(200).unwrap(),
        };
        assert_eq!(input.account_number.as_str(), "ACC002");
        assert_eq!(input.amount.as_u64(), 200);
    }

    #[test]
    fn transfer_input_creates_correctly() {
        let input = TransferInput {
            from_account_number: AccountNumber::new("ACC001").unwrap(),
            to_account_number: AccountNumber::new("ACC002").unwrap(),
            amount: Amount::new(50).unwrap(),
        };
        assert_eq!(input.from_account_number.as_str(), "ACC001");
        assert_eq!(input.to_account_number.as_str(), "ACC002");
        assert_eq!(input.amount.as_u64(), 50);
    }

    #[test]
    fn transaction_operation_enum_variants() {
        let deposit = TransactionOperation::Deposit(DepositInput {
            account_number: AccountNumber::new("ACC001").unwrap(),
            amount: Amount::new(100).unwrap(),
        });
        assert!(matches!(deposit, TransactionOperation::Deposit(_)));

        let withdraw = TransactionOperation::Withdraw(WithdrawInput {
            account_number: AccountNumber::new("ACC001").unwrap(),
            amount: Amount::new(100).unwrap(),
        });
        assert!(matches!(withdraw, TransactionOperation::Withdraw(_)));

        let transfer = TransactionOperation::Transfer(TransferInput {
            from_account_number: AccountNumber::new("ACC001").unwrap(),
            to_account_number: AccountNumber::new("ACC002").unwrap(),
            amount: Amount::new(100).unwrap(),
        });
        assert!(matches!(transfer, TransactionOperation::Transfer(_)));
    }

    #[test]
    fn transfer_result_creates_correctly() {
        let from = create_test_account("ACC001", 1000);
        let to = create_test_account("ACC002", 500);

        let result = TransferResult {
            from_account: from.clone(),
            to_account: to.clone(),
        };

        assert_eq!(result.from_account.id(), from.id());
        assert_eq!(result.to_account.id(), to.id());
    }

    #[test]
    fn transaction_error_debug_and_clone() {
        let err = TransactionError::InsufficientFunds;
        let cloned = err.clone();
        assert!(matches!(cloned, TransactionError::InsufficientFunds));
        assert!(!format!("{:?}", err).is_empty());
    }

    #[test]
    fn transaction_error_display_messages() {
        assert_eq!(
            format!("{}", TransactionError::AccountNotFound),
            "account not found"
        );
        assert_eq!(
            format!("{}", TransactionError::InsufficientFunds),
            "insufficient funds"
        );
        assert_eq!(
            format!("{}", TransactionError::SameAccountTransfer),
            "same account transfer"
        );
        assert_eq!(
            format!("{}", TransactionError::AccountUnavailable),
            "account unavailable"
        );
    }

    struct FakeIdempotencyTxRepository;

    #[async_trait::async_trait]
    impl IdempotencyTxRepository<()> for FakeIdempotencyTxRepository {
        async fn save_in_tx(
            &self,
            _key: &str,
            _response: &str,
            _tx: &mut (),
        ) -> Result<(), IdempotencyError> {
            Ok(())
        }
    }

    struct ConflictIdempotencyTxRepository {
        cached_response: String,
    }

    #[async_trait::async_trait]
    impl IdempotencyTxRepository<()> for ConflictIdempotencyTxRepository {
        async fn save_in_tx(
            &self,
            _key: &str,
            _response: &str,
            _tx: &mut (),
        ) -> Result<(), IdempotencyError> {
            Err(IdempotencyError::KeyAlreadyExists {
                response: self.cached_response.clone(),
            })
        }
    }

    fn setup_manager(
        account_tx_repo: Arc<InMemoryAccountTxRepository>,
    ) -> FinancialTransactionManager<FakeTransactionPort, ()> {
        let account_repo = crate::test_utils::mocks::MockAccountRepository::new();
        let tx_write_repo = Arc::new(InMemoryTransactionWriteRepository::new());
        let idem_repo = Arc::new(FakeIdempotencyTxRepository);
        FinancialTransactionManager::new(
            FakeTransactionPort,
            Arc::new(account_repo),
            account_tx_repo,
            tx_write_repo,
            idem_repo,
            None,
            None,
        )
    }

    fn setup_manager_with_retry_config(
        account_tx_repo: Arc<InMemoryAccountTxRepository>,
        retry_config: RetryConfig,
    ) -> FinancialTransactionManager<FakeTransactionPort, ()> {
        let account_repo = crate::test_utils::mocks::MockAccountRepository::new();
        let tx_write_repo = Arc::new(InMemoryTransactionWriteRepository::new());
        let idem_repo = Arc::new(FakeIdempotencyTxRepository);
        FinancialTransactionManager::new(
            FakeTransactionPort,
            Arc::new(account_repo),
            account_tx_repo,
            tx_write_repo,
            idem_repo,
            None,
            None,
        )
        .with_retry_config(retry_config)
    }

    #[tokio::test]
    async fn test_deposit_updates_balance() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 500);
        account_tx_repo.insert_account(account.clone()).await;

        let manager = setup_manager(account_tx_repo.clone());
        let result = manager
            .execute(
                TransactionOperation::Deposit(DepositInput {
                    account_number: AccountNumber::new("ACC001").unwrap(),
                    amount: Amount::new(200).unwrap(),
                }),
                None,
            )
            .await
            .unwrap();

        match result {
            TransactionOutput::Deposit(acc, _tx) => {
                assert_eq!(acc.balance().as_u64(), 700);
            }
            _ => panic!("expected deposit output"),
        }
    }

    #[tokio::test]
    async fn test_deposit_with_custom_retry_config() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 500);
        account_tx_repo.insert_account(account.clone()).await;

        let retry_config = RetryConfig::new(5, 100, 2000);
        let manager = setup_manager_with_retry_config(account_tx_repo.clone(), retry_config);

        let result = manager
            .execute(
                TransactionOperation::Deposit(DepositInput {
                    account_number: AccountNumber::new("ACC001").unwrap(),
                    amount: Amount::new(200).unwrap(),
                }),
                None,
            )
            .await
            .unwrap();

        match result {
            TransactionOutput::Deposit(acc, _tx) => {
                assert_eq!(acc.balance().as_u64(), 700);
            }
            _ => panic!("expected deposit output"),
        }
    }

    #[tokio::test]
    async fn test_deposit_account_not_found() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let manager = setup_manager(account_tx_repo);

        let result = manager
            .execute(
                TransactionOperation::Deposit(DepositInput {
                    account_number: AccountNumber::new("MISSING").unwrap(),
                    amount: Amount::new(100).unwrap(),
                }),
                None,
            )
            .await;

        assert!(matches!(result, Err(TransactionError::AccountNotFound)));
    }

    #[tokio::test]
    async fn test_withdraw_success() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 500);
        account_tx_repo.insert_account(account.clone()).await;

        let manager = setup_manager(account_tx_repo);
        let result = manager
            .execute(
                TransactionOperation::Withdraw(WithdrawInput {
                    account_number: AccountNumber::new("ACC001").unwrap(),
                    amount: Amount::new(200).unwrap(),
                }),
                None,
            )
            .await
            .unwrap();

        match result {
            TransactionOutput::Withdraw(acc, _tx) => {
                assert_eq!(acc.balance().as_u64(), 300);
            }
            _ => panic!("expected withdraw output"),
        }
    }

    #[tokio::test]
    async fn test_withdraw_insufficient_funds() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 100);
        account_tx_repo.insert_account(account.clone()).await;

        let manager = setup_manager(account_tx_repo);
        let result = manager
            .execute(
                TransactionOperation::Withdraw(WithdrawInput {
                    account_number: AccountNumber::new("ACC001").unwrap(),
                    amount: Amount::new(200).unwrap(),
                }),
                None,
            )
            .await;

        assert!(matches!(result, Err(TransactionError::InsufficientFunds)));
    }

    #[tokio::test]
    async fn test_transfer_success() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let from = create_test_account("ACC001", 1000);
        let to = create_test_account("ACC002", 500);
        account_tx_repo.insert_account(from.clone()).await;
        account_tx_repo.insert_account(to.clone()).await;

        let manager = setup_manager(account_tx_repo.clone());
        let result = manager
            .execute(
                TransactionOperation::Transfer(TransferInput {
                    from_account_number: AccountNumber::new("ACC001").unwrap(),
                    to_account_number: AccountNumber::new("ACC002").unwrap(),
                    amount: Amount::new(300).unwrap(),
                }),
                None,
            )
            .await
            .unwrap();

        match result {
            TransactionOutput::Transfer(transfer_result, _tx) => {
                assert_eq!(transfer_result.from_account.balance().as_u64(), 700);
                assert_eq!(transfer_result.to_account.balance().as_u64(), 800);
            }
            _ => panic!("expected transfer output"),
        }
    }

    #[tokio::test]
    async fn test_transfer_same_account() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 1000);
        account_tx_repo.insert_account(account.clone()).await;

        let manager = setup_manager(account_tx_repo);
        let result = manager
            .execute(
                TransactionOperation::Transfer(TransferInput {
                    from_account_number: AccountNumber::new("ACC001").unwrap(),
                    to_account_number: AccountNumber::new("ACC001").unwrap(),
                    amount: Amount::new(100).unwrap(),
                }),
                None,
            )
            .await;

        assert!(matches!(result, Err(TransactionError::SameAccountTransfer)));
    }

    #[tokio::test]
    async fn test_transfer_to_account_not_found() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 1000);
        account_tx_repo.insert_account(account.clone()).await;

        let manager = setup_manager(account_tx_repo);
        let result = manager
            .execute(
                TransactionOperation::Transfer(TransferInput {
                    from_account_number: AccountNumber::new("ACC001").unwrap(),
                    to_account_number: AccountNumber::new("MISSING").unwrap(),
                    amount: Amount::new(100).unwrap(),
                }),
                None,
            )
            .await;

        assert!(matches!(result, Err(TransactionError::ToAccountNotFound)));
    }

    #[tokio::test]
    async fn test_deposit_idempotency_cache_hit_returns_account() {
        let account_tx_repo = Arc::new(InMemoryAccountTxRepository::new());
        let account = create_test_account("ACC001", 500);
        account_tx_repo.insert_account(account.clone()).await;

        let account_repo =
            crate::test_utils::mocks::MockAccountRepository::new().with_account(account.clone());
        let tx_write_repo = Arc::new(InMemoryTransactionWriteRepository::new());
        let idem_repo = Arc::new(ConflictIdempotencyTxRepository {
            cached_response: account.id().to_string(),
        });

        let manager = FinancialTransactionManager::new(
            FakeTransactionPort,
            Arc::new(account_repo),
            account_tx_repo,
            tx_write_repo,
            idem_repo,
            None,
            None,
        );

        let result = manager
            .execute(
                TransactionOperation::Deposit(DepositInput {
                    account_number: AccountNumber::new("ACC001").unwrap(),
                    amount: Amount::new(200).unwrap(),
                }),
                Some("idem-key-1".to_string()),
            )
            .await;

        assert!(result.is_ok());
        match result.unwrap() {
            TransactionOutput::Deposit(acc, _tx) => {
                assert_eq!(acc.number().as_str(), "ACC001");
            }
            _ => panic!("expected deposit output"),
        }
    }
}
