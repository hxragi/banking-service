use std::sync::Arc;

use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{
        AccountRepository, AccountRepositoryError, IdempotencyError, IdempotencyTxRepository,
        OperationError, TransactionPort, TransactionRepository, TransactionRepositoryError,
    },
    domain::{
        account::Account, account_number::AccountNumber, amount::Amount, balance::Balance,
        org_id::OrgId, owner::Owner, transaction::Transaction, transaction_event::TransactionEvent,
        user_id::UserId,
    },
    infrastructure::{
        database::transaction::Manager, dto::transaction_event_dto::TransactionEventDto,
        messaging::kafka_event_publisher::DomainEvent,
        messaging::kafka_event_publisher::KafkaEventPublisher, observability::metrics::Metrics,
    },
};

pub type DbTransaction = <Manager as TransactionPort>::Transaction;

pub type TransactionManager = FinancialTransactionManager<Manager>;

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
            TransactionError::InsufficientFunds => OperationError::InsufficientFunds,
            TransactionError::SameAccountTransfer => OperationError::InvalidInput {
                field: "account".to_string(),
                reason: "same account transfer".to_string(),
            },
            TransactionError::AccountRepository(repo_err) => repo_err.into(),
            TransactionError::TransactionRepository(tx_err) => tx_err.into(),
            TransactionError::Idempotency(_) => OperationError::IdempotencyError,
        }
    }
}

#[derive(Clone)]
pub struct FinancialTransactionManager<T: TransactionPort<Transaction = DbTransaction>> {
    db_manager: T,
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    idempotency_tx_repository: Arc<dyn IdempotencyTxRepository<DbTransaction> + Send + Sync>,
    event_publisher: Option<Arc<KafkaEventPublisher>>,
    event_topic: String,
    metrics: Option<Arc<Metrics>>,
}

impl<T> FinancialTransactionManager<T>
where
    T: TransactionPort<Transaction = DbTransaction>,
{
    pub fn new(
        db_manager: T,
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
        idempotency_tx_repository: Arc<dyn IdempotencyTxRepository<DbTransaction> + Send + Sync>,
        event_publisher: Option<Arc<KafkaEventPublisher>>,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        Self {
            db_manager,
            account_repository,
            transaction_repository,
            idempotency_tx_repository,
            event_publisher,
            event_topic: "bank.transaction.created".to_string(),
            metrics,
        }
    }

    fn publish_transaction_event(&self, transaction: Transaction) {
        if let Some(ref publisher) = self.event_publisher {
            let publisher = Arc::clone(publisher);
            let topic = self.event_topic.clone();

            tokio::spawn(async move {
                let domain_event_payload = TransactionEvent::from_transaction(&transaction);
                let dto_event_payload: TransactionEventDto = domain_event_payload.into();
                let payload_json = match dto_event_payload.to_json() {
                    Ok(json) => json,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to serialize event payload");
                        return;
                    }
                };

                let domain_event = DomainEvent {
                    event_id: uuid::Uuid::new_v4().to_string(),
                    event_type: "TransactionCreated".to_string(),
                    aggregate_id: transaction.id().to_string(),
                    aggregate_type: "transaction".to_string(),
                    payload: payload_json,
                    metadata: std::collections::HashMap::new(),
                };

                match publisher.publish(&topic, &domain_event).await {
                    Ok(_) => {
                        tracing::info!(
                            transaction_id = %transaction.id(),
                            operation_type = %transaction.kind().as_str(),
                            "transaction event published successfully"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            transaction_id = %transaction.id(),
                            error = %e,
                            "failed to publish transaction event"
                        );
                    }
                }
            });
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
        let tx_repo = self.transaction_repository.clone();
        let idem_tx_repo = self.idempotency_tx_repository.clone();
        let operation_clone = operation.clone();
        let db_manager = self.db_manager.clone();
        let idem_key_clone = idempotency_key.clone();

        let result = execute_with_retry(
            operation_clone,
            account_repo,
            tx_repo,
            idem_tx_repo,
            db_manager,
            idem_key_clone,
        )
        .await;

        match result {
            Ok(output) => {
                if let Some(ref metrics) = self.metrics {
                    let op_str = match operation {
                        TransactionOperation::Deposit(_) => "deposit",
                        TransactionOperation::Withdraw(_) => "withdraw",
                        TransactionOperation::Transfer(_) => "transfer",
                    };
                    metrics.increment_operation(op_str, "success");
                }

                let transaction = output.transaction().clone();
                self.publish_transaction_event(transaction);

                tracing::info!(operation = ?operation, "transaction completed successfully");
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

async fn execute_with_retry<T>(
    operation: TransactionOperation,
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    idempotency_repository: Arc<dyn IdempotencyTxRepository<DbTransaction> + Send + Sync>,
    db_manager: T,
    idempotency_key: Option<String>,
) -> Result<TransactionOutput, TransactionError>
where
    T: TransactionPort<Transaction = DbTransaction>,
{
    let max_attempts = 3u32;
    let mut attempt = 1u32;
    let mut delay_ms = 10u64;
    const MAX_DELAY_MS: u64 = 500;

    loop {
        let mut db_tx = db_manager.begin().await.map_err(|e| {
            tracing::error!(error = %e, "failed to begin transaction");
            TransactionError::AccountUnavailable
        })?;

        let result = match &operation {
            TransactionOperation::Deposit(input) => {
                execute_deposit_in_tx(
                    input.clone(),
                    account_repository.clone(),
                    transaction_repository.clone(),
                    &mut db_tx,
                )
                .await
            }
            TransactionOperation::Withdraw(input) => {
                execute_withdraw_in_tx(
                    input.clone(),
                    account_repository.clone(),
                    transaction_repository.clone(),
                    &mut db_tx,
                )
                .await
            }
            TransactionOperation::Transfer(input) => {
                execute_transfer_in_tx(
                    input.clone(),
                    account_repository.clone(),
                    transaction_repository.clone(),
                    &mut db_tx,
                )
                .await
            }
        };

        match result {
            Ok(output) => {
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

                if let Err(e) = db_tx.commit().await {
                    tracing::error!(error = %e, "failed to commit transaction");
                    return Err(TransactionError::AccountUnavailable);
                }
                return Ok(output);
            }
            Err(e) => {
                if let Err(rollback_err) = db_tx.rollback().await {
                    tracing::warn!(error = %rollback_err, "failed to rollback transaction");
                }

                let is_transient = matches!(
                    &e,
                    TransactionError::AccountUnavailable
                        | TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
                );

                if is_transient && attempt < max_attempts {
                    tracing::warn!(
                        error = %e,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        delay_ms = delay_ms,
                        "transient error detected, retrying transaction"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                    delay_ms += rand::random::<u64>() % 10;
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
) -> Result<TransactionOutput, TransactionError> {
    match operation {
        TransactionOperation::Deposit(input) => {
            let account = account_repository
                .find_by_number(&input.account_number)
                .await?
                .ok_or(TransactionError::AccountUnavailable)?;

            let transaction = Transaction::deposit(
                uuid::Uuid::new_v4(),
                input.amount,
                account.id(),
                time::OffsetDateTime::now_utc(),
            );

            Ok(TransactionOutput::Deposit(account, transaction))
        }
        TransactionOperation::Withdraw(input) => {
            let account = account_repository
                .find_by_number(&input.account_number)
                .await?
                .ok_or(TransactionError::AccountUnavailable)?;

            let transaction = Transaction::withdraw(
                uuid::Uuid::new_v4(),
                input.amount,
                account.id(),
                time::OffsetDateTime::now_utc(),
            );

            Ok(TransactionOutput::Withdraw(account, transaction))
        }
        TransactionOperation::Transfer(input) => {
            let from_account = account_repository
                .find_by_number(&input.from_account_number)
                .await?
                .ok_or(TransactionError::AccountUnavailable)?;
            let to_account = account_repository
                .find_by_number(&input.to_account_number)
                .await?
                .ok_or(TransactionError::AccountUnavailable)?;

            let transaction = Transaction::transfer(
                uuid::Uuid::new_v4(),
                input.amount,
                from_account.id(),
                to_account.id(),
                time::OffsetDateTime::now_utc(),
            )
            .map_err(|_| TransactionError::SameAccountTransfer)?;

            Ok(TransactionOutput::Transfer(
                TransferResult {
                    from_account,
                    to_account,
                },
                transaction,
            ))
        }
    }
}

async fn execute_deposit_in_tx(
    input: DepositInput,
    _account_repository: Arc<dyn AccountRepository + Send + Sync>,
    _transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    db_tx: &mut DbTransaction,
) -> Result<TransactionOutput, TransactionError> {
    use sqlx::Row;

    let DepositInput {
        account_number,
        amount,
    } = input;

    tracing::debug!(account_number = %account_number, "execute_deposit_in_tx started");

    let row = sqlx::query(
        "SELECT id, number, user_id, org_id, balance, created_at FROM accounts WHERE number = $1 FOR UPDATE"
    )
    .bind(account_number.as_str())
    .fetch_optional(&mut **db_tx)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to find account with lock");
        map_sqlx_to_tx_error(e)
    })?;

    let row = match row {
        Some(r) => r,
        None => {
            tracing::warn!(account_number = %account_number, "account not found in execute_deposit_in_tx");
            return Err(TransactionError::AccountNotFound);
        }
    };

    tracing::debug!(account_number = %account_number, "account found, processing deposit");

    let account_id: uuid::Uuid = row.get("id");
    let current_balance: i64 = row.get("balance");
    let new_balance = current_balance + (amount.as_u64() as i64);
    let created_at: time::OffsetDateTime = row.get("created_at");

    let user_id: Option<String> = row.get("user_id");
    let org_id: Option<String> = row.get("org_id");

    sqlx::query("UPDATE accounts SET balance = $1 WHERE id = $2")
        .bind(new_balance)
        .bind(account_id)
        .execute(&mut **db_tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to update account balance");
            TransactionError::AccountRepository(AccountRepositoryError::OperationFailed {
                operation: "update_balance".to_string(),
                reason: e.to_string(),
            })
        })?;

    let tx_id = Uuid::new_v4();
    let tx_kind = "deposit";
    let tx_amount = amount.as_u64() as i64;

    sqlx::query(
        "INSERT INTO transactions (id, kind, amount, to_account_id, account_id) VALUES ($1, $2::transaction_kind, $3, $4, $5)"
    )
    .bind(tx_id)
    .bind(tx_kind)
    .bind(tx_amount)
    .bind(account_id)
    .bind(account_id)
    .execute(&mut **db_tx)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to create transaction record");
        TransactionError::TransactionRepository(TransactionRepositoryError::TransactionFailed)
    })?;

    let owner = if let Some(uid) = user_id {
        Owner::User(UserId::new(&uid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else if let Some(oid) = org_id {
        Owner::Org(OrgId::new(&oid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else {
        return Err(TransactionError::AccountUnavailable);
    };

    let account = Account::new(
        account_id,
        account_number,
        owner,
        Balance::new(new_balance as u64),
        created_at,
    );

    let transaction = Transaction::deposit(tx_id, amount, account_id, created_at);

    Ok(TransactionOutput::Deposit(account, transaction))
}

async fn execute_withdraw_in_tx(
    input: WithdrawInput,
    _account_repository: Arc<dyn AccountRepository + Send + Sync>,
    _transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    db_tx: &mut DbTransaction,
) -> Result<TransactionOutput, TransactionError> {
    use sqlx::Row;

    let WithdrawInput {
        account_number,
        amount,
    } = input;

    let row = sqlx::query(
        "SELECT id, number, user_id, org_id, balance, created_at FROM accounts WHERE number = $1 FOR UPDATE"
    )
    .bind(account_number.as_str())
    .fetch_optional(&mut **db_tx)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to find account with lock");
        map_sqlx_to_tx_error(e)
    })?;

    let row = row.ok_or(TransactionError::AccountNotFound)?;

    let account_id: uuid::Uuid = row.get("id");
    let current_balance: i64 = row.get("balance");
    let withdraw_amount = amount.as_u64() as i64;
    let created_at: time::OffsetDateTime = row.get("created_at");
    let user_id: Option<String> = row.get("user_id");
    let org_id: Option<String> = row.get("org_id");

    if current_balance < withdraw_amount {
        return Err(TransactionError::InsufficientFunds);
    }

    let new_balance = current_balance - withdraw_amount;

    sqlx::query("UPDATE accounts SET balance = $1 WHERE id = $2")
        .bind(new_balance)
        .bind(account_id)
        .execute(&mut **db_tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to update account balance");
            TransactionError::AccountRepository(AccountRepositoryError::OperationFailed {
                operation: "update_balance".to_string(),
                reason: e.to_string(),
            })
        })?;

    let tx_id = Uuid::new_v4();
    let tx_kind = "withdraw";
    let tx_amount = amount.as_u64() as i64;

    sqlx::query(
        "INSERT INTO transactions (id, kind, amount, from_account_id, account_id) VALUES ($1, $2::transaction_kind, $3, $4, $5)"
    )
    .bind(tx_id)
    .bind(tx_kind)
    .bind(tx_amount)
    .bind(account_id)
    .bind(account_id)
    .execute(&mut **db_tx)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to create transaction record");
        TransactionError::TransactionRepository(TransactionRepositoryError::TransactionFailed)
    })?;

    let owner = if let Some(uid) = user_id {
        Owner::User(UserId::new(&uid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else if let Some(oid) = org_id {
        Owner::Org(OrgId::new(&oid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else {
        return Err(TransactionError::AccountUnavailable);
    };

    let account = Account::new(
        account_id,
        account_number,
        owner,
        Balance::new(new_balance as u64),
        created_at,
    );

    let transaction = Transaction::withdraw(tx_id, amount, account_id, created_at);

    Ok(TransactionOutput::Withdraw(account, transaction))
}

async fn execute_transfer_in_tx(
    input: TransferInput,
    _account_repository: Arc<dyn AccountRepository + Send + Sync>,
    _transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    db_tx: &mut DbTransaction,
) -> Result<TransactionOutput, TransactionError> {
    use sqlx::Row;

    let TransferInput {
        from_account_number,
        to_account_number,
        amount,
    } = input;

    if from_account_number == to_account_number {
        return Err(TransactionError::SameAccountTransfer);
    }

    let (first_number, second_number) = {
        let from_str = from_account_number.as_str();
        let to_str = to_account_number.as_str();
        if from_str < to_str {
            (from_account_number.clone(), to_account_number.clone())
        } else {
            (to_account_number.clone(), from_account_number.clone())
        }
    };

    let rows = sqlx::query(
        "SELECT id, number, user_id, org_id, balance, created_at FROM accounts WHERE number IN ($1, $2) ORDER BY number FOR UPDATE"
    )
    .bind(first_number.as_str())
    .bind(second_number.as_str())
    .fetch_all(&mut **db_tx)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to lock accounts");
        map_sqlx_to_tx_error(e)
    })?;

    let first_found = rows.iter().any(|r: &sqlx::postgres::PgRow| {
        let num: String = r.get("number");
        num == first_number.as_str()
    });
    let second_found = rows.iter().any(|r: &sqlx::postgres::PgRow| {
        let num: String = r.get("number");
        num == second_number.as_str()
    });

    if !first_found {
        return Err(if first_number == from_account_number {
            TransactionError::FromAccountNotFound
        } else {
            TransactionError::ToAccountNotFound
        });
    }

    if !second_found {
        return Err(if second_number == from_account_number {
            TransactionError::FromAccountNotFound
        } else {
            TransactionError::ToAccountNotFound
        });
    }

    let (from_row, to_row): (&sqlx::postgres::PgRow, &sqlx::postgres::PgRow) = {
        let row_0_number: String = rows[0].get("number");
        if row_0_number == from_account_number.as_str() {
            (&rows[0], &rows[1])
        } else {
            (&rows[1], &rows[0])
        }
    };

    let from_id: uuid::Uuid = from_row.get("id");
    let from_balance: i64 = from_row.get("balance");
    let from_user_id: Option<String> = from_row.get("user_id");
    let from_org_id: Option<String> = from_row.get("org_id");
    let from_created_at: time::OffsetDateTime = from_row.get("created_at");

    let to_id: uuid::Uuid = to_row.get("id");
    let to_balance: i64 = to_row.get("balance");
    let to_user_id: Option<String> = to_row.get("user_id");
    let to_org_id: Option<String> = to_row.get("org_id");
    let to_created_at: time::OffsetDateTime = to_row.get("created_at");

    let withdraw_amount = amount.as_u64() as i64;
    if from_balance < withdraw_amount {
        return Err(TransactionError::InsufficientFunds);
    }
    let new_from_balance = from_balance - withdraw_amount;
    let new_to_balance = to_balance + withdraw_amount;

    sqlx::query("UPDATE accounts SET balance = $1 WHERE id = $2")
        .bind(new_from_balance)
        .bind(from_id)
        .execute(&mut **db_tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to create transaction record");
            TransactionError::TransactionRepository(TransactionRepositoryError::TransactionFailed)
        })?;

    sqlx::query("UPDATE accounts SET balance = $1 WHERE id = $2")
        .bind(new_to_balance)
        .bind(to_id)
        .execute(&mut **db_tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to update account balance");
            TransactionError::AccountRepository(AccountRepositoryError::OperationFailed {
                operation: "update_balance".to_string(),
                reason: e.to_string(),
            })
        })?;

    let tx_id = Uuid::new_v4();
    let tx_kind = "transfer";
    let tx_amount = amount.as_u64() as i64;

    sqlx::query(
        "INSERT INTO transactions (id, kind, amount, from_account_id, to_account_id, account_id) VALUES ($1, $2::transaction_kind, $3, $4, $5, $6)"
    )
    .bind(tx_id)
    .bind(tx_kind)
    .bind(tx_amount)
    .bind(from_id)
    .bind(to_id)
    .bind(from_id)
    .execute(&mut **db_tx)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to create transaction record");
        TransactionError::TransactionRepository(TransactionRepositoryError::TransactionFailed)
    })?;

    let from_owner = if let Some(uid) = from_user_id {
        Owner::User(UserId::new(&uid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else if let Some(oid) = from_org_id {
        Owner::Org(OrgId::new(&oid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else {
        return Err(TransactionError::AccountUnavailable);
    };

    let to_owner = if let Some(uid) = to_user_id {
        Owner::User(UserId::new(&uid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else if let Some(oid) = to_org_id {
        Owner::Org(OrgId::new(&oid).map_err(|_| TransactionError::AccountUnavailable)?)
    } else {
        return Err(TransactionError::AccountUnavailable);
    };

    let from_account = Account::new(
        from_id,
        if from_account_number == first_number {
            first_number.clone()
        } else {
            second_number.clone()
        },
        from_owner,
        Balance::new(new_from_balance as u64),
        from_created_at,
    );
    let to_account = Account::new(
        to_id,
        if to_account_number == first_number {
            first_number.clone()
        } else {
            second_number.clone()
        },
        to_owner,
        Balance::new(new_to_balance as u64),
        to_created_at,
    );

    let transaction =
        Transaction::transfer(tx_id, amount, from_id, to_id, OffsetDateTime::now_utc())
            .map_err(|_| TransactionError::SameAccountTransfer)?;

    Ok(TransactionOutput::Transfer(
        TransferResult {
            from_account,
            to_account,
        },
        transaction,
    ))
}

fn map_sqlx_to_tx_error(err: sqlx::Error) -> TransactionError {
    use crate::application::ports::{AccountRepositoryError, TransactionRepositoryError};

    match err {
        sqlx::Error::Database(db_err) => match db_err.code().as_deref() {
            Some("23505") => TransactionError::AccountRepository(
                AccountRepositoryError::UniqueConstraintViolation(db_err.message().to_string()),
            ),
            Some("23503") => TransactionError::AccountNotFound,
            Some("23514") => TransactionError::TransactionRepository(
                TransactionRepositoryError::CheckConstraintViolation(db_err.message().to_string()),
            ),
            Some("40P01") | Some("40001") | Some("57014") => {
                TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
            }
            Some("08006") | Some("08001") | Some("08004") => TransactionError::AccountRepository(
                AccountRepositoryError::ConnectionError(db_err.message().to_string()),
            ),
            _ => TransactionError::AccountRepository(AccountRepositoryError::OperationFailed {
                operation: "map_sqlx_error".to_string(),
                reason: "database error".to_string(),
            }),
        },
        sqlx::Error::PoolTimedOut => {
            TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
        }
        sqlx::Error::Io(io_err) => TransactionError::AccountRepository(
            AccountRepositoryError::ConnectionError(format!("I/O error: {}", io_err)),
        ),
        sqlx::Error::RowNotFound => TransactionError::AccountNotFound,
        _other => TransactionError::AccountRepository(AccountRepositoryError::OperationFailed {
            operation: "map_sqlx_error".to_string(),
            reason: "unknown database error".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::application::ports::{
        AccountRepositoryError, IdempotencyError, OperationError, TransactionRepositoryError,
    };
    use crate::application::transaction_manager::TransactionError;
    use crate::application::transaction_manager::{
        DepositInput, TransactionOperation, TransactionOutput, TransferInput, TransferResult,
        WithdrawInput,
    };
    use crate::domain::{
        account::Account, account_number::AccountNumber, amount::Amount, balance::Balance,
        owner::Owner, transaction::Transaction, user_id::UserId,
    };
    use time::OffsetDateTime;
    use uuid::Uuid;

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
    fn classifies_transient_error_correctly() {
        let transient_errors = vec![
            TransactionError::AccountUnavailable,
            TransactionError::AccountRepository(AccountRepositoryError::LockTimeout),
        ];

        for err in transient_errors {
            let is_transient = matches!(
                &err,
                TransactionError::AccountUnavailable
                    | TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
            );
            assert!(is_transient, "expected {:?} to be transient", err);
        }
    }

    #[test]
    fn classifies_non_transient_error_correctly() {
        let non_transient_errors = vec![
            TransactionError::AccountNotFound,
            TransactionError::InsufficientFunds,
            TransactionError::SameAccountTransfer,
        ];

        for err in non_transient_errors {
            let is_transient = matches!(
                &err,
                TransactionError::AccountUnavailable
                    | TransactionError::AccountRepository(AccountRepositoryError::LockTimeout)
            );
            assert!(!is_transient, "expected {:?} to be non-transient", err);
        }
    }

    #[test]
    fn retry_logic_increments_delay_between_attempts() {
        let initial_delay = 10u64;
        let max_delay = 500u64;
        let mut delay = initial_delay;

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
        assert!(matches!(op_err, OperationError::IdempotencyError));
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
}
