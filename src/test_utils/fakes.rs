use std::collections::HashMap;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    application::ports::{
        AccountRepositoryError, AccountTxRepository, EventPublishError, EventPublisher,
        MetricsPort, Transaction as TxTrait, TransactionError, TransactionPort,
        TransactionRepositoryError, TransactionWriteRepository,
    },
    domain::{
        account::Account, balance::Balance, transaction::Transaction,
        transaction_event::TransactionEvent,
    },
};

pub struct InMemoryAccountTxRepository {
    accounts_by_number: RwLock<HashMap<String, Account>>,
    accounts_by_id: RwLock<HashMap<Uuid, Account>>,
}

impl InMemoryAccountTxRepository {
    pub fn new() -> Self {
        Self {
            accounts_by_number: RwLock::new(HashMap::new()),
            accounts_by_id: RwLock::new(HashMap::new()),
        }
    }

    pub async fn insert_account(&self, account: Account) {
        self.accounts_by_number
            .write()
            .await
            .insert(account.number().to_string(), account.clone());
        self.accounts_by_id
            .write()
            .await
            .insert(account.id(), account);
    }
}

impl Default for InMemoryAccountTxRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl AccountTxRepository<()> for InMemoryAccountTxRepository {
    async fn find_by_number_for_update(
        &self,
        _tx: &mut (),
        account_number: &str,
    ) -> Result<Option<Account>, AccountRepositoryError> {
        let accounts = self.accounts_by_number.read().await;
        Ok(accounts.get(account_number).cloned())
    }

    async fn update_balance(
        &self,
        _tx: &mut (),
        account_id: Uuid,
        balance: u64,
    ) -> Result<(), AccountRepositoryError> {
        let mut by_id = self.accounts_by_id.write().await;
        let mut by_number = self.accounts_by_number.write().await;

        if let Some(account) = by_id.get(&account_id) {
            let updated = Account::new(
                account.id(),
                account.number().clone(),
                account.owner().clone(),
                Balance::new(balance),
                account.created_at(),
            );
            by_number.insert(updated.number().to_string(), updated.clone());
            by_id.insert(account_id, updated);
            Ok(())
        } else {
            Err(AccountRepositoryError::OperationFailed {
                operation: "update_balance".to_string(),
                reason: "account not found".to_string(),
            })
        }
    }

    async fn lock_for_update_by_numbers(
        &self,
        _tx: &mut (),
        first_number: &str,
        second_number: &str,
    ) -> Result<Vec<Account>, AccountRepositoryError> {
        let accounts = self.accounts_by_number.read().await;
        let mut result = Vec::new();
        if let Some(a) = accounts.get(first_number) {
            result.push(a.clone());
        }
        if let Some(a) = accounts.get(second_number) {
            result.push(a.clone());
        }
        let mut sorted = result;
        sorted.sort_by(|a, b| a.number().as_str().cmp(b.number().as_str()));
        Ok(sorted)
    }
}

pub struct InMemoryTransactionWriteRepository {
    pub transactions: RwLock<Vec<Transaction>>,
}

impl InMemoryTransactionWriteRepository {
    pub fn new() -> Self {
        Self {
            transactions: RwLock::new(Vec::new()),
        }
    }
}

impl Default for InMemoryTransactionWriteRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TransactionWriteRepository<()> for InMemoryTransactionWriteRepository {
    async fn create(
        &self,
        _tx: &mut (),
        transaction: &Transaction,
    ) -> Result<(), TransactionRepositoryError> {
        self.transactions.write().await.push(transaction.clone());
        Ok(())
    }
}

pub struct NoOpEventPublisher;

#[async_trait::async_trait]
impl EventPublisher for NoOpEventPublisher {
    async fn publish(
        &self,
        _topic: &str,
        _event: &TransactionEvent,
    ) -> Result<(), EventPublishError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoOpMetrics;

impl MetricsPort for NoOpMetrics {
    fn increment_operation(&self, _operation: &str, _status: &str) {}
    fn record_error(&self, _error_type: &str, _operation: &str) {}
}

#[derive(Clone)]
pub struct FakeTransactionPort;

impl TransactionPort for FakeTransactionPort {
    type Transaction = ();

    fn begin(&self) -> impl std::future::Future<Output = Result<(), TransactionError>> + Send {
        std::future::ready(Ok(()))
    }
}

impl TxTrait for () {
    fn commit(self) -> impl std::future::Future<Output = Result<(), TransactionError>> + Send {
        std::future::ready(Ok(()))
    }

    fn rollback(self) -> impl std::future::Future<Output = Result<(), TransactionError>> + Send {
        std::future::ready(Ok(()))
    }
}
