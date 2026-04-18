use std::sync::Arc;

use crate::{
    application::ports::{
        AccountRepository, OperationError, PaginatedTransactions, TransactionRepository,
    },
    domain::account_number::AccountNumber,
};

#[derive(Debug)]
pub struct GetTransactionsInput {
    pub account_number: AccountNumber,
    pub page: u32,
    pub page_size: u32,
}

pub struct GetTransactionsUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
}

impl GetTransactionsUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            transaction_repository,
        }
    }

    #[tracing::instrument(
        skip(self),
        fields(account_number = %input.account_number, page = input.page, page_size = input.page_size)
    )]
    pub async fn execute(
        &self,
        input: GetTransactionsInput,
    ) -> Result<PaginatedTransactions, OperationError> {
        let GetTransactionsInput {
            account_number,
            page,
            page_size,
        } = input;

        let account = self
            .account_repository
            .find_by_number(&account_number)
            .await?
            .ok_or(OperationError::NotFound {
                resource: "account".to_string(),
            })?;

        let transactions = self
            .transaction_repository
            .find_by_account_id_paginated(account.id(), page, page_size)
            .await?;

        tracing::info!(
            account_number = %account_number,
            total_count = transactions.total_count,
            returned_count = transactions.transactions.len(),
            "transactions fetched"
        );

        Ok(transactions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    use crate::application::ports::{
        AccountRepository, AccountRepositoryError, TransactionRepository,
        TransactionRepositoryError, TransactionWithAccounts,
    };
    use crate::domain::{
        account::Account, account_number::AccountNumber, amount::Amount, balance::Balance,
        owner::Owner, transaction::Transaction, user_id::UserId,
    };
    use time::OffsetDateTime;
    use uuid::Uuid;

    struct FakeAccountRepository {
        found_account: Mutex<Option<Account>>,
        find_result: Result<(), AccountRepositoryError>,
    }

    #[async_trait]
    impl AccountRepository for FakeAccountRepository {
        async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
            Err(AccountRepositoryError::OperationFailed {
                operation: "count_by_owner".to_string(),
                reason: "not implemented in test".to_string(),
            })
        }

        async fn create(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            Err(AccountRepositoryError::OperationFailed {
                operation: "create".to_string(),
                reason: "not implemented in test".to_string(),
            })
        }

        async fn find_by_number(
            &self,
            _number: &AccountNumber,
        ) -> Result<Option<Account>, AccountRepositoryError> {
            self.find_result.clone()?;
            Ok(self.found_account.lock().unwrap().clone())
        }

        async fn find_by_owner(
            &self,
            _owner: &Owner,
        ) -> Result<Vec<Account>, AccountRepositoryError> {
            Err(AccountRepositoryError::OperationFailed {
                operation: "find_by_owner".to_string(),
                reason: "not implemented in test".to_string(),
            })
        }
    }

    struct FakeTransactionRepository {
        transactions: Mutex<Vec<TransactionWithAccounts>>,
    }

    #[async_trait]
    impl TransactionRepository for FakeTransactionRepository {
        async fn find_by_account_id_paginated(
            &self,
            _account_id: Uuid,
            _page: u32,
            page_size: u32,
        ) -> Result<PaginatedTransactions, TransactionRepositoryError> {
            let transactions = self.transactions.lock().unwrap().clone();
            let total_count = transactions.len() as u64;
            Ok(PaginatedTransactions {
                transactions,
                total_count,
                page: 1,
                page_size,
                has_more: false,
            })
        }

        async fn count_by_account_id(
            &self,
            _account_id: Uuid,
        ) -> Result<u64, TransactionRepositoryError> {
            Err(TransactionRepositoryError::TransactionFailed)
        }
    }

    fn make_account() -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new("ACC001").unwrap(),
            Owner::User(UserId::new("user-1").unwrap()),
            Balance::new(100),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn make_transaction() -> TransactionWithAccounts {
        let transaction = Transaction::deposit(
            Uuid::new_v4(),
            Amount::new(50).unwrap(),
            Uuid::new_v4(),
            OffsetDateTime::UNIX_EPOCH,
        );
        TransactionWithAccounts {
            transaction,
            from_account_number: None,
            to_account_number: Some("ACC-TO-123".to_string()),
        }
    }

    #[tokio::test]
    async fn returns_transactions_for_account() {
        let account = make_account();
        let transactions = vec![make_transaction()];

        let account_repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(account)),
            find_result: Ok(()),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            transactions: Mutex::new(transactions),
        });

        let use_case = GetTransactionsUseCase::new(account_repo, tx_repo);

        let result = use_case
            .execute(GetTransactionsInput {
                account_number: AccountNumber::new("ACC001").unwrap(),
                page: 1,
                page_size: 10,
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().transactions.len(), 1);
    }

    #[tokio::test]
    async fn returns_account_not_found_when_missing() {
        let account_repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(None),
            find_result: Ok(()),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            transactions: Mutex::new(vec![]),
        });

        let use_case = GetTransactionsUseCase::new(account_repo, tx_repo);

        let result = use_case
            .execute(GetTransactionsInput {
                account_number: AccountNumber::new("ACC001").unwrap(),
                page: 1,
                page_size: 10,
            })
            .await;

        assert!(
            matches!(result, Err(OperationError::NotFound { resource }) if resource == "account")
        );
    }
}
