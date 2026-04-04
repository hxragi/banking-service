use std::sync::Arc;

use thiserror::Error;

use crate::{
    application::ports::{
        AccountRepository, AccountRepositoryError, TransactionRepository,
        TransactionRepositoryError,
    },
    domain::{account_number::AccountNumber, transaction::Transaction},
};

pub struct GetTransactionsInput {
    pub account_number: AccountNumber,
}

#[derive(Error, Debug)]
pub enum GetTransactionsError {
    #[error("account not found")]
    AccountNotFound,
    #[error("account repository error")]
    AccountRepository(#[from] AccountRepositoryError),
    #[error("transaction repository error")]
    TransactionRepository(#[from] TransactionRepositoryError),
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

    pub async fn execute(
        &self,
        input: GetTransactionsInput,
    ) -> Result<Vec<Transaction>, GetTransactionsError> {
        let GetTransactionsInput { account_number } = input;

        let account = self
            .account_repository
            .find_by_number(&account_number)
            .await?
            .ok_or(GetTransactionsError::AccountNotFound)?;

        let transactions = self
            .transaction_repository
            .find_by_account_id(account.id())
            .await?;

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
        TransactionRepositoryError,
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
            unimplemented!("count_by_owner is not used in GetTransactions tests")
        }

        async fn create(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            unimplemented!("create is not used in GetTransactions tests")
        }

        async fn find_by_number(
            &self,
            _number: &AccountNumber,
        ) -> Result<Option<Account>, AccountRepositoryError> {
            self.find_result.clone()?;
            Ok(self.found_account.lock().unwrap().clone())
        }

        async fn update(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            unimplemented!("update is not used in GetTransactions tests")
        }

        async fn find_by_owner(
            &self,
            _owner: &Owner,
        ) -> Result<Vec<Account>, AccountRepositoryError> {
            unimplemented!("find_by_owner is not used in GetTransactions tests")
        }
    }

    struct FakeTransactionRepository {
        transactions: Mutex<Vec<Transaction>>,
        find_result: Result<(), TransactionRepositoryError>,
    }

    #[async_trait]
    impl TransactionRepository for FakeTransactionRepository {
        async fn create(
            &self,
            _transaction: &Transaction,
        ) -> Result<(), TransactionRepositoryError> {
            unimplemented!("create is not used in GetTransactions tests")
        }
    }

    fn make_owner() -> Owner {
        Owner::User(UserId::new("user-1").unwrap())
    }

    fn make_account() -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new("acc-1").unwrap(),
            make_owner(),
            Balance::new(100),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn make_transaction_for(account_id: Uuid, amount: u64) -> Transaction {
        Transaction::deposit(
            Uuid::new_v4(),
            Amount::new(amount).unwrap(),
            account_id,
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn make_input() -> GetTransactionsInput {
        GetTransactionsInput {
            account_number: AccountNumber::new("acc-1").unwrap(),
        }
    }

    #[tokio::test]
    async fn returns_transactions_when_account_exists() {
        let account = make_account();

        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(account.clone())),
            find_result: Ok(()),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            transactions: Mutex::new(vec![
                make_transaction_for(account.id(), 50),
                make_transaction_for(account.id(), 75),
            ]),
            find_result: Ok(()),
        });

        let use_case = GetTransactionsUseCase::new(repo, tx_repo);

        let transactions = use_case.execute(make_input()).await.unwrap();

        assert_eq!(transactions.len(), 2);
        assert_eq!(transactions[0].amount().as_u64(), 50);
        assert_eq!(transactions[1].amount().as_u64(), 75);
    }

    #[tokio::test]
    async fn returns_empty_list_when_account_has_no_transactions() {
        let account = make_account();

        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(account)),
            find_result: Ok(()),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            transactions: Mutex::new(vec![]),
            find_result: Ok(()),
        });

        let use_case = GetTransactionsUseCase::new(repo, tx_repo);

        let transactions = use_case.execute(make_input()).await.unwrap();

        assert!(transactions.is_empty());
    }

    #[tokio::test]
    async fn returns_account_not_found_when_account_missing() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(None),
            find_result: Ok(()),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            transactions: Mutex::new(vec![]),
            find_result: Ok(()),
        });

        let use_case = GetTransactionsUseCase::new(repo, tx_repo);

        let result = use_case.execute(make_input()).await;

        assert!(matches!(result, Err(GetTransactionsError::AccountNotFound)));
    }

    #[tokio::test]
    async fn returns_transaction_repository_error_when_find_fails() {
        let account = make_account();

        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(account)),
            find_result: Ok(()),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            transactions: Mutex::new(vec![]),
            find_result: Err(TransactionRepositoryError::TransactionFailed),
        });

        let use_case = GetTransactionsUseCase::new(repo, tx_repo);

        let result = use_case.execute(make_input()).await;

        assert!(matches!(
            result,
            Err(GetTransactionsError::TransactionRepository(
                TransactionRepositoryError::TransactionFailed
            ))
        ));
    }
}
