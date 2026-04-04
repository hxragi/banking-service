use std::sync::Arc;

use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{
        AccountRepository, AccountRepositoryError, TransactionRepository,
        TransactionRepositoryError,
    },
    domain::{
        account::Account, account_number::AccountNumber, amount::Amount, transaction::Transaction,
    },
};

pub struct DepositInput {
    pub account_number: AccountNumber,
    pub amount: Amount,
}

#[derive(Error, Debug)]
pub enum DepositError {
    #[error("account not found")]
    AccountNotFound,
    #[error("account repository error")]
    AccountRepository(#[from] AccountRepositoryError),
    #[error("transaction repository error")]
    TransactionRepository(#[from] TransactionRepositoryError),
}

pub struct DepositUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
}

impl DepositUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            transaction_repository,
        }
    }

    pub async fn execute(&self, input: DepositInput) -> Result<Account, DepositError> {
        let DepositInput {
            account_number,
            amount,
        } = input;
        let account = self
            .account_repository
            .find_by_number(&account_number)
            .await?
            .ok_or(DepositError::AccountNotFound)?;

        let updated_account = account.deposit(amount);
        let transaction = Transaction::deposit(
            Uuid::new_v4(),
            amount,
            updated_account.id(),
            OffsetDateTime::now_utc(),
        );
        self.account_repository.update(&updated_account).await?;
        self.transaction_repository.create(&transaction).await?;

        Ok(updated_account)
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
        update_result: Result<(), AccountRepositoryError>,
        updated_accounts: Mutex<Vec<Account>>,
    }

    #[async_trait]
    impl AccountRepository for FakeAccountRepository {
        async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
            Ok(0)
        }

        async fn create(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            Ok(())
        }

        async fn find_by_number(
            &self,
            _number: &AccountNumber,
        ) -> Result<Option<Account>, AccountRepositoryError> {
            self.find_result.clone()?;
            Ok(self.found_account.lock().unwrap().clone())
        }

        async fn update(&self, account: &Account) -> Result<(), AccountRepositoryError> {
            self.update_result.clone()?;
            self.updated_accounts.lock().unwrap().push(account.clone());
            Ok(())
        }

        async fn find_by_owner(
            &self,
            _owner: &Owner,
        ) -> Result<Vec<Account>, AccountRepositoryError> {
            unimplemented!("find_by_owner is not used in this test")
        }

        async fn find_by_account_id(
            &self,
            _account_id: Uuid,
        ) -> Result<Vec<Transaction>, TransactionRepositoryError> {
            unimplemented!("find_by_account_id is not used in this test")
        }
    }

    struct FakeTransactionRepository {
        create_result: Result<(), TransactionRepositoryError>,
        created_transactions: Mutex<Vec<Transaction>>,
    }

    #[async_trait]
    impl TransactionRepository for FakeTransactionRepository {
        async fn create(
            &self,
            transaction: &Transaction,
        ) -> Result<(), TransactionRepositoryError> {
            self.create_result.clone()?;
            self.created_transactions
                .lock()
                .unwrap()
                .push(transaction.clone());
            Ok(())
        }
    }

    fn make_owner() -> Owner {
        Owner::User(UserId::new("user-1").unwrap())
    }

    fn make_account(balance: u64) -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new("acc-1").unwrap(),
            make_owner(),
            Balance::new(balance),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn make_input() -> DepositInput {
        DepositInput {
            account_number: AccountNumber::new("acc-1").unwrap(),
            amount: Amount::new(50).unwrap(),
        }
    }

    #[tokio::test]
    async fn deposit_updates_account_and_creates_transaction() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(make_account(100))),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = DepositUseCase::new(repo.clone(), tx_repo.clone());

        let updated_account = use_case.execute(make_input()).await.unwrap();

        assert_eq!(updated_account.balance().as_u64(), 150);

        let updated_accounts = repo.updated_accounts.lock().unwrap();
        assert_eq!(updated_accounts.len(), 1);
        assert_eq!(updated_accounts[0].balance().as_u64(), 150);

        let created_transactions = tx_repo.created_transactions.lock().unwrap();
        assert_eq!(created_transactions.len(), 1);
        assert_eq!(created_transactions[0].amount().as_u64(), 50);
    }

    #[tokio::test]
    async fn returns_account_not_found_when_account_missing() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(None),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = DepositUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case.execute(make_input()).await;

        assert!(matches!(result, Err(DepositError::AccountNotFound)));

        assert!(repo.updated_accounts.lock().unwrap().is_empty());
        assert!(tx_repo.created_transactions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn returns_account_repository_error_when_find_fails() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(make_account(100))),
            find_result: Err(AccountRepositoryError::OperationFailed),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = DepositUseCase::new(repo, tx_repo);

        let result = use_case.execute(make_input()).await;

        assert!(matches!(
            result,
            Err(DepositError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));
    }

    #[tokio::test]
    async fn returns_account_repository_error_when_update_fails() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(make_account(100))),
            find_result: Ok(()),
            update_result: Err(AccountRepositoryError::OperationFailed),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = DepositUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case.execute(make_input()).await;

        assert!(matches!(
            result,
            Err(DepositError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));

        assert!(tx_repo.created_transactions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn returns_transaction_repository_error_when_create_fails() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(make_account(100))),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Err(TransactionRepositoryError::TransactionFailed),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = DepositUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case.execute(make_input()).await;

        assert!(matches!(
            result,
            Err(DepositError::TransactionRepository(
                TransactionRepositoryError::TransactionFailed
            ))
        ));

        let updated_accounts = repo.updated_accounts.lock().unwrap();
        assert_eq!(updated_accounts.len(), 1);
        assert_eq!(updated_accounts[0].balance().as_u64(), 150);
    }
}
