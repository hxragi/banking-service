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
        account::Account, account_number::AccountNumber, amount::Amount, errors::DomainError,
        transaction::Transaction,
    },
};

pub struct TransferInput {
    pub from_account_number: AccountNumber,
    pub to_account_number: AccountNumber,
    pub amount: Amount,
}

pub struct TransferResult {
    pub from_account: Account,
    pub to_account: Account,
}

#[derive(Error, Debug)]
pub enum TransferError {
    #[error("from account not found")]
    FromAccountNotFound,
    #[error("to account not found")]
    ToAccountNotFound,
    #[error("same account transfer")]
    SameAccountTransfer,
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("account repository error")]
    AccountRepository(#[from] AccountRepositoryError),
    #[error("transaction repository error")]
    TransactionRepository(#[from] TransactionRepositoryError),
}

pub struct TransferUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
}

impl TransferUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        transaction_repository: Arc<dyn TransactionRepository + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            transaction_repository,
        }
    }

    pub async fn execute(&self, input: TransferInput) -> Result<TransferResult, TransferError> {
        let TransferInput {
            from_account_number,
            to_account_number,
            amount,
        } = input;

        if from_account_number == to_account_number {
            return Err(TransferError::SameAccountTransfer);
        }

        let from_account = self
            .account_repository
            .find_by_number(&from_account_number)
            .await?
            .ok_or(TransferError::FromAccountNotFound)?;

        let to_account = self
            .account_repository
            .find_by_number(&to_account_number)
            .await?
            .ok_or(TransferError::ToAccountNotFound)?;

        let updated_from_account = from_account.withdraw(amount).map_err(|err| match err {
            DomainError::InsufficientFunds => TransferError::InsufficientFunds,
            _ => unreachable!("unexpected domain error from account withdraw"),
        })?;
        let updated_to_account = to_account.deposit(amount);

        let transaction = Transaction::transfer(
            Uuid::new_v4(),
            amount,
            updated_from_account.id(),
            updated_to_account.id(),
            OffsetDateTime::now_utc(),
        )
        .map_err(|err| match err {
            DomainError::SameAccountTransfer => TransferError::SameAccountTransfer,
            _ => unreachable!("unexpected domain error from transaction transfer"),
        })?;

        self.account_repository
            .update(&updated_from_account)
            .await?;
        self.account_repository.update(&updated_to_account).await?;
        self.transaction_repository.create(&transaction).await?;

        Ok(TransferResult {
            from_account: updated_from_account,
            to_account: updated_to_account,
        })
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
        accounts: Mutex<Vec<Account>>,
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
            number: &AccountNumber,
        ) -> Result<Option<Account>, AccountRepositoryError> {
            self.find_result.clone()?;

            let accounts = self.accounts.lock().unwrap();
            Ok(accounts
                .iter()
                .find(|account| account.number() == number)
                .cloned())
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

        async fn find_by_account_id(
            &self,
            _account_id: Uuid,
        ) -> Result<Vec<Transaction>, TransactionRepositoryError> {
            unimplemented!("find_by_account_id is not used in this test")
        }
    }

    fn make_owner() -> Owner {
        Owner::User(UserId::new("user-1").unwrap())
    }

    fn make_account(number: &str, balance: u64) -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new(number).unwrap(),
            make_owner(),
            Balance::new(balance),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn make_input(from: &str, to: &str, amount: u64) -> TransferInput {
        TransferInput {
            from_account_number: AccountNumber::new(from).unwrap(),
            to_account_number: AccountNumber::new(to).unwrap(),
            amount: Amount::new(amount).unwrap(),
        }
    }

    #[tokio::test]
    async fn transfer_updates_both_accounts_and_creates_transaction() {
        let from_account = make_account("acc-from", 100);
        let to_account = make_account("acc-to", 20);

        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![from_account.clone(), to_account.clone()]),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case
            .execute(make_input("acc-from", "acc-to", 30))
            .await
            .unwrap();

        assert_eq!(result.from_account.balance().as_u64(), 70);
        assert_eq!(result.to_account.balance().as_u64(), 50);

        let updated_accounts = repo.updated_accounts.lock().unwrap();
        assert_eq!(updated_accounts.len(), 2);

        let created_transactions = tx_repo.created_transactions.lock().unwrap();
        assert_eq!(created_transactions.len(), 1);
        assert_eq!(created_transactions[0].amount().as_u64(), 30);
    }

    #[tokio::test]
    async fn returns_same_account_transfer_when_numbers_equal() {
        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![]),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case.execute(make_input("same", "same", 10)).await;

        assert!(matches!(result, Err(TransferError::SameAccountTransfer)));
        assert!(repo.updated_accounts.lock().unwrap().is_empty());
        assert!(tx_repo.created_transactions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn returns_from_account_not_found() {
        let to_account = make_account("acc-to", 20);

        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![to_account]),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo, tx_repo);

        let result = use_case.execute(make_input("acc-from", "acc-to", 30)).await;

        assert!(matches!(result, Err(TransferError::FromAccountNotFound)));
    }

    #[tokio::test]
    async fn returns_to_account_not_found() {
        let from_account = make_account("acc-from", 100);

        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![from_account]),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo, tx_repo);

        let result = use_case.execute(make_input("acc-from", "acc-to", 30)).await;

        assert!(matches!(result, Err(TransferError::ToAccountNotFound)));
    }

    #[tokio::test]
    async fn returns_insufficient_funds() {
        let from_account = make_account("acc-from", 10);
        let to_account = make_account("acc-to", 20);

        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![from_account, to_account]),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case.execute(make_input("acc-from", "acc-to", 30)).await;

        assert!(matches!(result, Err(TransferError::InsufficientFunds)));
        assert!(repo.updated_accounts.lock().unwrap().is_empty());
        assert!(tx_repo.created_transactions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn returns_account_repository_error_when_find_fails() {
        let from_account = make_account("acc-from", 100);
        let to_account = make_account("acc-to", 20);

        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![from_account, to_account]),
            find_result: Err(AccountRepositoryError::OperationFailed),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Ok(()),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo, tx_repo);

        let result = use_case.execute(make_input("acc-from", "acc-to", 30)).await;

        assert!(matches!(
            result,
            Err(TransferError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));
    }

    #[tokio::test]
    async fn returns_transaction_repository_error_when_create_fails() {
        let from_account = make_account("acc-from", 100);
        let to_account = make_account("acc-to", 20);

        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![from_account, to_account]),
            find_result: Ok(()),
            update_result: Ok(()),
            updated_accounts: Mutex::new(vec![]),
        });

        let tx_repo = Arc::new(FakeTransactionRepository {
            create_result: Err(TransactionRepositoryError::TransactionFailed),
            created_transactions: Mutex::new(vec![]),
        });

        let use_case = TransferUseCase::new(repo.clone(), tx_repo.clone());

        let result = use_case.execute(make_input("acc-from", "acc-to", 30)).await;

        assert!(matches!(
            result,
            Err(TransferError::TransactionRepository(
                TransactionRepositoryError::TransactionFailed
            ))
        ));

        let updated_accounts = repo.updated_accounts.lock().unwrap();
        assert_eq!(updated_accounts.len(), 2);
    }
}
