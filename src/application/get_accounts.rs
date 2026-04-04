use std::sync::Arc;

use thiserror::Error;

use crate::{
    application::ports::{AccountRepository, AccountRepositoryError},
    domain::{account::Account, owner::Owner},
};

pub struct GetAccountsInput {
    pub owner: Owner,
}

#[derive(Error, Debug)]
pub enum GetAccountsError {
    #[error("account repository error")]
    AccountRepository(#[from] AccountRepositoryError),
}

pub struct GetAccountsUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
}

impl GetAccountsUseCase {
    pub fn new(account_repository: Arc<dyn AccountRepository + Send + Sync>) -> Self {
        Self { account_repository }
    }

    pub async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, GetAccountsError> {
        let GetAccountsInput { owner } = input;
        let accounts = self.account_repository.find_by_owner(&owner).await?;
        Ok(accounts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    use crate::application::ports::{
        AccountRepository, AccountRepositoryError, TransactionRepositoryError,
    };
    use crate::domain::transaction::Transaction;
    use crate::domain::{
        account::Account, account_number::AccountNumber, balance::Balance, owner::Owner,
        user_id::UserId,
    };
    use time::OffsetDateTime;
    use uuid::Uuid;

    struct FakeAccountRepository {
        accounts: Mutex<Vec<Account>>,
        find_by_owner_result: Result<(), AccountRepositoryError>,
    }

    #[async_trait]
    impl AccountRepository for FakeAccountRepository {
        async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
            unimplemented!("count_by_owner is not used in GetAccounts tests")
        }

        async fn create(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            unimplemented!("create is not used in GetAccounts tests")
        }

        async fn find_by_number(
            &self,
            _number: &AccountNumber,
        ) -> Result<Option<Account>, AccountRepositoryError> {
            unimplemented!("find_by_number is not used in GetAccounts tests")
        }

        async fn update(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            unimplemented!("update is not used in GetAccounts tests")
        }

        async fn find_by_owner(
            &self,
            _owner: &Owner,
        ) -> Result<Vec<Account>, AccountRepositoryError> {
            self.find_by_owner_result.clone()?;
            Ok(self.accounts.lock().unwrap().clone())
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

    fn make_input() -> GetAccountsInput {
        GetAccountsInput {
            owner: make_owner(),
        }
    }

    #[tokio::test]
    async fn returns_accounts_when_found() {
        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![make_account("acc-1", 100), make_account("acc-2", 200)]),
            find_by_owner_result: Ok(()),
        });

        let use_case = GetAccountsUseCase::new(repo);

        let accounts = use_case.execute(make_input()).await.unwrap();

        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].number().as_str(), "acc-1");
        assert_eq!(accounts[1].number().as_str(), "acc-2");
    }

    #[tokio::test]
    async fn returns_empty_list_when_no_accounts_found() {
        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![]),
            find_by_owner_result: Ok(()),
        });

        let use_case = GetAccountsUseCase::new(repo);

        let accounts = use_case.execute(make_input()).await.unwrap();

        assert!(accounts.is_empty());
    }

    #[tokio::test]
    async fn returns_repository_error_when_find_by_owner_fails() {
        let repo = Arc::new(FakeAccountRepository {
            accounts: Mutex::new(vec![make_account("acc-1", 100)]),
            find_by_owner_result: Err(AccountRepositoryError::OperationFailed),
        });

        let use_case = GetAccountsUseCase::new(repo);

        let result = use_case.execute(make_input()).await;

        assert!(matches!(
            result,
            Err(GetAccountsError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));
    }
}
