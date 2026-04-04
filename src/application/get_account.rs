use std::sync::Arc;

use thiserror::Error;

use crate::{
    application::ports::{AccountRepository, AccountRepositoryError},
    domain::{account::Account, account_number::AccountNumber},
};

pub struct GetAccountInput {
    pub account_number: AccountNumber,
}

#[derive(Error, Debug)]
pub enum GetAccountError {
    #[error("account not found")]
    AccountNotFound,
    #[error("account repository error")]
    AccountRepository(#[from] AccountRepositoryError),
}

pub struct GetAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
}

impl GetAccountUseCase {
    pub fn new(account_repository: Arc<dyn AccountRepository + Send + Sync>) -> Self {
        Self { account_repository }
    }

    pub async fn execute(&self, input: GetAccountInput) -> Result<Account, GetAccountError> {
        let GetAccountInput { account_number } = input;
        let account = self
            .account_repository
            .find_by_number(&account_number)
            .await?
            .ok_or(GetAccountError::AccountNotFound)?;

        Ok(account)
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
        found_account: Mutex<Option<Account>>,
        find_result: Result<(), AccountRepositoryError>,
    }

    #[async_trait]
    impl AccountRepository for FakeAccountRepository {
        async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
            unimplemented!("count_by_owner is not used in GetAccount tests")
        }

        async fn create(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            unimplemented!("create is not used in GetAccount tests")
        }

        async fn find_by_number(
            &self,
            _number: &AccountNumber,
        ) -> Result<Option<Account>, AccountRepositoryError> {
            self.find_result.clone()?;
            Ok(self.found_account.lock().unwrap().clone())
        }

        async fn update(&self, _account: &Account) -> Result<(), AccountRepositoryError> {
            unimplemented!("update is not used in GetAccount tests")
        }

        async fn find_by_owner(
            &self,
            _owner: &Owner,
        ) -> Result<Vec<Account>, AccountRepositoryError> {
            unimplemented!("find_by_owner is not used in this test")
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

    fn make_input() -> GetAccountInput {
        GetAccountInput {
            account_number: AccountNumber::new("acc-1").unwrap(),
        }
    }

    #[tokio::test]
    async fn returns_account_when_found() {
        let expected_account = make_account();

        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(expected_account.clone())),
            find_result: Ok(()),
        });

        let use_case = GetAccountUseCase::new(repo);

        let account = use_case.execute(make_input()).await.unwrap();

        assert_eq!(account.id(), expected_account.id());
        assert_eq!(account.number(), expected_account.number());
        assert_eq!(account.owner(), expected_account.owner());
        assert_eq!(
            account.balance().as_u64(),
            expected_account.balance().as_u64()
        );
        assert_eq!(account.created_at(), expected_account.created_at());
    }

    #[tokio::test]
    async fn returns_account_not_found_when_missing() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(None),
            find_result: Ok(()),
        });

        let use_case = GetAccountUseCase::new(repo);

        let result = use_case.execute(make_input()).await;

        assert!(matches!(result, Err(GetAccountError::AccountNotFound)));
    }

    #[tokio::test]
    async fn returns_repository_error_when_find_fails() {
        let repo = Arc::new(FakeAccountRepository {
            found_account: Mutex::new(Some(make_account())),
            find_result: Err(AccountRepositoryError::OperationFailed),
        });

        let use_case = GetAccountUseCase::new(repo);

        let result = use_case.execute(make_input()).await;

        assert!(matches!(
            result,
            Err(GetAccountError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));
    }
}
