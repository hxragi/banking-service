use std::sync::Arc;

use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{
        AccountNumberGenerator, AccountNumberGeneratorError, AccountRepository,
        AccountRepositoryError,
    },
    domain::{
        account::Account,
        balance::Balance,
        owner::Owner,
        tier::{AccountLimit, Tier},
    },
};

pub struct CreateAccountInput {
    pub owner: Owner,
    pub tier: Tier,
}

#[derive(Error, Debug)]
pub enum CreateAccountError {
    #[error("tier limit exceeded")]
    TierLimitExceeded,
    #[error("account repository error")]
    AccountRepository(#[from] AccountRepositoryError),
    #[error("account number generator error")]
    AccountNumberGenerator(#[from] AccountNumberGeneratorError),
}

pub struct CreateAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    account_number_generator: Arc<dyn AccountNumberGenerator + Send + Sync>,
}

impl CreateAccountUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        account_number_generator: Arc<dyn AccountNumberGenerator + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            account_number_generator,
        }
    }

    pub async fn execute(&self, input: CreateAccountInput) -> Result<Account, CreateAccountError> {
        let CreateAccountInput { owner, tier } = input;

        let limit = tier.account_limit();
        let count = self.account_repository.count_by_owner(&owner).await?;

        if let AccountLimit::Limited(limit) = limit {
            if count >= limit {
                return Err(CreateAccountError::TierLimitExceeded);
            }
        }

        let number = self.account_number_generator.generate().await?;
        let id = Uuid::new_v4();
        let balance = Balance::zero();
        let created_at = OffsetDateTime::now_utc();
        let account = Account::new(id, number, owner, balance, created_at);

        self.account_repository.create(&account).await?;

        Ok(account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    use crate::application::ports::{
        AccountNumberGenerator, AccountNumberGeneratorError, AccountRepository,
        AccountRepositoryError,
    };
    use crate::domain::{
        account::Account, account_number::AccountNumber, owner::Owner, tier::Tier, user_id::UserId,
    };

    struct FakeAccountRepository {
        count_result: Result<u64, AccountRepositoryError>,
        create_result: Result<(), AccountRepositoryError>,
        saved_accounts: Mutex<Vec<Account>>,
    }

    #[async_trait]
    impl AccountRepository for FakeAccountRepository {
        async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
            self.count_result.clone()
        }

        async fn create(&self, account: &Account) -> Result<(), AccountRepositoryError> {
            if self.create_result.is_ok() {
                self.saved_accounts.lock().unwrap().push(account.clone());
            }

            self.create_result.clone()
        }
    }

    struct FakeAccountNumberGenerator {
        result: Result<AccountNumber, AccountNumberGeneratorError>,
    }

    #[async_trait]
    impl AccountNumberGenerator for FakeAccountNumberGenerator {
        async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError> {
            self.result.clone()
        }
    }

    fn make_owner() -> Owner {
        Owner::User(UserId::new("user-1").unwrap())
    }

    fn make_input(tier: Tier) -> CreateAccountInput {
        CreateAccountInput {
            owner: make_owner(),
            tier,
        }
    }

    #[tokio::test]
    async fn creates_account_when_under_limit() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(0),
            create_result: Ok(()),
            saved_accounts: Mutex::new(vec![]),
        });

        let generator = Arc::new(FakeAccountNumberGenerator {
            result: Ok(AccountNumber::new("acc-123").unwrap()),
        });

        let use_case = CreateAccountUseCase::new(repo.clone(), generator);

        let account = use_case.execute(make_input(Tier::Basic)).await.unwrap();

        assert_eq!(account.number().as_str(), "acc-123");
        assert_eq!(account.owner(), &make_owner());
        assert_eq!(account.balance().as_u64(), 0);

        let saved = repo.saved_accounts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].number().as_str(), "acc-123");
    }

    #[tokio::test]
    async fn returns_tier_limit_exceeded_when_limit_reached() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(1),
            create_result: Ok(()),
            saved_accounts: Mutex::new(vec![]),
        });

        let generator = Arc::new(FakeAccountNumberGenerator {
            result: Ok(AccountNumber::new("acc-123").unwrap()),
        });

        let use_case = CreateAccountUseCase::new(repo.clone(), generator);

        let result = use_case.execute(make_input(Tier::Basic)).await;

        assert!(matches!(result, Err(CreateAccountError::TierLimitExceeded)));

        let saved = repo.saved_accounts.lock().unwrap();
        assert!(saved.is_empty());
    }

    #[tokio::test]
    async fn returns_repository_error_when_count_by_owner_fails() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Err(AccountRepositoryError::OperationFailed),
            create_result: Ok(()),
            saved_accounts: Mutex::new(vec![]),
        });

        let generator = Arc::new(FakeAccountNumberGenerator {
            result: Ok(AccountNumber::new("acc-123").unwrap()),
        });

        let use_case = CreateAccountUseCase::new(repo, generator);

        let result = use_case.execute(make_input(Tier::Basic)).await;

        assert!(matches!(
            result,
            Err(CreateAccountError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));
    }

    #[tokio::test]
    async fn returns_generator_error_when_generation_fails() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(0),
            create_result: Ok(()),
            saved_accounts: Mutex::new(vec![]),
        });

        let generator = Arc::new(FakeAccountNumberGenerator {
            result: Err(AccountNumberGeneratorError::GenerationFailed),
        });

        let use_case = CreateAccountUseCase::new(repo, generator);

        let result = use_case.execute(make_input(Tier::Basic)).await;

        assert!(matches!(
            result,
            Err(CreateAccountError::AccountNumberGenerator(
                AccountNumberGeneratorError::GenerationFailed
            ))
        ));
    }

    #[tokio::test]
    async fn returns_repository_error_when_save_fails() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(0),
            create_result: Err(AccountRepositoryError::OperationFailed),
            saved_accounts: Mutex::new(vec![]),
        });

        let generator = Arc::new(FakeAccountNumberGenerator {
            result: Ok(AccountNumber::new("acc-123").unwrap()),
        });

        let use_case = CreateAccountUseCase::new(repo, generator);

        let result = use_case.execute(make_input(Tier::Basic)).await;

        assert!(matches!(
            result,
            Err(CreateAccountError::AccountRepository(
                AccountRepositoryError::OperationFailed
            ))
        ));
    }
}
