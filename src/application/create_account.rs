use std::sync::Arc;

use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::ports::{
        AccountNumberGenerator, AccountRepository, OperationError, OwnerTierRepository,
    },
    domain::{account::Account, balance::Balance, owner::Owner},
};

pub struct CreateAccountInput {
    pub owner: Owner,
}

pub struct CreateAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    account_number_generator: Arc<dyn AccountNumberGenerator + Send + Sync>,
    owner_tier_repository: Arc<dyn OwnerTierRepository + Send + Sync>,
}

impl CreateAccountUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        account_number_generator: Arc<dyn AccountNumberGenerator + Send + Sync>,
        owner_tier_repository: Arc<dyn OwnerTierRepository + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            account_number_generator,
            owner_tier_repository,
        }
    }

    pub async fn execute(&self, input: CreateAccountInput) -> Result<Account, OperationError> {
        let CreateAccountInput { owner } = input;

        let owner_tier = self.owner_tier_repository.get_or_default(&owner).await?;

        let tier = owner_tier.tier();
        let limit = tier.account_limit();

        let number = self.account_number_generator.generate().await?;
        let id = Uuid::new_v4();
        let balance = Balance::zero();
        let created_at = OffsetDateTime::now_utc();
        let account = Account::new(id, number, owner, balance, created_at);

        self.account_repository
            .create_within_limit(&account, limit)
            .await?;

        Ok(account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::application::ports::{
        AccountNumberGeneratorError, AccountRepositoryError, RepositoryOperation,
    };

    use crate::domain::{owner::Owner, tier::Tier, user_id::UserId};

    use crate::test_utils::mocks::{
        MockAccountNumberGenerator, MockAccountRepository, MockOwnerTierRepository,
    };

    #[tokio::test]
    async fn creates_account_when_limit_not_exceeded() {
        let repo = Arc::new(
            MockAccountRepository::new()
                .with_count_result(Ok(0))
                .with_create_result(Ok(())),
        );
        let generator = Arc::new(MockAccountNumberGenerator::new());
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Basic));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        let accounts: tokio::sync::MutexGuard<Vec<Account>> = repo.accounts.lock().await;
        assert_eq!(accounts.len(), 1);
    }

    #[tokio::test]
    async fn returns_error_when_limit_exceeded() {
        let repo = Arc::new(
            MockAccountRepository::new()
                .with_count_result(Ok(5))
                .with_create_result(Ok(())),
        );
        let generator = Arc::new(MockAccountNumberGenerator::new());
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Basic));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(matches!(result, Err(OperationError::TierLimitExceeded)));
        let accounts: tokio::sync::MutexGuard<Vec<Account>> = repo.accounts.lock().await;
        assert!(accounts.is_empty());
    }

    #[tokio::test]
    async fn returns_error_when_count_query_fails() {
        let repo = Arc::new(MockAccountRepository::new().with_count_result(Err(
            AccountRepositoryError::OperationFailed {
                operation: "count_by_owner".to_string(),
                reason: "test failure".to_string(),
            },
        )));
        let generator = Arc::new(MockAccountNumberGenerator::new());
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Basic));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(matches!(
            result,
            Err(OperationError::RepositoryError { operation, reason: _ })
            if operation == RepositoryOperation::CountByOwner
        ));
    }

    #[tokio::test]
    async fn returns_error_when_create_fails() {
        let repo = Arc::new(
            MockAccountRepository::new()
                .with_count_result(Ok(0))
                .with_create_result(Err(AccountRepositoryError::OperationFailed {
                    operation: "create".to_string(),
                    reason: "test failure".to_string(),
                })),
        );
        let generator = Arc::new(MockAccountNumberGenerator::new());
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Basic));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(matches!(
            result,
            Err(OperationError::RepositoryError { operation, reason: _ })
            if operation == RepositoryOperation::CreateAccount
        ));
    }

    #[tokio::test]
    async fn returns_error_when_generator_fails() {
        let repo = Arc::new(
            MockAccountRepository::new()
                .with_count_result(Ok(0))
                .with_create_result(Ok(())),
        );
        let generator = Arc::new(
            MockAccountNumberGenerator::new()
                .with_result(Err(AccountNumberGeneratorError::GenerationFailed)),
        );
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Basic));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(matches!(
            result,
            Err(OperationError::RepositoryError { operation, reason: _ })
            if operation == RepositoryOperation::GenerateAccountNumber
        ));
        let accounts: tokio::sync::MutexGuard<Vec<Account>> = repo.accounts.lock().await;
        assert!(accounts.is_empty());
    }

    #[tokio::test]
    async fn creates_account_with_premium_tier() {
        let repo = Arc::new(
            MockAccountRepository::new()
                .with_count_result(Ok(2))
                .with_create_result(Ok(())),
        );
        let generator = Arc::new(MockAccountNumberGenerator::new());
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Premium));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        let accounts: tokio::sync::MutexGuard<Vec<Account>> = repo.accounts.lock().await;
        assert_eq!(accounts.len(), 1);
    }

    #[tokio::test]
    async fn respects_premium_limit() {
        let repo = Arc::new(
            MockAccountRepository::new()
                .with_count_result(Ok(3))
                .with_create_result(Ok(())),
        );
        let generator = Arc::new(MockAccountNumberGenerator::new());
        let tier_repo = Arc::new(MockOwnerTierRepository::new(Tier::Premium));

        let use_case = CreateAccountUseCase::new(repo.clone(), generator, tier_repo);

        let input = CreateAccountInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
        };

        let result = use_case.execute(input).await;

        assert!(matches!(result, Err(OperationError::TierLimitExceeded)));
        let accounts: tokio::sync::MutexGuard<Vec<Account>> = repo.accounts.lock().await;
        assert!(accounts.is_empty());
    }
}
