use std::sync::Arc;

use crate::{
    application::ports::{AccountRepository, BalanceCachePort, OperationError},
    domain::{account::Account, account_number::AccountNumber},
};

pub struct GetAccountInput {
    pub account_number: AccountNumber,
}

pub struct GetAccountUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl GetAccountUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        balance_cache: Arc<dyn BalanceCachePort>,
    ) -> Self {
        Self {
            account_repository,
            balance_cache,
        }
    }

    pub async fn execute(&self, input: GetAccountInput) -> Result<Account, OperationError> {
        let GetAccountInput { account_number } = input;

        let account = self
            .account_repository
            .find_by_number(&account_number)
            .await?
            .ok_or(OperationError::NotFound {
                resource: "account".to_string(),
            })?;

        self.balance_cache
            .set(account.id(), account.balance().as_u64())
            .await;

        Ok(account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::domain::{
        account::Account, account_number::AccountNumber, balance::Balance, owner::Owner,
        user_id::UserId,
    };

    use crate::test_utils::mocks::MockAccountRepository;
    use crate::test_utils::redis_setup::setup_redis;

    use time::OffsetDateTime;

    use uuid::Uuid;

    fn make_account(balance: u64) -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new("ACC001").unwrap(),
            Owner::User(UserId::new("user-1").unwrap()),
            Balance::new(balance),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    #[tokio::test]
    async fn returns_account_when_found() {
        let account = make_account(1000);
        let repo = Arc::new(MockAccountRepository::new().with_account(account.clone()));

        let (cache, _container): (_, _) = setup_redis().await;
        let use_case = GetAccountUseCase::new(repo, Arc::new(cache));

        let result = use_case
            .execute(GetAccountInput {
                account_number: AccountNumber::new("ACC001").unwrap(),
            })
            .await;

        assert!(result.is_ok());
        let returned_account = result.unwrap();
        assert_eq!(returned_account.number().as_str(), "ACC001");
        assert_eq!(returned_account.balance().as_u64(), 1000);
    }

    #[tokio::test]
    async fn returns_error_when_not_found() {
        let repo = Arc::new(MockAccountRepository::new());

        let (cache, _container): (_, _) = setup_redis().await;
        let use_case = GetAccountUseCase::new(repo, Arc::new(cache));

        let result = use_case
            .execute(GetAccountInput {
                account_number: AccountNumber::new("ACC001").unwrap(),
            })
            .await;

        assert!(
            matches!(result, Err(OperationError::NotFound { resource } ) if resource == "account")
        );
    }

    #[tokio::test]
    async fn returns_cached_balance_on_cache_hit() {
        let account = make_account(1000);
        let account_id = account.id();
        let repo = Arc::new(MockAccountRepository::new().with_account(account));

        let (cache, _container): (_, _) = setup_redis().await;
        BalanceCachePort::set(&cache, account_id, 5000).await;

        let use_case = GetAccountUseCase::new(repo, Arc::new(cache));

        let result = use_case
            .execute(GetAccountInput {
                account_number: AccountNumber::new("ACC001").unwrap(),
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().balance().as_u64(), 5000);
    }

    #[tokio::test]
    async fn populates_cache_on_cache_miss() {
        let account = make_account(2000);
        let account_id = account.id();
        let repo = Arc::new(MockAccountRepository::new().with_account(account));

        let (cache, _container): (_, _) = setup_redis().await;
        let cache_arc: Arc<dyn BalanceCachePort> = Arc::new(cache);
        let use_case = GetAccountUseCase::new(repo, cache_arc.clone());

        let _: Result<_, _> = use_case
            .execute(GetAccountInput {
                account_number: AccountNumber::new("ACC001").unwrap(),
            })
            .await;

        let cached_balance = BalanceCachePort::get(cache_arc.as_ref(), &account_id).await;
        assert_eq!(cached_balance, Some(2000));
    }
}
