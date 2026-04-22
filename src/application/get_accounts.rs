use std::sync::Arc;

use crate::{
    application::ports::{AccountRepository, BalanceCachePort, OperationError},
    domain::{account::Account, owner::Owner},
};

pub struct GetAccountsInput {
    pub owner: Owner,
}

pub struct GetAccountsUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    balance_cache: Arc<dyn BalanceCachePort>,
}

impl GetAccountsUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        balance_cache: Arc<dyn BalanceCachePort>,
    ) -> Self {
        Self {
            account_repository,
            balance_cache,
        }
    }

    pub async fn execute(&self, input: GetAccountsInput) -> Result<Vec<Account>, OperationError> {
        let GetAccountsInput { owner } = input;
        let accounts = self.account_repository.find_by_owner(&owner).await?;

        for account in &accounts {
            self.balance_cache
                .set(account.id(), account.balance().as_u64())
                .await;
        }

        Ok(accounts)
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

    fn make_account(account_number: &str, balance: u64) -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new(account_number).unwrap(),
            Owner::User(UserId::new("user-1").unwrap()),
            Balance::new(balance),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    #[tokio::test]
    async fn returns_accounts_for_owner() {
        let accounts = vec![make_account("ACC001", 100), make_account("ACC002", 200)];

        let repo = Arc::new(MockAccountRepository::new().with_accounts(accounts));

        let (cache, _container) = setup_redis().await;
        let use_case = GetAccountsUseCase::new(repo, Arc::new(cache));

        let result = use_case
            .execute(GetAccountsInput {
                owner: Owner::User(UserId::new("user-1").unwrap()),
            })
            .await;

        assert!(result.is_ok());
        let returned_accounts = result.unwrap();
        assert_eq!(returned_accounts.len(), 2);
        assert_eq!(returned_accounts[0].balance().as_u64(), 100);
        assert_eq!(returned_accounts[1].balance().as_u64(), 200);
    }

    #[tokio::test]
    async fn populates_cache_on_miss() {
        let account1 = make_account("ACC001", 300);
        let account2 = make_account("ACC002", 400);
        let id1 = account1.id();
        let id2 = account2.id();

        let repo = Arc::new(MockAccountRepository::new().with_accounts(vec![account1, account2]));

        let (cache, _container): (_, _) = setup_redis().await;
        let cache_arc: Arc<dyn BalanceCachePort> = Arc::new(cache);
        let use_case = GetAccountsUseCase::new(repo, cache_arc.clone());

        let _ = use_case
            .execute(GetAccountsInput {
                owner: Owner::User(UserId::new("user-1").unwrap()),
            })
            .await;

        assert_eq!(
            BalanceCachePort::get(cache_arc.as_ref(), &id1).await,
            Some(300)
        );
        assert_eq!(
            BalanceCachePort::get(cache_arc.as_ref(), &id2).await,
            Some(400)
        );
    }
}
