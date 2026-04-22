use std::sync::Arc;

use crate::{
    application::ports::{AccountRepository, OperationError, OwnerTierRepository},
    domain::{owner::Owner, tier::Tier},
};

pub struct ChangeTierInput {
    pub owner: Owner,
    pub new_tier: Tier,
}

pub struct ChangeTierUseCase {
    account_repository: Arc<dyn AccountRepository + Send + Sync>,
    owner_tier_repository: Arc<dyn OwnerTierRepository + Send + Sync>,
}

impl ChangeTierUseCase {
    pub fn new(
        account_repository: Arc<dyn AccountRepository + Send + Sync>,
        owner_tier_repository: Arc<dyn OwnerTierRepository + Send + Sync>,
    ) -> Self {
        Self {
            account_repository,
            owner_tier_repository,
        }
    }

    pub async fn execute(&self, input: ChangeTierInput) -> Result<Tier, OperationError> {
        let ChangeTierInput { owner, new_tier } = input;

        let account_count = self.account_repository.count_by_owner(&owner).await?;

        let new_limit = new_tier.account_limit();

        if let Some(limit) = new_limit
            && account_count > limit
        {
            return Err(OperationError::TierDowngradeNotAllowed {
                reason: "current account count exceeds new tier limit".to_string(),
            });
        }

        self.owner_tier_repository
            .set_tier(&owner, new_tier)
            .await?;

        Ok(new_tier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::application::{
        create_account::{CreateAccountInput, CreateAccountUseCase},
        ports::{AccountRepositoryError, OwnerTierRepositoryError, RepositoryOperation},
    };
    use crate::domain::{
        account::Account, account_number::AccountNumber, balance::Balance, owner_tier::OwnerTier,
        tier::Tier, user_id::UserId,
    };

    struct FakeAccountRepository {
        count_result: Result<u64, AccountRepositoryError>,
        accounts: Mutex<Vec<Account>>,
    }

    #[async_trait]
    impl AccountRepository for FakeAccountRepository {
        async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
            self.count_result.clone()
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
            Err(AccountRepositoryError::OperationFailed {
                operation: "find_by_number".to_string(),
                reason: "not implemented in test".to_string(),
            })
        }

        async fn find_by_owner(
            &self,
            _owner: &Owner,
        ) -> Result<Vec<Account>, AccountRepositoryError> {
            Ok(self.accounts.lock().unwrap().clone())
        }

        async fn create_within_limit(
            &self,
            account: &Account,
            limit: Option<u64>,
        ) -> Result<(), AccountRepositoryError> {
            let owner = account.owner();
            let count = self.count_by_owner(owner).await?;
            if let Some(limit) = limit
                && count >= limit
            {
                return Err(AccountRepositoryError::LimitExceeded);
            }
            self.create(account).await
        }
    }

    struct FakeOwnerTierRepository {
        stored_tier: Mutex<Option<Tier>>,
    }

    #[async_trait]
    impl OwnerTierRepository for FakeOwnerTierRepository {
        async fn get_or_default(
            &self,
            owner: &Owner,
        ) -> Result<OwnerTier, OwnerTierRepositoryError> {
            let tier = self.stored_tier.lock().unwrap().unwrap_or(Tier::Basic);
            Ok(OwnerTier::new(
                owner.clone(),
                tier,
                OffsetDateTime::now_utc(),
                OffsetDateTime::now_utc(),
            ))
        }

        async fn set_tier(
            &self,
            _owner: &Owner,
            tier: Tier,
        ) -> Result<OwnerTier, OwnerTierRepositoryError> {
            *self.stored_tier.lock().unwrap() = Some(tier);
            Ok(OwnerTier::new(
                Owner::User(UserId::new("test").unwrap()),
                tier,
                OffsetDateTime::now_utc(),
                OffsetDateTime::now_utc(),
            ))
        }
    }

    fn make_account(id: Uuid, _tier: Tier) -> Account {
        Account::new(
            id,
            AccountNumber::new(&format!("acc-{}", id)).unwrap(),
            Owner::User(UserId::new("user-1").unwrap()),
            Balance::zero(),
            OffsetDateTime::now_utc(),
        )
    }

    #[tokio::test]
    async fn allows_upgrade_from_basic_to_premium() {
        let accounts = vec![make_account(Uuid::new_v4(), Tier::Basic)];
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(1),
            accounts: Mutex::new(accounts),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Basic)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Premium,
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Tier::Premium);
        assert_eq!(
            tier_repo.stored_tier.lock().unwrap().unwrap(),
            Tier::Premium
        );
    }

    #[tokio::test]
    async fn allows_upgrade_from_basic_to_elite() {
        let accounts = vec![make_account(Uuid::new_v4(), Tier::Basic)];
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(1),
            accounts: Mutex::new(accounts),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Basic)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Elite,
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Tier::Elite);
        assert_eq!(tier_repo.stored_tier.lock().unwrap().unwrap(), Tier::Elite);
    }

    #[tokio::test]
    async fn allows_upgrade_from_premium_to_elite() {
        let accounts = vec![
            make_account(Uuid::new_v4(), Tier::Premium),
            make_account(Uuid::new_v4(), Tier::Premium),
        ];
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(2),
            accounts: Mutex::new(accounts),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Premium)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Elite,
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Tier::Elite);
        assert_eq!(tier_repo.stored_tier.lock().unwrap().unwrap(), Tier::Elite);
    }

    #[tokio::test]
    async fn allows_downgrade_when_account_count_within_limit() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(2),
            accounts: Mutex::new(vec![]),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Elite)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Premium,
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Tier::Premium);
        assert_eq!(
            tier_repo.stored_tier.lock().unwrap().unwrap(),
            Tier::Premium
        );
    }

    #[tokio::test]
    async fn rejects_downgrade_when_account_count_exceeds_limit() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(3),
            accounts: Mutex::new(vec![]),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Premium)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Basic,
        };

        let result = use_case.execute(input).await;

        assert!(matches!(
            result,
            Err(OperationError::TierDowngradeNotAllowed { .. })
        ));
        assert_eq!(
            tier_repo.stored_tier.lock().unwrap().unwrap(),
            Tier::Premium
        );
    }

    #[tokio::test]
    async fn rejects_downgrade_from_premium_to_basic_when_3_accounts() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(3),
            accounts: Mutex::new(vec![]),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Premium)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Basic,
        };

        let result = use_case.execute(input).await;

        assert!(matches!(
            result,
            Err(OperationError::TierDowngradeNotAllowed { .. })
        ));
        assert_eq!(
            tier_repo.stored_tier.lock().unwrap().unwrap(),
            Tier::Premium
        );
    }

    #[tokio::test]
    async fn allows_same_tier_change() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Ok(1),
            accounts: Mutex::new(vec![]),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(Some(Tier::Premium)),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Premium,
        };

        let result = use_case.execute(input).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Tier::Premium);
    }

    #[tokio::test]
    async fn returns_error_when_count_query_fails() {
        let repo = Arc::new(FakeAccountRepository {
            count_result: Err(AccountRepositoryError::OperationFailed {
                operation: "count_by_owner".to_string(),
                reason: "test failure".to_string(),
            }),
            accounts: Mutex::new(vec![]),
        });
        let tier_repo = Arc::new(FakeOwnerTierRepository {
            stored_tier: Mutex::new(None),
        });

        let use_case = ChangeTierUseCase::new(repo.clone(), tier_repo.clone());

        let input = ChangeTierInput {
            owner: Owner::User(UserId::new("user-1").unwrap()),
            new_tier: Tier::Premium,
        };

        let result = use_case.execute(input).await;

        assert!(matches!(
            result,
            Err(OperationError::RepositoryError { operation })
            if operation == RepositoryOperation::CountByOwner
        ));
    }

    #[tokio::test]
    async fn upgrade_unlocks_additional_account_slots() {
        use crate::application::ports::{AccountNumberGenerator, AccountNumberGeneratorError};

        let owner = Owner::User(UserId::new("user-upgrade-test").unwrap());

        struct SharedState {
            accounts: Mutex<Vec<Account>>,
            tier: Mutex<Tier>,
        }

        struct TrackingAccountRepo {
            state: Arc<SharedState>,
        }

        struct TrackingTierRepo {
            state: Arc<SharedState>,
        }

        #[async_trait]
        impl AccountRepository for TrackingAccountRepo {
            async fn count_by_owner(&self, owner: &Owner) -> Result<u64, AccountRepositoryError> {
                let accounts = self.state.accounts.lock().unwrap();
                let count = accounts.iter().filter(|a| a.owner() == owner).count() as u64;
                Ok(count)
            }

            async fn create(&self, account: &Account) -> Result<(), AccountRepositoryError> {
                self.state.accounts.lock().unwrap().push(account.clone());
                Ok(())
            }

            async fn find_by_number(
                &self,
                _number: &AccountNumber,
            ) -> Result<Option<Account>, AccountRepositoryError> {
                Err(AccountRepositoryError::OperationFailed {
                    operation: "find_by_number".to_string(),
                    reason: "not implemented in test".to_string(),
                })
            }

            async fn find_by_owner(
                &self,
                owner: &Owner,
            ) -> Result<Vec<Account>, AccountRepositoryError> {
                let accounts = self.state.accounts.lock().unwrap();
                Ok(accounts
                    .iter()
                    .filter(|a| a.owner() == owner)
                    .cloned()
                    .collect())
            }

            async fn create_within_limit(
                &self,
                account: &Account,
                limit: Option<u64>,
            ) -> Result<(), AccountRepositoryError> {
                let owner = account.owner();
                let count = self.count_by_owner(owner).await?;
                if let Some(limit) = limit
                    && count >= limit
                {
                    return Err(AccountRepositoryError::LimitExceeded);
                }
                self.create(account).await
            }
        }

        #[async_trait]
        impl OwnerTierRepository for TrackingTierRepo {
            async fn get_or_default(
                &self,
                owner: &Owner,
            ) -> Result<OwnerTier, OwnerTierRepositoryError> {
                let tier = *self.state.tier.lock().unwrap();
                Ok(OwnerTier::new(
                    owner.clone(),
                    tier,
                    OffsetDateTime::now_utc(),
                    OffsetDateTime::now_utc(),
                ))
            }

            async fn set_tier(
                &self,
                owner: &Owner,
                tier: Tier,
            ) -> Result<OwnerTier, OwnerTierRepositoryError> {
                *self.state.tier.lock().unwrap() = tier;
                Ok(OwnerTier::new(
                    owner.clone(),
                    tier,
                    OffsetDateTime::now_utc(),
                    OffsetDateTime::now_utc(),
                ))
            }
        }

        struct FakeGenerator {
            counter: Mutex<u64>,
        }

        #[async_trait]
        impl AccountNumberGenerator for FakeGenerator {
            async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError> {
                let mut counter = self.counter.lock().unwrap();
                *counter += 1;
                AccountNumber::new(&format!("ACC-{:03}", *counter))
                    .map_err(|_| AccountNumberGeneratorError::GenerationFailed)
            }
        }

        let state = Arc::new(SharedState {
            accounts: Mutex::new(vec![]),
            tier: Mutex::new(Tier::Basic),
        });

        let account_repo = Arc::new(TrackingAccountRepo {
            state: state.clone(),
        });
        let tier_repo = Arc::new(TrackingTierRepo {
            state: state.clone(),
        });
        let generator = Arc::new(FakeGenerator {
            counter: Mutex::new(0),
        });

        let create_uc =
            CreateAccountUseCase::new(account_repo.clone(), generator.clone(), tier_repo.clone());
        let result = create_uc
            .execute(CreateAccountInput {
                owner: owner.clone(),
            })
            .await;
        assert!(result.is_ok(), "Should create 1st account with Basic tier");

        let result = create_uc
            .execute(CreateAccountInput {
                owner: owner.clone(),
            })
            .await;
        assert!(
            matches!(result, Err(OperationError::TierLimitExceeded)),
            "Should fail to create 2nd account with Basic tier (limit=1)"
        );

        let change_tier_uc = ChangeTierUseCase::new(account_repo.clone(), tier_repo.clone());
        let result = change_tier_uc
            .execute(ChangeTierInput {
                owner: owner.clone(),
                new_tier: Tier::Premium,
            })
            .await;
        assert!(result.is_ok(), "Upgrade to Premium should succeed");
        assert_eq!(result.unwrap(), Tier::Premium);

        let result = create_uc
            .execute(CreateAccountInput {
                owner: owner.clone(),
            })
            .await;
        assert!(
            result.is_ok(),
            "Should create 2nd account after upgrade to Premium"
        );

        let result = create_uc
            .execute(CreateAccountInput {
                owner: owner.clone(),
            })
            .await;
        assert!(
            result.is_ok(),
            "Should create 3rd account with Premium tier"
        );

        {
            let accounts = state.accounts.lock().unwrap();
            assert_eq!(accounts.len(), 3, "Should have 3 accounts total");
        }

        let result = create_uc
            .execute(CreateAccountInput {
                owner: owner.clone(),
            })
            .await;
        assert!(
            matches!(result, Err(OperationError::TierLimitExceeded)),
            "Should fail to create 4th account with Premium tier (limit=3)"
        );

        let result = change_tier_uc
            .execute(ChangeTierInput {
                owner: owner.clone(),
                new_tier: Tier::Elite,
            })
            .await;
        assert!(result.is_ok(), "Upgrade to Elite should succeed");
        assert_eq!(result.unwrap(), Tier::Elite);

        let result = create_uc
            .execute(CreateAccountInput {
                owner: owner.clone(),
            })
            .await;
        assert!(
            result.is_ok(),
            "Should create 4th account with Elite tier (unlimited)"
        );

        {
            let accounts = state.accounts.lock().unwrap();
            assert_eq!(accounts.len(), 4, "Should have 4 accounts total");
        }
    }
}
