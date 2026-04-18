use tokio::sync::Mutex;

use crate::{
    application::ports::{
        AccountNumberGenerator, AccountNumberGeneratorError, AccountRepository,
        AccountRepositoryError, IdempotencyError, IdempotencyRepository, OwnerTierRepository,
        OwnerTierRepositoryError, PaginatedTransactions, TransactionRepository,
        TransactionRepositoryError,
    },
    domain::{
        account::Account, account_number::AccountNumber, owner::Owner, owner_tier::OwnerTier,
        tier::Tier, transaction::Transaction,
    },
};

pub struct MockAccountRepository {
    pub found_account: Mutex<Option<Account>>,
    pub accounts: Mutex<Vec<Account>>,
    pub count_result: Result<u64, AccountRepositoryError>,
    pub create_result: Result<(), AccountRepositoryError>,
    pub find_result: Result<(), AccountRepositoryError>,
    pub find_by_owner_result: Result<(), AccountRepositoryError>,
}

impl Default for MockAccountRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAccountRepository {
    pub fn new() -> Self {
        Self {
            found_account: Mutex::new(None),
            accounts: Mutex::new(vec![]),
            count_result: Ok(0),
            create_result: Ok(()),
            find_result: Ok(()),
            find_by_owner_result: Ok(()),
        }
    }

    pub fn with_account(mut self, account: Account) -> Self {
        self.found_account = Mutex::new(Some(account));
        self
    }

    pub fn with_accounts(mut self, accounts: Vec<Account>) -> Self {
        self.accounts = Mutex::new(accounts);
        self
    }

    pub fn with_count_result(mut self, result: Result<u64, AccountRepositoryError>) -> Self {
        self.count_result = result;
        self
    }

    pub fn with_create_result(mut self, result: Result<(), AccountRepositoryError>) -> Self {
        self.create_result = result;
        self
    }
}

#[async_trait::async_trait]
impl AccountRepository for MockAccountRepository {
    async fn count_by_owner(&self, _owner: &Owner) -> Result<u64, AccountRepositoryError> {
        self.count_result.clone()
    }

    async fn create(&self, account: &Account) -> Result<(), AccountRepositoryError> {
        if self.create_result.is_ok() {
            self.accounts.lock().await.push(account.clone());
        }
        self.create_result.clone()
    }

    async fn find_by_number(
        &self,
        _number: &AccountNumber,
    ) -> Result<Option<Account>, AccountRepositoryError> {
        self.find_result.clone()?;
        let found = self.found_account.lock().await;
        if found.is_some() {
            return Ok(found.clone());
        }
        let accounts = self.accounts.lock().await;
        Ok(accounts.iter().find(|a| a.number() == _number).cloned())
    }

    async fn find_by_owner(&self, _owner: &Owner) -> Result<Vec<Account>, AccountRepositoryError> {
        self.find_by_owner_result.clone()?;
        Ok(self.accounts.lock().await.clone())
    }
}

pub struct MockTransactionRepository {
    pub should_fail: bool,
    pub created_transactions: Mutex<Vec<Transaction>>,
}

impl Default for MockTransactionRepository {
    fn default() -> Self {
        Self::new(false)
    }
}

impl MockTransactionRepository {
    pub fn new(should_fail: bool) -> Self {
        Self {
            should_fail,
            created_transactions: Mutex::new(vec![]),
        }
    }

    pub async fn get_created_count(&self) -> usize {
        self.created_transactions.lock().await.len()
    }
}

#[async_trait::async_trait]
impl TransactionRepository for MockTransactionRepository {
    async fn find_by_account_id_paginated(
        &self,
        _account_id: uuid::Uuid,
        _page: u32,
        _page_size: u32,
    ) -> Result<PaginatedTransactions, TransactionRepositoryError> {
        Ok(PaginatedTransactions {
            transactions: vec![],
            total_count: 0,
            page: 0,
            page_size: 0,
            has_more: false,
        })
    }

    async fn count_by_account_id(
        &self,
        _account_id: uuid::Uuid,
    ) -> Result<u64, TransactionRepositoryError> {
        Ok(0)
    }
}

pub struct MockIdempotencyRepository;

#[async_trait::async_trait]
impl IdempotencyRepository for MockIdempotencyRepository {
    async fn get(&self, _key: &str) -> Result<Option<String>, IdempotencyError> {
        Ok(None)
    }

    async fn save(&self, _key: &str, _response: &str) -> Result<(), IdempotencyError> {
        Ok(())
    }
}

pub struct MockAccountNumberGenerator {
    pub generate_result: Result<AccountNumber, AccountNumberGeneratorError>,
}

impl Default for MockAccountNumberGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAccountNumberGenerator {
    pub fn new() -> Self {
        Self {
            generate_result: Ok(AccountNumber::new("ACC001").unwrap()),
        }
    }

    pub fn with_result(
        mut self,
        result: Result<AccountNumber, AccountNumberGeneratorError>,
    ) -> Self {
        self.generate_result = result;
        self
    }
}

#[async_trait::async_trait]
impl AccountNumberGenerator for MockAccountNumberGenerator {
    async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError> {
        self.generate_result.clone()
    }
}

pub struct MockOwnerTierRepository {
    pub tier_to_return: Tier,
    pub stored_tier: Mutex<Option<Tier>>,
}

impl MockOwnerTierRepository {
    pub fn new(tier: Tier) -> Self {
        Self {
            tier_to_return: tier,
            stored_tier: Mutex::new(None),
        }
    }

    pub fn default_with_tier(tier: Tier) -> Self {
        Self::new(tier)
    }

    pub fn with_stored_tier(tier: Tier) -> Self {
        Self {
            tier_to_return: tier,
            stored_tier: Mutex::new(Some(tier)),
        }
    }
}

#[async_trait::async_trait]
impl OwnerTierRepository for MockOwnerTierRepository {
    async fn get_or_default(&self, owner: &Owner) -> Result<OwnerTier, OwnerTierRepositoryError> {
        use time::OffsetDateTime;
        let tier = self.stored_tier.lock().await.unwrap_or(self.tier_to_return);
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
        use time::OffsetDateTime;
        *self.stored_tier.lock().await = Some(tier);
        Ok(OwnerTier::new(
            _owner.clone(),
            tier,
            OffsetDateTime::now_utc(),
            OffsetDateTime::now_utc(),
        ))
    }
}
