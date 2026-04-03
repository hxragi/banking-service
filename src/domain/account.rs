use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    account_number::AccountNumber, amount::Amount, balance::Balance, errors::DomainError,
    owner::Owner,
};

#[derive(Debug, Clone)]
pub struct Account {
    id: Uuid,
    number: AccountNumber,
    owner: Owner,
    balance: Balance,
    created_at: OffsetDateTime,
}

impl Account {
    pub fn new(
        id: Uuid,
        number: AccountNumber,
        owner: Owner,
        balance: Balance,
        created_at: OffsetDateTime,
    ) -> Self {
        Self {
            id,
            number,
            owner,
            balance,
            created_at,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn number(&self) -> &AccountNumber {
        &self.number
    }

    pub fn owner(&self) -> &Owner {
        &self.owner
    }

    pub fn balance(&self) -> Balance {
        self.balance
    }

    pub fn created_at(&self) -> OffsetDateTime {
        self.created_at
    }

    pub fn deposit(self, amount: Amount) -> Self {
        let new_balance = self.balance.deposit(amount);
        Account {
            id: self.id,
            number: self.number,
            owner: self.owner,
            balance: new_balance,
            created_at: self.created_at,
        }
    }

    pub fn withdraw(self, amount: Amount) -> Result<Self, DomainError> {
        let new_balance = self.balance.withdraw(amount)?;
        Ok(Account {
            id: self.id,
            number: self.number,
            owner: self.owner,
            balance: new_balance,
            created_at: self.created_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{account_number::AccountNumber, balance::Balance, user_id::UserId};
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn make_account(balance: u64) -> Account {
        Account::new(
            Uuid::new_v4(),
            AccountNumber::new("acc-1").unwrap(),
            Owner::User(UserId::new("user-1").unwrap()),
            Balance::new(balance),
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    #[test]
    fn new_creates_account_with_given_fields() {
        let id = Uuid::new_v4();
        let number = AccountNumber::new("acc-123").unwrap();
        let owner = Owner::User(UserId::new("user-42").unwrap());
        let balance = Balance::new(100);
        let created_at = OffsetDateTime::UNIX_EPOCH;

        let account = Account::new(id, number.clone(), owner.clone(), balance, created_at);

        assert_eq!(account.id(), id);
        assert_eq!(account.number(), &number);
        assert_eq!(account.owner(), &owner);
        assert_eq!(account.balance(), balance);
        assert_eq!(account.created_at(), created_at);
    }

    #[test]
    fn deposit_returns_new_account_with_increased_balance() {
        let account = make_account(100);
        let amount = Amount::new(25).unwrap();

        let updated = account.clone().deposit(amount);

        assert_eq!(updated.balance().as_u64(), 125);
        assert_eq!(account.balance().as_u64(), 100);

        assert_eq!(updated.id(), account.id());
        assert_eq!(updated.number(), account.number());
        assert_eq!(updated.owner(), account.owner());
        assert_eq!(updated.created_at(), account.created_at());
    }

    #[test]
    fn withdraw_returns_new_account_with_decreased_balance() {
        let account = make_account(100);
        let amount = Amount::new(40).unwrap();

        let updated = account.clone().withdraw(amount).unwrap();

        assert_eq!(updated.balance().as_u64(), 60);
        assert_eq!(account.balance().as_u64(), 100);

        assert_eq!(updated.id(), account.id());
        assert_eq!(updated.number(), account.number());
        assert_eq!(updated.owner(), account.owner());
        assert_eq!(updated.created_at(), account.created_at());
    }

    #[test]
    fn withdraw_returns_error_when_insufficient_funds() {
        let account = make_account(10);
        let amount = Amount::new(50).unwrap();

        let result = account.withdraw(amount);

        assert!(matches!(result, Err(DomainError::InsufficientFunds)));
    }
}
