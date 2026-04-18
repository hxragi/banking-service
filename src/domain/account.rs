use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{account_number::AccountNumber, balance::Balance, owner::Owner};

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{account_number::AccountNumber, balance::Balance, user_id::UserId};
    use time::OffsetDateTime;
    use uuid::Uuid;

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
}
