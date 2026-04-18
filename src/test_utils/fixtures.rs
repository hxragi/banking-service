use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    account::Account, account_number::AccountNumber, amount::Amount, balance::Balance,
    owner::Owner, transaction::Transaction, user_id::UserId,
};

pub fn create_test_account(number: &str, balance: u64) -> Account {
    Account::new(
        Uuid::new_v4(),
        AccountNumber::new(number).unwrap(),
        Owner::User(UserId::new("user-1").unwrap()),
        Balance::new(balance),
        OffsetDateTime::UNIX_EPOCH,
    )
}

pub fn create_test_amount(value: u64) -> Amount {
    Amount::new(value).unwrap()
}

pub fn create_test_deposit_transaction(account_id: Uuid, amount: u64) -> Transaction {
    Transaction::deposit(
        Uuid::new_v4(),
        Amount::new(amount).unwrap(),
        account_id,
        OffsetDateTime::now_utc(),
    )
}

pub fn create_test_withdraw_transaction(account_id: Uuid, amount: u64) -> Transaction {
    Transaction::withdraw(
        Uuid::new_v4(),
        Amount::new(amount).unwrap(),
        account_id,
        OffsetDateTime::now_utc(),
    )
}

pub fn test_user_id() -> UserId {
    UserId::new("user-1").unwrap()
}

pub fn test_owner() -> Owner {
    Owner::User(test_user_id())
}

pub fn test_account_number(number: &str) -> AccountNumber {
    AccountNumber::new(number).unwrap()
}
