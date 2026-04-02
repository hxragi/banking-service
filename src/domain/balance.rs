use std::fmt;

use crate::domain::{amount::Amount, errors::DomainError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Balance(u64);

impl Balance {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn can_withdraw(&self, amount: Amount) -> bool {
        self.0 >= amount.as_u64()
    }

    pub fn deposit(self, amount: Amount) -> Self {
        Self(self.0 + amount.as_u64())
    }

    pub fn withdraw(self, amount: Amount) -> Result<Self, DomainError> {
        if self.can_withdraw(amount) {
            Ok(Self(self.0 - amount.as_u64()))
        } else {
            Err(DomainError::InsufficientFunds)
        }
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::balance::Balance;

    #[test]
    fn zero_creates_zero_balance() {
        let balance = Balance::zero();
        assert_eq!(balance.as_u64(), 0);
    }

    #[test]
    fn new_creates_balance_with_value() {
        let balance = Balance::new(10);
        assert_eq!(balance.as_u64(), 10);
    }

    #[test]
    fn can_withdraw_returns_true_if_enough() {
        let balance = Balance::new(50);
        let amount = Amount::new(30).unwrap();
        assert!(balance.can_withdraw(amount));
    }

    #[test]
    fn can_withdraw_returns_false_if_not_enough() {
        let balance = Balance::new(20);
        let amount = Amount::new(30).unwrap();
        assert!(!balance.can_withdraw(amount));
    }

    #[test]
    fn deposit_increases_balance() {
        let balance = Balance::new(40);
        let amount = Amount::new(25).unwrap();
        let new_balance = balance.deposit(amount);
        assert_eq!(new_balance.as_u64(), 65);
        assert_eq!(balance.as_u64(), 40);
    }

    #[test]
    fn withdraw_decreases_balance() {
        let balance = Balance::new(50);
        let amount = Amount::new(20).unwrap();
        let new_balance = balance.withdraw(amount).unwrap();
        assert_eq!(new_balance.as_u64(), 30);
        assert_eq!(balance.as_u64(), 50);
    }

    #[test]
    fn withdraw_insufficient_funds_returns_error() {
        let balance = Balance::new(10);
        let amount = Amount::new(20).unwrap();
        let result = balance.withdraw(amount);
        assert!(matches!(result, Err(DomainError::InsufficientFunds)));
    }

    #[test]
    fn display_outputs_number() {
        let balance = Balance::new(123);
        let formatted = format!("{}", balance);
        assert_eq!(formatted, "123");
    }
}
