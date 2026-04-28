use std::fmt;

use crate::errors::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Amount(u64);

impl Amount {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        if value == 0 {
            Err(DomainError::InvalidAmount)
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<u64> for Amount {
    type Error = DomainError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Amount::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amount_new_success() {
        let amount = Amount::new(1);
        assert!(amount.is_ok());
        assert_eq!(amount.unwrap().as_u64(), 1);
    }

    #[test]
    fn amount_new_zero_returns_error() {
        let amount = Amount::new(0);
        assert!(amount.is_err());
        assert_eq!(amount.unwrap_err(), DomainError::InvalidAmount)
    }

    #[test]
    fn try_from_u64_works() {
        let amount = Amount::try_from(5);
        assert!(amount.is_ok());
        assert_eq!(amount.unwrap().as_u64(), 5);
    }

    #[test]
    fn display_outputs_number() {
        let amount = Amount::new(42).unwrap();
        let formatted = format!("{}", amount);
        assert_eq!(formatted, "42")
    }
}
