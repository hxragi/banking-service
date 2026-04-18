use core::fmt;

use crate::domain::errors::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountNumber(String);

impl AccountNumber {
    pub fn validate(input: &str) -> Result<&str, DomainError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            Err(DomainError::InvalidAccountNumber)
        } else {
            Ok(trimmed)
        }
    }

    pub fn new(input: &str) -> Result<Self, DomainError> {
        Ok(Self(Self::validate(input)?.to_owned()))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AccountNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<&str> for AccountNumber {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        AccountNumber::new(value)
    }
}

impl TryFrom<String> for AccountNumber {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        AccountNumber::new(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_valid_account_number() {
        let acc = AccountNumber::new(" 12345 ").unwrap();
        assert_eq!(acc.as_str(), "12345");
    }

    #[test]
    fn trims_input() {
        let acc = AccountNumber::new("  abc  ").unwrap();
        assert_eq!(acc.as_str(), "abc");
    }

    #[test]
    fn rejects_empty_string() {
        let err = AccountNumber::new("").unwrap_err();
        assert_eq!(err, DomainError::InvalidAccountNumber);
    }

    #[test]
    fn rejects_whitespace_only() {
        let err = AccountNumber::new("      ").unwrap_err();
        assert_eq!(err, DomainError::InvalidAccountNumber);
    }

    #[test]
    fn try_from_str_works() {
        let acc = AccountNumber::try_from("123").unwrap();
        assert_eq!(acc.as_str(), "123");
    }

    #[test]
    fn try_from_string_works() {
        let acc = AccountNumber::try_from(String::from("123")).unwrap();
        assert_eq!(acc.as_str(), "123");
    }

    #[test]
    fn display_outputs_inner_value() {
        let acc = AccountNumber::new("123").unwrap();
        assert_eq!(format!("{}", acc), "123");
    }
}
