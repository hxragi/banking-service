use core::fmt;

use crate::errors::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserId(String);

impl UserId {
    pub fn validate(input: &str) -> Result<&str, DomainError> {
        let trimmed = input.trim();

        if trimmed.is_empty() || trimmed.len() > 256 {
            return Err(DomainError::InvalidUserId);
        }

        Ok(trimmed)
    }

    pub fn new(input: &str) -> Result<Self, DomainError> {
        Ok(Self(Self::validate(input)?.to_owned()))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<&str> for UserId {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        UserId::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_valid_user_id() {
        let acc = UserId::new(" 12345 ").unwrap();
        assert_eq!(acc.as_str(), "12345");
    }

    #[test]
    fn trims_input() {
        let acc = UserId::new("  abc  ").unwrap();
        assert_eq!(acc.as_str(), "abc");
    }

    #[test]
    fn rejects_empty_string() {
        let err = UserId::new("").unwrap_err();
        assert_eq!(err, DomainError::InvalidUserId);
    }

    #[test]
    fn rejects_whitespace_only() {
        let err = UserId::new("      ").unwrap_err();
        assert_eq!(err, DomainError::InvalidUserId);
    }

    #[test]
    fn try_from_str_works() {
        let acc = UserId::try_from("123").unwrap();
        assert_eq!(acc.as_str(), "123");
    }

    #[test]
    fn display_outputs_inner_value() {
        let acc = UserId::new("123").unwrap();
        assert_eq!(format!("{}", acc), "123");
    }

    #[test]
    fn user_id_rejects_too_long() {
        let long_id = "x".repeat(300);
        assert!(UserId::new(&long_id).is_err());
    }
}
