use core::fmt;

use crate::domain::errors::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgId(String);

impl OrgId {
    pub fn validate(input: &str) -> Result<&str, DomainError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            Err(DomainError::InvalidOrgId)
        } else {
            Ok(trimmed)
        }
    }

    pub fn new(input: &str) -> Result<Self, DomainError> {
        Ok(Self(Self::validate(input)?.to_owned()))
    }

    pub fn from_string(input: String) -> Result<Self, DomainError> {
        let trimmed = input.trim();

        if trimmed.len() == input.len() {
            Ok(Self(input))
        } else {
            Ok(Self(trimmed.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OrgId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<&str> for OrgId {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        OrgId::new(value)
    }
}

impl TryFrom<String> for OrgId {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        OrgId::from_string(value)
    }
}

impl AsRef<str> for OrgId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_valid_user_id() {
        let acc = OrgId::new(" 12345 ").unwrap();
        assert_eq!(acc.as_str(), "12345");
    }

    #[test]
    fn trims_input() {
        let acc = OrgId::new("  abc  ").unwrap();
        assert_eq!(acc.as_str(), "abc");
    }

    #[test]
    fn rejects_empty_string() {
        let err = OrgId::new("").unwrap_err();
        assert_eq!(err, DomainError::InvalidOrgId);
    }

    #[test]
    fn rejects_whitespace_only() {
        let err = OrgId::new("      ").unwrap_err();
        assert_eq!(err, DomainError::InvalidOrgId);
    }

    #[test]
    fn try_from_str_works() {
        let acc = OrgId::try_from("123").unwrap();
        assert_eq!(acc.as_str(), "123");
    }

    #[test]
    fn try_from_string_works() {
        let acc = OrgId::try_from(String::from("123")).unwrap();
        assert_eq!(acc.as_str(), "123");
    }

    #[test]
    fn display_outputs_inner_value() {
        let acc = OrgId::new("123").unwrap();
        assert_eq!(format!("{}", acc), "123");
    }
}
