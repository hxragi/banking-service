use core::fmt;

use crate::domain::errors::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgId(String);

impl OrgId {
    pub fn validate(input: &str) -> Result<&str, DomainError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            Err(DomainError::InvalidOrgId)
        } else if trimmed.len() > 256 {
            Err(DomainError::InvalidOrgId)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_valid_org_id() {
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
    fn display_outputs_inner_value() {
        let acc = OrgId::new("123").unwrap();
        assert_eq!(format!("{}", acc), "123");
    }

    #[test]
    fn org_id_rejects_too_long() {
        let long_id = "x".repeat(300);
        assert!(OrgId::new(&long_id).is_err());
    }
}
