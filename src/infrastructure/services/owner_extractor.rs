use crate::domain::org_id::OrgId;
use crate::domain::owner::Owner;
use crate::domain::user_id::UserId;

#[derive(Debug, PartialEq, Eq)]
pub enum OwnerExtractionError {
    MissingOwner,
    InvalidUserId,
    InvalidOrgId,
    BothUserAndOrgProvided,
    OrgNotAllowed,
}

pub struct OwnerExtractor;

impl OwnerExtractor {
    pub fn extract_from_params(
        user_id: Option<String>,
        org_id: Option<String>,
        is_internal: bool,
    ) -> Result<Owner, OwnerExtractionError> {
        match (user_id, org_id) {
            (Some(user_id), None) => UserId::new(&user_id)
                .map(Owner::User)
                .map_err(|_| OwnerExtractionError::InvalidUserId),
            (None, Some(org_id)) => {
                if !is_internal {
                    return Err(OwnerExtractionError::OrgNotAllowed);
                }
                OrgId::new(&org_id)
                    .map(Owner::Org)
                    .map_err(|_| OwnerExtractionError::InvalidOrgId)
            }
            (Some(_), Some(_)) => Err(OwnerExtractionError::BothUserAndOrgProvided),
            (None, None) => Err(OwnerExtractionError::MissingOwner),
        }
    }

    pub fn extract_from_header_and_params(
        user_id_header: Option<&str>,
        user_id_param: Option<String>,
        org_id_param: Option<String>,
        is_internal: bool,
    ) -> Result<Owner, OwnerExtractionError> {
        match (user_id_param, org_id_param) {
            (Some(user_id), None) => UserId::new(&user_id)
                .map(Owner::User)
                .map_err(|_| OwnerExtractionError::InvalidUserId),
            (None, Some(org_id)) => {
                if !is_internal {
                    return Err(OwnerExtractionError::OrgNotAllowed);
                }
                OrgId::new(&org_id)
                    .map(Owner::Org)
                    .map_err(|_| OwnerExtractionError::InvalidOrgId)
            }
            (Some(_), Some(_)) => Err(OwnerExtractionError::BothUserAndOrgProvided),
            (None, None) => {
                let user_id = user_id_header.ok_or(OwnerExtractionError::MissingOwner)?;
                UserId::new(user_id)
                    .map(Owner::User)
                    .map_err(|_| OwnerExtractionError::InvalidUserId)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_from_params_with_valid_user_id() {
        let result = OwnerExtractor::extract_from_params(Some("user123".to_string()), None, false);
        assert!(matches!(result, Ok(Owner::User(_))));
    }

    #[test]
    fn extract_from_params_with_valid_org_id_internal() {
        let result = OwnerExtractor::extract_from_params(None, Some("org456".to_string()), true);
        assert!(matches!(result, Ok(Owner::Org(_))));
    }

    #[test]
    fn extract_from_params_with_org_id_not_internal_fails() {
        let result = OwnerExtractor::extract_from_params(None, Some("org456".to_string()), false);
        assert_eq!(result, Err(OwnerExtractionError::OrgNotAllowed));
    }

    #[test]
    fn extract_from_params_with_both_fails() {
        let result = OwnerExtractor::extract_from_params(
            Some("user123".to_string()),
            Some("org456".to_string()),
            true,
        );
        assert_eq!(result, Err(OwnerExtractionError::BothUserAndOrgProvided));
    }

    #[test]
    fn extract_from_params_with_none_fails() {
        let result = OwnerExtractor::extract_from_params(None, None, true);
        assert_eq!(result, Err(OwnerExtractionError::MissingOwner));
    }

    #[test]
    fn extract_from_params_with_invalid_user_id() {
        let result = OwnerExtractor::extract_from_params(Some("   ".to_string()), None, false);
        assert_eq!(result, Err(OwnerExtractionError::InvalidUserId));
    }

    #[test]
    fn extract_from_header_and_params_uses_param_when_provided() {
        let result = OwnerExtractor::extract_from_header_and_params(
            Some("header_user"),
            Some("param_user".to_string()),
            None,
            false,
        );
        assert!(matches!(result, Ok(Owner::User(ref id)) if id.as_str() == "param_user"));
    }

    #[test]
    fn extract_from_header_and_params_uses_header_when_param_missing() {
        let result =
            OwnerExtractor::extract_from_header_and_params(Some("header_user"), None, None, false);
        assert!(matches!(result, Ok(Owner::User(ref id)) if id.as_str() == "header_user"));
    }

    #[test]
    fn extract_from_header_and_params_fails_when_both_missing() {
        let result = OwnerExtractor::extract_from_header_and_params(None, None, None, false);
        assert_eq!(result, Err(OwnerExtractionError::MissingOwner));
    }
}
