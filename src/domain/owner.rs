use crate::domain::{org_id::OrgId, user_id::UserId};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Owner {
    User(UserId),
    Org(OrgId),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_can_be_constructed_with_user_variant() {
        let owner = Owner::User(UserId::new("user-1").unwrap());
        assert!(matches!(owner, Owner::User(_)));
    }

    #[test]
    fn owner_can_be_constructed_with_org_variant() {
        let owner = Owner::Org(OrgId::new("org-1").unwrap());
        assert!(matches!(owner, Owner::Org(_)));
    }
}
