use crate::domain::{org_id::OrgId, user_id::UserId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    User(UserId),
    Org(OrgId),
}
