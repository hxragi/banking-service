#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Basic,
    Premium,
    Elite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountLimit {
    Limited(u64),
    Unlimited,
}

impl Tier {
    pub fn account_limit(self) -> AccountLimit {
        match self {
            Tier::Basic => AccountLimit::Limited(1),
            Tier::Premium => AccountLimit::Limited(3),
            Tier::Elite => AccountLimit::Unlimited,
        }
    }
}
