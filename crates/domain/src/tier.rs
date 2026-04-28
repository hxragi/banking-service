#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Basic,
    Premium,
    Elite,
}

impl Tier {
    pub fn account_limit(self) -> Option<u64> {
        match self {
            Tier::Basic => Some(1),
            Tier::Premium => Some(3),
            Tier::Elite => None,
        }
    }

    pub fn as_i32(self) -> i32 {
        match self {
            Tier::Basic => 1,
            Tier::Premium => 2,
            Tier::Elite => 3,
        }
    }
}
