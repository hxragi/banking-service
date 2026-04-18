use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Balance(u64);

impl Balance {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_balance_with_value() {
        let balance = Balance::new(10);
        assert_eq!(balance.as_u64(), 10);
    }

    #[test]
    fn display_outputs_number() {
        let balance = Balance::new(123);
        let formatted = format!("{}", balance);
        assert_eq!(formatted, "123");
    }
}
