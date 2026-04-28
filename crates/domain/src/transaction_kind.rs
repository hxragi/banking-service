#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionKind {
    Deposit,
    Withdraw,
    Transfer,
}

impl TransactionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionKind::Deposit => "deposit",
            TransactionKind::Withdraw => "withdraw",
            TransactionKind::Transfer => "transfer",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown transaction kind")]
pub struct ParseTransactionKindError;

impl std::str::FromStr for TransactionKind {
    type Err = ParseTransactionKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "deposit" => Ok(TransactionKind::Deposit),
            "withdraw" => Ok(TransactionKind::Withdraw),
            "transfer" => Ok(TransactionKind::Transfer),
            _ => Err(ParseTransactionKindError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_display_message() {
        let err = ParseTransactionKindError;
        assert_eq!(format!("{}", err), "unknown transaction kind");
    }
}
