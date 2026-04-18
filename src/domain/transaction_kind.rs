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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTransactionKindError;

impl std::fmt::Display for ParseTransactionKindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown transaction kind")
    }
}

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
