use crate::{amount::Amount, errors::DomainError, transaction_kind::TransactionKind};

use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Transaction {
    id: Uuid,
    kind: TransactionKind,
    amount: Amount,
    from_account_id: Option<Uuid>,
    to_account_id: Option<Uuid>,
    created_at: OffsetDateTime,
}

impl Transaction {
    pub fn deposit(
        id: Uuid,
        amount: Amount,
        to_account_id: Uuid,
        created_at: OffsetDateTime,
    ) -> Self {
        Self {
            id,
            kind: TransactionKind::Deposit,
            amount,
            from_account_id: None,
            to_account_id: Some(to_account_id),
            created_at,
        }
    }

    pub fn withdraw(
        id: Uuid,
        amount: Amount,
        from_account_id: Uuid,
        created_at: OffsetDateTime,
    ) -> Self {
        Self {
            id,
            kind: TransactionKind::Withdraw,
            amount,
            from_account_id: Some(from_account_id),
            to_account_id: None,
            created_at,
        }
    }

    pub fn transfer(
        id: Uuid,
        amount: Amount,
        from_account_id: Uuid,
        to_account_id: Uuid,
        created_at: OffsetDateTime,
    ) -> Result<Self, DomainError> {
        if from_account_id != to_account_id {
            Ok(Self {
                id,
                kind: TransactionKind::Transfer,
                amount,
                from_account_id: Some(from_account_id),
                to_account_id: Some(to_account_id),
                created_at,
            })
        } else {
            Err(DomainError::SameAccountTransfer)
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn kind(&self) -> TransactionKind {
        self.kind
    }

    pub fn amount(&self) -> Amount {
        self.amount
    }

    pub fn source_account_id(&self) -> Option<Uuid> {
        self.from_account_id
    }

    pub fn destination_account_id(&self) -> Option<Uuid> {
        self.to_account_id
    }

    pub fn created_at(&self) -> OffsetDateTime {
        self.created_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{amount::Amount, errors::DomainError, transaction_kind::TransactionKind};
    use time::OffsetDateTime;
    use uuid::Uuid;

    #[test]
    fn deposit_creates_deposit_transaction() {
        let id = Uuid::new_v4();
        let to_account_id = Uuid::new_v4();
        let amount = Amount::new(50).unwrap();
        let created_at = OffsetDateTime::UNIX_EPOCH;

        let transaction = Transaction::deposit(id, amount, to_account_id, created_at);

        assert_eq!(transaction.id(), id);
        assert_eq!(transaction.kind(), TransactionKind::Deposit);
        assert_eq!(transaction.amount(), amount);
        assert_eq!(transaction.source_account_id(), None);
        assert_eq!(transaction.destination_account_id(), Some(to_account_id));
        assert_eq!(transaction.created_at(), created_at);
    }

    #[test]
    fn withdraw_creates_withdraw_transaction() {
        let id = Uuid::new_v4();
        let from_account_id = Uuid::new_v4();
        let amount = Amount::new(75).unwrap();
        let created_at = OffsetDateTime::UNIX_EPOCH;

        let transaction = Transaction::withdraw(id, amount, from_account_id, created_at);

        assert_eq!(transaction.id(), id);
        assert_eq!(transaction.kind(), TransactionKind::Withdraw);
        assert_eq!(transaction.amount(), amount);
        assert_eq!(transaction.source_account_id(), Some(from_account_id));
        assert_eq!(transaction.destination_account_id(), None);
        assert_eq!(transaction.created_at(), created_at);
    }

    #[test]
    fn transfer_creates_transfer_transaction() {
        let id = Uuid::new_v4();
        let from_account_id = Uuid::new_v4();
        let to_account_id = Uuid::new_v4();
        let amount = Amount::new(100).unwrap();
        let created_at = OffsetDateTime::UNIX_EPOCH;

        let transaction =
            Transaction::transfer(id, amount, from_account_id, to_account_id, created_at).unwrap();

        assert_eq!(transaction.id(), id);
        assert_eq!(transaction.kind(), TransactionKind::Transfer);
        assert_eq!(transaction.amount(), amount);
        assert_eq!(transaction.source_account_id(), Some(from_account_id));
        assert_eq!(transaction.destination_account_id(), Some(to_account_id));
        assert_eq!(transaction.created_at(), created_at);
    }

    #[test]
    fn transfer_to_same_account_returns_error() {
        let id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let amount = Amount::new(10).unwrap();
        let created_at = OffsetDateTime::UNIX_EPOCH;

        let result = Transaction::transfer(id, amount, account_id, account_id, created_at);

        assert!(matches!(result, Err(DomainError::SameAccountTransfer)));
    }
}
