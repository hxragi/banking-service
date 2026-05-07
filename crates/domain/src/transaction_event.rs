use crate::{amount::Amount, transaction::Transaction, transaction_kind::TransactionKind};

use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionEvent {
    pub transaction_id: Uuid,
    pub kind: TransactionKind,
    pub from_account_id: Option<Uuid>,
    pub to_account_id: Option<Uuid>,
    pub amount: u64,
    pub timestamp: OffsetDateTime,
}

impl TransactionEvent {
    pub fn new(
        transaction_id: Uuid,
        kind: TransactionKind,
        from_account_id: Option<Uuid>,
        to_account_id: Option<Uuid>,
        amount: Amount,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self {
            transaction_id,
            kind,
            from_account_id,
            to_account_id,
            amount: amount.as_u64(),
            timestamp,
        }
    }

    pub fn from_transaction(transaction: &Transaction) -> Self {
        Self::new(
            transaction.id(),
            transaction.kind(),
            transaction.source_account_id(),
            transaction.destination_account_id(),
            transaction.amount(),
            transaction.created_at(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    fn create_test_transaction(kind: TransactionKind) -> Transaction {
        let id = Uuid::new_v4();
        let from_id = Uuid::new_v4();
        let to_id = Uuid::new_v4();
        let amount = Amount::new(1000).unwrap();
        let timestamp = OffsetDateTime::UNIX_EPOCH;

        match kind {
            TransactionKind::Deposit => Transaction::deposit(id, amount, to_id, timestamp),
            TransactionKind::Withdraw => Transaction::withdraw(id, amount, from_id, timestamp),
            TransactionKind::Transfer => {
                Transaction::transfer(id, amount, from_id, to_id, timestamp).unwrap()
            }
        }
    }

    #[test]
    fn transaction_event_from_deposit_transaction() {
        let transaction = create_test_transaction(TransactionKind::Deposit);
        let event = TransactionEvent::from_transaction(&transaction);

        assert_eq!(event.transaction_id, transaction.id());
        assert_eq!(event.kind, TransactionKind::Deposit);
        assert_eq!(event.from_account_id, None);
        assert_eq!(event.to_account_id, transaction.destination_account_id());
        assert_eq!(event.amount, transaction.amount().as_u64());
        assert_eq!(event.timestamp, transaction.created_at());
    }

    #[test]
    fn transaction_event_from_withdraw_transaction() {
        let transaction = create_test_transaction(TransactionKind::Withdraw);
        let event = TransactionEvent::from_transaction(&transaction);

        assert_eq!(event.transaction_id, transaction.id());
        assert_eq!(event.kind, TransactionKind::Withdraw);
        assert_eq!(event.from_account_id, transaction.source_account_id());
        assert_eq!(event.to_account_id, None);
        assert_eq!(event.amount, transaction.amount().as_u64());
    }

    #[test]
    fn transaction_event_from_transfer_transaction() {
        let transaction = create_test_transaction(TransactionKind::Transfer);
        let event = TransactionEvent::from_transaction(&transaction);

        assert_eq!(event.transaction_id, transaction.id());
        assert_eq!(event.kind, TransactionKind::Transfer);
        assert_eq!(event.from_account_id, transaction.source_account_id());
        assert_eq!(event.to_account_id, transaction.destination_account_id());
        assert_eq!(event.amount, transaction.amount().as_u64());
    }
}
