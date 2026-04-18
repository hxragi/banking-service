use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::transaction_event::TransactionEvent;
use crate::domain::transaction_kind::TransactionKind;

#[cfg(test)]
use crate::domain::transaction::Transaction;

fn serialize_operation_type<S>(kind: &TransactionKind, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    kind.as_str().serialize(serializer)
}

fn deserialize_operation_type<'de, D>(deserializer: D) -> Result<TransactionKind, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    TransactionKind::from_str(&s)
        .map_err(|_| serde::de::Error::custom(format!("unknown operation_type: {}", s)))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionEventDto {
    pub transaction_id: Uuid,

    #[serde(
        rename = "operation_type",
        serialize_with = "serialize_operation_type",
        deserialize_with = "deserialize_operation_type"
    )]
    pub kind: TransactionKind,

    pub from_account_id: Option<Uuid>,

    pub to_account_id: Option<Uuid>,

    pub amount: u64,

    #[serde(with = "time::serde::iso8601")]
    pub timestamp: OffsetDateTime,

    #[serde(default = "default_schema_version")]
    pub schema_version: String,
}

fn default_schema_version() -> String {
    "1.0".to_string()
}

impl TransactionEventDto {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl From<TransactionEvent> for TransactionEventDto {
    fn from(event: TransactionEvent) -> Self {
        Self {
            transaction_id: event.transaction_id,
            kind: event.kind,
            from_account_id: event.from_account_id,
            to_account_id: event.to_account_id,
            amount: event.amount,
            timestamp: event.timestamp,
            schema_version: "1.0".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{amount::Amount, transaction_event::TransactionEvent};
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
        let event: TransactionEventDto = TransactionEvent::from_transaction(&transaction).into();

        assert_eq!(event.transaction_id, transaction.id());
        assert_eq!(event.kind, TransactionKind::Deposit);
        assert_eq!(event.from_account_id, None);
        assert_eq!(event.to_account_id, transaction.destination_account_id());
        assert_eq!(event.amount, transaction.amount().as_u64());
        assert_eq!(event.timestamp, transaction.created_at());
        assert_eq!(event.schema_version, "1.0");
    }

    #[test]
    fn transaction_event_from_withdraw_transaction() {
        let transaction = create_test_transaction(TransactionKind::Withdraw);
        let event: TransactionEventDto = TransactionEvent::from_transaction(&transaction).into();

        assert_eq!(event.transaction_id, transaction.id());
        assert_eq!(event.kind, TransactionKind::Withdraw);
        assert_eq!(event.from_account_id, transaction.source_account_id());
        assert_eq!(event.to_account_id, None);
        assert_eq!(event.amount, transaction.amount().as_u64());
    }

    #[test]
    fn transaction_event_from_transfer_transaction() {
        let transaction = create_test_transaction(TransactionKind::Transfer);
        let event: TransactionEventDto = TransactionEvent::from_transaction(&transaction).into();

        assert_eq!(event.transaction_id, transaction.id());
        assert_eq!(event.kind, TransactionKind::Transfer);
        assert_eq!(event.from_account_id, transaction.source_account_id());
        assert_eq!(event.to_account_id, transaction.destination_account_id());
        assert_eq!(event.amount, transaction.amount().as_u64());
    }
}
