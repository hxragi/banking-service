use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovFineCreatedEvent {
    pub fine_id: String,
    pub user_id: String,
    pub account_number: Option<String>,
    pub amount: u64,
    #[serde(default = "default_currency")]
    pub currency: String,
    pub reason: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketOrderPaidEvent {
    pub order_id: String,
    pub buyer_id: String,
    pub buyer_account_number: Option<String>,
    pub total_amount: u64,
    #[serde(default = "default_currency")]
    pub currency: String,
    pub seller_id: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DonateTopupEvent {
    pub donation_id: String,
    pub recipient_id: String,
    pub recipient_account_number: Option<String>,
    pub amount: u64,
    #[serde(default = "default_currency")]
    pub currency: String,
    pub message: Option<String>,
    #[serde(default)]
    pub is_anonymous: bool,
    pub idempotency_key: String,
}

fn default_currency() -> String {
    "A".to_string()
}

#[cfg(test)]
impl GovFineCreatedEvent {
    pub fn new(
        fine_id: impl Into<String>,
        user_id: impl Into<String>,
        amount: u64,
        reason: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            fine_id: fine_id.into(),
            user_id: user_id.into(),
            account_number: None,
            amount,
            currency: default_currency(),
            reason: reason.into(),
            idempotency_key: idempotency_key.into(),
        }
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
impl MarketOrderPaidEvent {
    pub fn new(
        order_id: impl Into<String>,
        buyer_id: impl Into<String>,
        total_amount: u64,
        seller_id: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            buyer_id: buyer_id.into(),
            buyer_account_number: None,
            total_amount,
            currency: default_currency(),
            seller_id: seller_id.into(),
            idempotency_key: idempotency_key.into(),
        }
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
impl DonateTopupEvent {
    pub fn new(
        donation_id: impl Into<String>,
        recipient_id: impl Into<String>,
        amount: u64,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            donation_id: donation_id.into(),
            recipient_id: recipient_id.into(),
            recipient_account_number: None,
            amount,
            currency: default_currency(),
            message: None,
            is_anonymous: false,
            idempotency_key: idempotency_key.into(),
        }
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gov_fine_created_event_serialization_roundtrip() {
        let event = GovFineCreatedEvent::new("fine-123", "user-456", 5000, "Speeding", "idem-789");

        let json = event.to_json().unwrap();
        let deserialized = GovFineCreatedEvent::from_json(&json).unwrap();

        assert_eq!(event.fine_id, deserialized.fine_id);
        assert_eq!(event.user_id, deserialized.user_id);
        assert_eq!(event.amount, deserialized.amount);
        assert_eq!(event.reason, deserialized.reason);
        assert_eq!(event.idempotency_key, deserialized.idempotency_key);
        assert_eq!(event.currency, "A");
    }

    #[test]
    fn market_order_paid_event_serialization_roundtrip() {
        let event =
            MarketOrderPaidEvent::new("order-456", "buyer-789", 15000, "seller-123", "idem-abc");

        let json = event.to_json().unwrap();
        let deserialized = MarketOrderPaidEvent::from_json(&json).unwrap();

        assert_eq!(event.order_id, deserialized.order_id);
        assert_eq!(event.buyer_id, deserialized.buyer_id);
        assert_eq!(event.total_amount, deserialized.total_amount);
        assert_eq!(event.seller_id, deserialized.seller_id);
        assert_eq!(event.idempotency_key, deserialized.idempotency_key);
        assert_eq!(event.currency, "A");
    }

    #[test]
    fn donate_topup_event_serialization_roundtrip() {
        let event = DonateTopupEvent::new("donation-789", "streamer-123", 1000, "idem-def");

        let json = event.to_json().unwrap();
        let deserialized = DonateTopupEvent::from_json(&json).unwrap();

        assert_eq!(event.donation_id, deserialized.donation_id);
        assert_eq!(event.recipient_id, deserialized.recipient_id);
        assert_eq!(event.amount, deserialized.amount);
        assert_eq!(event.idempotency_key, deserialized.idempotency_key);
        assert_eq!(event.currency, "A");
        assert!(!event.is_anonymous);
    }

    #[test]
    fn gov_fine_with_account_number() {
        let event = GovFineCreatedEvent {
            fine_id: "fine-001".to_string(),
            user_id: "user-001".to_string(),
            account_number: Some("ACC-123456".to_string()),
            amount: 1000,
            currency: "A".to_string(),
            reason: "Test".to_string(),
            idempotency_key: "key-001".to_string(),
        };

        assert_eq!(event.account_number, Some("ACC-123456".to_string()));
    }

    #[test]
    fn donate_topup_with_message_and_anonymous() {
        let event = DonateTopupEvent {
            donation_id: "don-001".to_string(),
            recipient_id: "streamer-001".to_string(),
            recipient_account_number: None,
            amount: 500,
            currency: "A".to_string(),
            message: Some("Great stream!".to_string()),
            is_anonymous: true,
            idempotency_key: "key-002".to_string(),
        };

        assert_eq!(event.message, Some("Great stream!".to_string()));
        assert!(event.is_anonymous);
    }
}
