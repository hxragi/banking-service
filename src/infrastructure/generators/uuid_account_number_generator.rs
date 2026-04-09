use async_trait::async_trait;
use uuid::Uuid;

use crate::application::ports::{AccountNumberGenerator, AccountNumberGeneratorError};
use crate::domain::account_number::AccountNumber;

pub struct UuidAccountNumberGenerator;

#[async_trait]
impl AccountNumberGenerator for UuidAccountNumberGenerator {
    async fn generate(&self) -> Result<AccountNumber, AccountNumberGeneratorError> {
        let uuid = Uuid::new_v4();
        let short = &uuid.simple().to_string()[..8];
        let value = format!("ACC-{short}");

        AccountNumber::new(&value).map_err(|_| AccountNumberGeneratorError::GenerationFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generate_produces_valid_account_number() {
        let generator = UuidAccountNumberGenerator;
        let number = generator.generate().await.unwrap();
        assert!(number.as_str().starts_with("ACC-"));
        assert_eq!(number.as_str().len(), 12);
    }

    #[tokio::test]
    async fn generate_returns_unique_values() {
        let generator = UuidAccountNumberGenerator;
        let a = generator.generate().await.unwrap();
        let b = generator.generate().await.unwrap();
        assert_ne!(a, b);
    }
}
