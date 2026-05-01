use crate::{
    deposit::{DepositInput, DepositPort},
    ports::{AccountRepository, OperationError},
    withdraw::{WithdrawInput, WithdrawPort},
};
use domain::{account_number::AccountNumber, amount::Amount, owner::Owner, user_id::UserId};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum ExternalEventError {
    #[error("account not found for user {user_id}")]
    AccountNotFound { user_id: String },
    #[error("insufficient funds for user {user_id}")]
    InsufficientFunds { user_id: String },
    #[error("invalid amount: {0}")]
    InvalidAmount(String),
    #[error("invalid account number format: {0}")]
    InvalidAccountFormat(String),
    #[error("account temporarily unavailable")]
    AccountUnavailable,
    #[error("operation failed: {0}")]
    OperationFailed(String),
}

#[derive(Debug)]
pub struct ProcessGovFineInput {
    pub fine_id: String,
    pub user_id: String,
    pub account_number: Option<String>,
    pub amount: u64,
}

#[derive(Debug)]
pub struct ProcessMarketOrderInput {
    pub order_id: String,
    pub buyer_id: String,
    pub buyer_account_number: Option<String>,
    pub total_amount: u64,
    pub seller_id: String,
}

#[derive(Debug)]
pub struct ProcessDonateTopupInput {
    pub donation_id: String,
    pub recipient_id: String,
    pub recipient_account_number: Option<String>,
    pub amount: u64,
    pub is_anonymous: bool,
}

pub struct ExternalEventProcessor {
    deposit_port: Arc<dyn DepositPort>,
    withdraw_port: Arc<dyn WithdrawPort>,
    account_repo: Arc<dyn AccountRepository + Send + Sync>,
}

impl ExternalEventProcessor {
    pub fn new(
        deposit_port: Arc<dyn DepositPort>,
        withdraw_port: Arc<dyn WithdrawPort>,
        account_repo: Arc<dyn AccountRepository + Send + Sync>,
    ) -> Self {
        Self {
            deposit_port,
            withdraw_port,
            account_repo,
        }
    }

    #[tracing::instrument(
        skip(self),
        fields(fine_id = %input.fine_id, user_id = %input.user_id, amount = %input.amount)
    )]
    pub async fn process_gov_fine(
        &self,
        input: ProcessGovFineInput,
    ) -> Result<(), ExternalEventError> {
        let account_number = self
            .resolve_account_number(input.account_number.as_deref(), &input.user_id)
            .await?;

        let amount = Amount::new(input.amount)
            .map_err(|_| ExternalEventError::InvalidAmount(input.amount.to_string()))?;

        let idempotency_key = format!("gov.fine:{}", input.fine_id);

        let withdraw_input = WithdrawInput {
            account_number,
            amount,
            idempotency_key: Some(idempotency_key),
        };

        match self.withdraw_port.execute(withdraw_input).await {
            Ok(account) => {
                tracing::info!(
                    fine_id = %input.fine_id,
                    account_number = %account.number(),
                    new_balance = %account.balance(),
                    "fine payment processed successfully"
                );
                Ok(())
            }
            Err(OperationError::NotFound { .. }) => Err(ExternalEventError::AccountNotFound {
                user_id: input.user_id,
            }),
            Err(OperationError::InsufficientFunds) => Err(ExternalEventError::InsufficientFunds {
                user_id: input.user_id,
            }),
            Err(OperationError::Unavailable { .. }) => Err(ExternalEventError::AccountUnavailable),
            Err(e) => Err(ExternalEventError::OperationFailed(e.to_string())),
        }
    }

    #[tracing::instrument(
        skip(self),
        fields(order_id = %input.order_id, buyer_id = %input.buyer_id, amount = %input.total_amount)
    )]
    pub async fn process_market_order(
        &self,
        input: ProcessMarketOrderInput,
    ) -> Result<(), ExternalEventError> {
        let account_number = self
            .resolve_account_number(input.buyer_account_number.as_deref(), &input.buyer_id)
            .await?;

        let amount = Amount::new(input.total_amount)
            .map_err(|_| ExternalEventError::InvalidAmount(input.total_amount.to_string()))?;

        let idempotency_key = format!("market.order:{}", input.order_id);

        let withdraw_input = WithdrawInput {
            account_number,
            amount,
            idempotency_key: Some(idempotency_key),
        };

        match self.withdraw_port.execute(withdraw_input).await {
            Ok(account) => {
                tracing::info!(
                    order_id = %input.order_id,
                    buyer_account = %account.number(),
                    new_balance = %account.balance(),
                    seller_id = %input.seller_id,
                    "market order payment processed successfully"
                );
                Ok(())
            }
            Err(OperationError::NotFound { .. }) => Err(ExternalEventError::AccountNotFound {
                user_id: input.buyer_id,
            }),
            Err(OperationError::InsufficientFunds) => Err(ExternalEventError::InsufficientFunds {
                user_id: input.buyer_id,
            }),
            Err(OperationError::Unavailable { .. }) => Err(ExternalEventError::AccountUnavailable),
            Err(e) => Err(ExternalEventError::OperationFailed(e.to_string())),
        }
    }

    #[tracing::instrument(
        skip(self),
        fields(donation_id = %input.donation_id, recipient_id = %input.recipient_id, amount = %input.amount)
    )]
    pub async fn process_donate_topup(
        &self,
        input: ProcessDonateTopupInput,
    ) -> Result<(), ExternalEventError> {
        let account_number = self
            .resolve_account_number(
                input.recipient_account_number.as_deref(),
                &input.recipient_id,
            )
            .await?;

        let amount = Amount::new(input.amount)
            .map_err(|_| ExternalEventError::InvalidAmount(input.amount.to_string()))?;

        let idempotency_key = format!("donate:{}", input.donation_id);

        let deposit_input = DepositInput {
            account_number,
            amount,
            idempotency_key: Some(idempotency_key),
        };

        match self.deposit_port.execute(deposit_input).await {
            Ok(account) => {
                tracing::info!(
                    donation_id = %input.donation_id,
                    recipient_account = %account.number(),
                    new_balance = %account.balance(),
                    amount = %input.amount,
                    anonymous = %input.is_anonymous,
                    "donation topup processed successfully"
                );
                Ok(())
            }
            Err(OperationError::NotFound { .. }) => Err(ExternalEventError::AccountNotFound {
                user_id: input.recipient_id,
            }),
            Err(OperationError::Unavailable { .. }) => Err(ExternalEventError::AccountUnavailable),
            Err(e) => Err(ExternalEventError::OperationFailed(e.to_string())),
        }
    }

    async fn find_account_by_user_id(
        &self,
        user_id: &str,
    ) -> Result<AccountNumber, ExternalEventError> {
        let owner_id = UserId::new(user_id).map_err(|_| ExternalEventError::AccountNotFound {
            user_id: user_id.to_string(),
        })?;
        let owner = Owner::User(owner_id);

        let accounts = self.account_repo.find_by_owner(&owner).await.map_err(|e| {
            tracing::error!(error = ?e, user_id = %user_id, "Failed to query accounts by owner");
            ExternalEventError::AccountUnavailable
        })?;

        accounts
            .into_iter()
            .next()
            .map(|account| account.number().clone())
            .ok_or_else(|| ExternalEventError::AccountNotFound {
                user_id: user_id.to_string(),
            })
    }

    async fn resolve_account_number(
        &self,
        explicit_account: Option<&str>,
        user_id: &str,
    ) -> Result<AccountNumber, ExternalEventError> {
        if let Some(account_number) = explicit_account {
            AccountNumber::new(account_number)
                .map_err(|_| ExternalEventError::InvalidAccountFormat(account_number.to_string()))
        } else {
            self.find_account_by_user_id(user_id).await
        }
    }
}
