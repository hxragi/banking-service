use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    application::ports::AccountRepositoryError,
    domain::{
        account::Account, account_number::AccountNumber, balance::Balance, org_id::OrgId,
        owner::Owner, user_id::UserId,
    },
};

pub fn row_to_account(row: &PgRow) -> Result<Account, AccountRepositoryError> {
    let id: Uuid = row
        .try_get("id")
        .map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "missing id".to_string(),
        })?;
    let number: String =
        row.try_get("number")
            .map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "row_conversion".to_string(),
                reason: "missing number".to_string(),
            })?;
    let user_id: Option<String> =
        row.try_get("user_id")
            .map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "row_conversion".to_string(),
                reason: "missing user_id".to_string(),
            })?;
    let org_id: Option<String> =
        row.try_get("org_id")
            .map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "row_conversion".to_string(),
                reason: "missing org_id".to_string(),
            })?;
    let balance: i64 =
        row.try_get("balance")
            .map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "row_conversion".to_string(),
                reason: "missing balance".to_string(),
            })?;
    let created_at: time::OffsetDateTime =
        row.try_get("created_at")
            .map_err(|_| AccountRepositoryError::OperationFailed {
                operation: "row_conversion".to_string(),
                reason: "missing created_at".to_string(),
            })?;

    let owner =
        match (user_id, org_id) {
            (Some(uid), None) => Owner::User(UserId::new(&uid).map_err(|_| {
                AccountRepositoryError::OperationFailed {
                    operation: "row_conversion".to_string(),
                    reason: "invalid user_id".to_string(),
                }
            })?),
            (None, Some(oid)) => Owner::Org(OrgId::new(&oid).map_err(|_| {
                AccountRepositoryError::OperationFailed {
                    operation: "row_conversion".to_string(),
                    reason: "invalid org_id".to_string(),
                }
            })?),
            _ => {
                return Err(AccountRepositoryError::OperationFailed {
                    operation: "row_conversion".to_string(),
                    reason: "ambiguous owner".to_string(),
                });
            }
        };

    let balance_u64 =
        u64::try_from(balance).map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "negative balance in database".to_string(),
        })?;

    Ok(Account::new(
        id,
        AccountNumber::new(&number).map_err(|_| AccountRepositoryError::OperationFailed {
            operation: "row_conversion".to_string(),
            reason: "invalid account number".to_string(),
        })?,
        owner,
        Balance::new(balance_u64),
        created_at,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_to_account_error_is_account_repository_error() {
        let _fn: fn(&sqlx::postgres::PgRow) -> Result<Account, AccountRepositoryError> =
            row_to_account;
    }
}
