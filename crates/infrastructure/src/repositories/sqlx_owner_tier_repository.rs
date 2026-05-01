use sqlx::PgPool;
use time::OffsetDateTime;

use crate::database::error::classify_sqlx;
use application::ports::{OwnerTierRepository, OwnerTierRepositoryError};
use domain::org_id::OrgId;
use domain::owner::Owner;
use domain::owner_tier::OwnerTier;
use domain::tier::Tier;
use domain::user_id::UserId;

pub struct SqlxOwnerTierRepository {
    pool: PgPool,
}

impl SqlxOwnerTierRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn owner_to_db(owner: &Owner) -> (String, String) {
    match owner {
        Owner::User(user_id) => ("user".to_string(), user_id.as_str().to_string()),
        Owner::Org(org_id) => ("org".to_string(), org_id.as_str().to_string()),
    }
}

fn row_to_owner_tier(
    row: (String, String, i32, OffsetDateTime, OffsetDateTime),
) -> Result<OwnerTier, OwnerTierRepositoryError> {
    let (owner_type, owner_id, tier_val, created_at, updated_at) = row;

    let owner = match owner_type.as_str() {
        "user" => Owner::User(UserId::new(&owner_id).map_err(|_| {
            OwnerTierRepositoryError::OperationFailed {
                operation: "owner_conversion".to_string(),
                reason: "invalid user id".to_string(),
            }
        })?),
        "org" => Owner::Org(OrgId::new(&owner_id).map_err(|_| {
            OwnerTierRepositoryError::OperationFailed {
                operation: "owner_conversion".to_string(),
                reason: "invalid org id".to_string(),
            }
        })?),
        _ => {
            return Err(OwnerTierRepositoryError::OperationFailed {
                operation: "owner_conversion".to_string(),
                reason: format!("unknown owner type: {}", owner_type),
            });
        }
    };

    let tier = match tier_val {
        1 => Tier::Basic,
        2 => Tier::Premium,
        3 => Tier::Elite,
        _ => {
            return Err(OwnerTierRepositoryError::OperationFailed {
                operation: "tier_conversion".to_string(),
                reason: format!("unknown tier value: {}", tier_val),
            });
        }
    };

    Ok(OwnerTier::new(owner, tier, created_at, updated_at))
}

#[async_trait::async_trait]
impl OwnerTierRepository for SqlxOwnerTierRepository {
    async fn get_or_default(&self, owner: &Owner) -> Result<OwnerTier, OwnerTierRepositoryError> {
        let (owner_type, owner_id) = owner_to_db(owner);

        let result: Result<
            Option<(String, String, i32, OffsetDateTime, OffsetDateTime)>,
            sqlx::Error,
        > = sqlx::query_as(
            r#"
            SELECT owner_type, owner_id, tier, created_at, updated_at
            FROM owner_tiers
            WHERE owner_type = $1 AND owner_id = $2
            "#,
        )
        .bind(&owner_type)
        .bind(&owner_id)
        .fetch_optional(&self.pool)
        .await;

        match result {
            Ok(Some(row)) => row_to_owner_tier(row),
            Ok(None) => Ok(OwnerTier::new(
                owner.clone(),
                Tier::Basic,
                OffsetDateTime::now_utc(),
                OffsetDateTime::now_utc(),
            )),
            Err(e) => {
                let context = classify_sqlx(&e, "get_or_default");
                tracing::warn!(err = %context, owner = ?owner, "failed to get owner tier");
                Err(OwnerTierRepositoryError::OperationFailed {
                    operation: "get_or_default".to_string(),
                    reason: context,
                })
            }
        }
    }

    async fn set_tier(
        &self,
        owner: &Owner,
        tier: Tier,
    ) -> Result<OwnerTier, OwnerTierRepositoryError> {
        let (owner_type, owner_id) = owner_to_db(owner);
        let tier_value = tier.as_i32();
        let now = OffsetDateTime::now_utc();

        let result: Result<(String, String, i32, OffsetDateTime, OffsetDateTime), sqlx::Error> =
            sqlx::query_as(
                r#"
            INSERT INTO owner_tiers (owner_type, owner_id, tier, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $4)
            ON CONFLICT (owner_type, owner_id) DO UPDATE SET
                tier = $3,
                updated_at = $4
            RETURNING owner_type, owner_id, tier, created_at, updated_at
            "#,
            )
            .bind(&owner_type)
            .bind(&owner_id)
            .bind(tier_value)
            .bind(now)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => row_to_owner_tier(row),
            Err(e) => {
                let context = classify_sqlx(&e, "set_tier");
                tracing::error!(err = %context, owner = ?owner, tier = ?tier, "failed to set owner tier");
                Err(OwnerTierRepositoryError::OperationFailed {
                    operation: "set_tier".to_string(),
                    reason: context,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owner_to_db_user() {
        let owner = Owner::User(UserId::new("user-123").unwrap());
        let (ty, id) = owner_to_db(&owner);
        assert_eq!(ty, "user");
        assert_eq!(id, "user-123");
    }

    #[test]
    fn test_owner_to_db_org() {
        let owner = Owner::Org(OrgId::new("org-456").unwrap());
        let (ty, id) = owner_to_db(&owner);
        assert_eq!(ty, "org");
        assert_eq!(id, "org-456");
    }
}
