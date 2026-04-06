use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let options = PgPoolOptions::new().max_connections(5);

    let pool = options.connect(database_url).await?;
    Ok(pool)
}
