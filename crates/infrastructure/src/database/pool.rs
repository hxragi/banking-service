use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;

pub async fn create_with_config(
    database_url: &str,
    max_connections: u32,
    connection_timeout_secs: u64,
    statement_timeout_secs: u64,
) -> anyhow::Result<PgPool> {
    let options = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(connection_timeout_secs))
        .after_connect(move |conn, _| {
            let timeout = statement_timeout_secs;
            Box::pin(async move {
                sqlx::query(&format!("SET statement_timeout = {}", timeout * 1000))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        });

    let pool = options.connect(database_url).await?;
    Ok(pool)
}
