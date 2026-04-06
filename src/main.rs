use tracing_subscriber::EnvFilter;

use crate::infrastructure::{config::AppConfig, database::create_pool};

mod application;
mod domain;
mod infrastructure;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .init();

    tracing::info!("starting service");

    let config = AppConfig::from_env()?;
    tracing::info!("config loaded");

    let _pool = create_pool(&config.database_url).await?;
    tracing::info!("database connected");

    tracing::info!("service started");
    Ok(())
}
