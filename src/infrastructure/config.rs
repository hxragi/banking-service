use config::{Config, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database_url: String,
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<AppConfig> {
        let _ = dotenvy::dotenv().ok();
        let config = Config::builder()
            .add_source(Environment::default())
            .build()?;

        let app_config: AppConfig = config.try_deserialize()?;
        Ok(app_config)
    }
}
