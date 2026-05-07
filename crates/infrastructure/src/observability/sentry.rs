use sentry::ClientOptions;
use sentry::Level;

use crate::settings::settings::SentryConfig;

pub fn init_sentry(config: &Option<SentryConfig>) -> Option<sentry::ClientInitGuard> {
    let sentry_config = config.as_ref()?;

    let options = ClientOptions {
        dsn: sentry_config.dsn.parse().ok(),
        environment: Some(sentry_config.environment.clone().into()),
        sample_rate: sentry_config.sample_rate,
        before_send: Some(std::sync::Arc::new(|event| match event.level {
            Level::Error | Level::Fatal => Some(event),
            _ => None,
        })),
        ..Default::default()
    };

    let guard = sentry::init(options);
    tracing::info!("sentry initialized with error/critical level filtering");
    Some(guard)
}
