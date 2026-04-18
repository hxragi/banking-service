use std::time::Duration;

use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::infrastructure::services::balance_cache::BalanceCache;

pub async fn setup_redis() -> (BalanceCache, ContainerAsync<Redis>) {
    let container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get port");

    let redis_url = format!("redis://{}:{}", host, port);
    let cache = BalanceCache::new(&redis_url, Duration::from_secs(60), None)
        .await
        .expect("Failed to connect to Redis container");

    (cache, container)
}

pub async fn create_test_balance_cache() -> (BalanceCache, ContainerAsync<Redis>) {
    setup_redis().await
}
