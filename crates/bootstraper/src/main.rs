use application::{
    change_tier::ChangeTierUseCase,
    create_account::CreateAccountUseCase,
    deposit::DepositUseCase,
    get_account::GetAccountUseCase,
    get_accounts::GetAccountsUseCase,
    get_transactions::GetTransactionsUseCase,
    ports::{BalanceCachePort, EventPublisher, MetricsPort},
    transaction_manager::{FinancialTransactionManager, RetryConfig},
    transfer::TransferUseCase,
    withdraw::WithdrawUseCase,
};
use axum::{Router, routing::get};
use infrastructure::{
    database::{
        pool::create_with_config, transaction::DbTransaction, transaction_manager::Manager,
    },
    generators::sequence_account_number_generator::SequenceAccountNumberGenerator,
    messaging::outbox_relay::OutboxRelay,
    repositories::sqlx_outbox_repository::SqlxOutboxRepository,
    services::retrying_transaction_manager::RetryingTransactionManager,
    settings::settings::AppConfig,
};
use infrastructure::{
    messaging::{
        kafka_consumer::{
            ConsumerCommand, DlqProducer, ExternalEventHandlerImpl, KafkaConsumerConfig,
            RetryTracker, start_consumer_with_retry,
        },
        kafka_event_publisher::KafkaEventPublisher,
    },
    observability::{
        health::{HealthChecker, health_check, readiness_check},
        metrics::{create_metrics_router, setup_metrics},
        sentry::init_sentry,
        signal::shutdown_signal,
        telemetry::init_telemetry,
    },
    repositories::{
        sqlx_account_repository::SqlxAccountRepository,
        sqlx_account_tx_repository::SqlxAccountTxRepository,
        sqlx_idempotency_repository::SqlxIdempotencyRepository,
        sqlx_owner_tier_repository::SqlxOwnerTierRepository,
        sqlx_transaction_repository::SqlxTransactionRepository,
        sqlx_transaction_write_repository::SqlxTransactionWriteRepository,
    },
    services::balance_cache::BalanceCache,
};
use presentation::{
    grpc::{
        bank_service::{BankGrpcService, bank},
        interceptor::InternalAuthInterceptor,
    },
    http::{
        handlers::{AccountHttpHandler, accounts::JwtDecoder},
        router::create_router,
    },
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

type TransactionManager =
    RetryingTransactionManager<FinancialTransactionManager<Manager, DbTransaction>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;

    let tracer_provider = init_telemetry("bank-service", &config)?;
    let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, "bank-service");

    let otlp_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(
            otlp_layer.and_then(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true),
            ),
        )
        .with(sentry_tracing::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    tracing::info!("starting service");

    let _sentry_guard = init_sentry(&config.sentry);

    let metrics = setup_metrics()?;
    let metrics_registry = metrics.registry.clone();

    let pool = create_with_config(
        &config.database.url,
        config.database.max_connections,
        config.database.connection_timeout_secs,
        config.database.default_statement_timeout_secs,
    )
    .await?;
    tracing::info!("database connected");

    sqlx::migrate!("../../../").run(&pool).await?;
    tracing::info!("migrations applied");

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let generator = Arc::new(SequenceAccountNumberGenerator::new(
        pool.clone(),
        "ACC".to_string(),
    ));

    let db_tx_manager = Manager::new(pool.clone());

    let transaction_repository = Arc::new(SqlxTransactionRepository::new(pool.clone()));

    let idempotency_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));

    let event_publisher: Arc<dyn EventPublisher + Send + Sync> = Arc::new(
        KafkaEventPublisher::new(&config.kafka)
            .map_err(|e| anyhow::anyhow!("failed to create Kafka event publisher: {}", e))?,
    );
    tracing::info!("kafka event publisher initialized");

    let retry_config = RetryConfig::new(
        config.transaction_retry.max_attempts,
        config.transaction_retry.base_delay_ms,
        config.transaction_retry.max_delay_ms,
    );

    let outbox_repo = Arc::new(SqlxOutboxRepository::new());

    let core_manager = Arc::new(
        FinancialTransactionManager::new(
            db_tx_manager.clone(),
            account_repo.clone(),
            Arc::new(SqlxAccountTxRepository),
            Arc::new(SqlxTransactionWriteRepository),
            idempotency_repo.clone(),
            Some(event_publisher.clone()),
            Some(metrics.clone() as Arc<dyn MetricsPort>),
        )
        .with_outbox_repository(outbox_repo),
    );

    let transaction_manager: Arc<TransactionManager> =
        Arc::new(RetryingTransactionManager::new(core_manager, retry_config));

    let outbox_relay = OutboxRelay::new(
        pool.clone(),
        event_publisher.clone(),
        Duration::from_secs(5),
        100,
    );
    tokio::spawn(async move {
        outbox_relay.run().await;
    });

    let balance_cache_ttl = Duration::from_secs(config.cache.balance_cache_ttl_secs);
    let balance_cache = BalanceCache::new(
        &config.dragonfly.url,
        balance_cache_ttl,
        Some(metrics.clone()),
    )
    .await?;

    let owner_tier_repo = Arc::new(SqlxOwnerTierRepository::new(pool.clone()));

    let create_account_use_case = Arc::new(CreateAccountUseCase::new(
        account_repo.clone(),
        generator.clone(),
        owner_tier_repo.clone(),
    ));

    let balance_cache_arc: Arc<dyn BalanceCachePort> = Arc::new(balance_cache.clone());

    let get_account_use_case = Arc::new(GetAccountUseCase::new(
        account_repo.clone(),
        balance_cache_arc.clone(),
    ));
    let get_accounts_use_case = Arc::new(GetAccountsUseCase::new(
        account_repo.clone(),
        balance_cache_arc.clone(),
    ));
    let get_transactions_use_case = Arc::new(GetTransactionsUseCase::new(
        account_repo.clone(),
        transaction_repository.clone(),
    ));

    let grpc_service = BankGrpcService::new(
        create_account_use_case.clone(),
        get_account_use_case.clone(),
        get_accounts_use_case.clone(),
        Arc::new(DepositUseCase::new(
            transaction_manager.clone(),
            balance_cache_arc.clone(),
        )),
        Arc::new(WithdrawUseCase::new(
            transaction_manager.clone(),
            balance_cache_arc.clone(),
        )),
        Arc::new(TransferUseCase::new(
            transaction_manager.clone(),
            balance_cache_arc.clone(),
        )),
        get_transactions_use_case.clone(),
        Arc::new(ChangeTierUseCase::new(
            account_repo.clone(),
            owner_tier_repo.clone(),
        )),
        metrics.clone(),
    );
    let grpc_addr: std::net::SocketAddr =
        format!("{}:{}", config.server.grpc_host, config.server.grpc_port).parse()?;
    let internal_auth_interceptor = InternalAuthInterceptor::new(config.internal_api_key.clone());
    let grpc_server = Server::builder()
        .add_service(
            bank::bank_service_server::BankServiceServer::with_interceptor(
                grpc_service,
                internal_auth_interceptor,
            ),
        )
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    let health_checker = Arc::new(HealthChecker::new(pool.clone()));

    let jwt_decoder = Arc::new(JwtDecoder::new(config.jwt_config.as_bytes()));

    let http_handler = Arc::new(AccountHttpHandler::new(
        create_account_use_case,
        get_account_use_case.clone(),
        get_accounts_use_case.clone(),
        Arc::new(DepositUseCase::new(
            transaction_manager.clone(),
            balance_cache_arc.clone(),
        )),
        Arc::new(WithdrawUseCase::new(
            transaction_manager.clone(),
            balance_cache_arc.clone(),
        )),
        Arc::new(TransferUseCase::new(
            transaction_manager.clone(),
            balance_cache_arc.clone(),
        )),
        get_transactions_use_case.clone(),
        jwt_decoder,
    ));

    let http_app = create_router(http_handler).merge(
        Router::new()
            .route("/health", get(health_check))
            .route("/ready", get(readiness_check))
            .with_state(health_checker.clone()),
    );

    let http_addr: std::net::SocketAddr =
        format!("{}:{}", config.server.http_host, config.server.http_port).parse()?;
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let http_server =
        axum::serve(http_listener, http_app).with_graceful_shutdown(shutdown_signal());

    tracing::info!(grpc = %grpc_addr, http = %http_addr, "starting servers");

    let consumer_config = KafkaConsumerConfig {
        bootstrap_servers: config.kafka.bootstrap_servers.clone(),
        group_id: "bank-service-external-events".to_string(),
        topics: vec![
            "gov.fine.created".to_string(),
            "market.order.paid".to_string(),
            "donate.topup".to_string(),
        ],
        session_timeout_ms: 10000,
        auto_offset_reset: "earliest".to_string(),
    };

    let deposit_use_case = Arc::new(DepositUseCase::new(
        transaction_manager.clone(),
        balance_cache_arc.clone(),
    ));
    let withdraw_use_case = Arc::new(WithdrawUseCase::new(
        transaction_manager.clone(),
        balance_cache_arc.clone(),
    ));

    let event_handler = Arc::new(ExternalEventHandlerImpl::new(
        deposit_use_case,
        withdraw_use_case,
        account_repo.clone(),
    ));

    let (consumer_shutdown_tx, consumer_shutdown_rx) = mpsc::channel(1);

    let kafka_check_interval_secs = 30;
    let consumer_shutdown_token = CancellationToken::new();

    let retry_tracker = RetryTracker::new(&config.dragonfly.url).await;
    let dlq_producer = DlqProducer::new(&config.kafka.bootstrap_servers);

    let consumer_handle = match (retry_tracker, dlq_producer) {
        (Ok(retry_tracker), Ok(dlq_producer)) => {
            let retry_tracker = Arc::new(retry_tracker);
            let dlq_producer = Arc::new(dlq_producer);
            match start_consumer_with_retry(
                consumer_config,
                event_handler,
                consumer_shutdown_rx,
                kafka_check_interval_secs,
                config.kafka.consumer_connect_max_retries,
                config.kafka.consumer_connect_timeout_secs,
                retry_tracker,
                dlq_producer,
            )
            .await
            {
                Ok(Some(handle)) => {
                    tracing::info!("kafka consumer started immediately for external events");
                    Some(handle)
                }
                Ok(None) => {
                    tracing::warn!(
                        "service started in degraded mode - kafka unavailable. external event processing is temporarily disabled."
                    );
                    let shutdown_token = consumer_shutdown_token.clone();
                    Some(tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(30)) => {},
                                _ = shutdown_token.cancelled() => {
                                    tracing::info!("degraded mode retry loop cancelled, shutting down gracefully");
                                    break;
                                }
                            }
                        }
                    }))
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "failed to initialize kafka consumer retry mechanism. continuing without external event processing."
                    );
                    None
                }
            }
        }
        _ => {
            tracing::warn!(
                "service started in degraded mode - kafka/redis infrastructure unavailable. external event processing is temporarily disabled."
            );
            let shutdown_token = consumer_shutdown_token.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {},
                        _ = shutdown_token.cancelled() => {
                            tracing::info!("degraded mode retry loop cancelled, shutting down gracefully");
                            break;
                        }
                    }
                }
            }))
        }
    };

    let cleanup_pool = pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        let repo = SqlxIdempotencyRepository::new(cleanup_pool);
        let batch_size = 1000i64;

        loop {
            interval.tick().await;
            match repo.cleanup_expired_batched(batch_size).await {
                Ok(deleted) => {
                    if deleted > 0 {
                        tracing::info!(
                            rows_deleted = deleted,
                            "cleaned up expired idempotency keys"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(err = ?e, "failed to cleanup expired idempotency keys");
                }
            }
        }
    });

    let metrics_addr: std::net::SocketAddr = config.server.metrics_addr.parse()?;
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    let metrics_app = create_metrics_router(metrics_registry);
    let metrics_server =
        axum::serve(metrics_listener, metrics_app).with_graceful_shutdown(shutdown_signal());

    tracing::info!(metrics = %metrics_addr, "metrics server started");

    tokio::try_join!(
        async { grpc_server.await.map_err(anyhow::Error::from) },
        async { http_server.await.map_err(anyhow::Error::from) },
        async { metrics_server.await.map_err(anyhow::Error::from) },
    )?;

    tracing::info!("http and grpc servers stopped, shutting down consumer");

    if let Some(handle) = consumer_handle {
        tracing::info!("sending shutdown command to kafka consumer");
        if let Err(e) = consumer_shutdown_tx.send(ConsumerCommand::Shutdown).await {
            tracing::warn!(error = %e, "failed to send shutdown command to consumer, it may already be stopped");
        }

        consumer_shutdown_token.cancel();

        tracing::info!("waiting for kafka consumer to finish");
        if let Err(e) = tokio::time::timeout(Duration::from_secs(30), handle).await {
            tracing::warn!(error = %e, "consumer shutdown timed out or failed");
        } else {
            tracing::info!("kafka consumer stopped gracefully");
        }
    }

    tracing::info!("service stopped");

    if let Err(e) = tracer_provider.shutdown() {
        tracing::warn!(error = ?e, "error shutting down tracer provider");
    }

    Ok(())
}
