use std::sync::Arc;

use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use crate::application::create_account::CreateAccountUseCase;
use crate::infrastructure::{
    config::AppConfig,
    database::create_pool,
    generators::uuid_account_number_generator::UuidAccountNumberGenerator,
    grpc::{bank, bank_service::BankGrpcService},
    http,
    repositories::sqlx_account_repository::SqlxAccountRepository,
};

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

    let pool = create_pool(&config.database_url).await?;
    tracing::info!("database connected");

    let repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let generator = Arc::new(UuidAccountNumberGenerator);

    let create_uc_grpc = CreateAccountUseCase::new(repo.clone(), generator.clone());
    let grpc_service = BankGrpcService::new(Arc::new(create_uc_grpc));
    let grpc_addr: std::net::SocketAddr = "[::1]:50051".parse()?;
    let grpc_server = Server::builder()
        .add_service(bank::bank_service_server::BankServiceServer::new(
            grpc_service,
        ))
        .serve(grpc_addr);

    let create_uc_http = CreateAccountUseCase::new(repo.clone(), generator.clone());
    let http_app = http::create_router(Arc::new(create_uc_http));
    let http_addr: std::net::SocketAddr = "0.0.0.0:8080".parse()?;
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_app);

    tracing::info!(grpc = %grpc_addr, http = %http_addr, "starting servers");

    let grpc_fut = async { grpc_server.await.map_err(anyhow::Error::from) };
    let http_fut = async { http_server.await.map_err(anyhow::Error::from) };

    tokio::try_join!(grpc_fut, http_fut)?;

    Ok(())
}
