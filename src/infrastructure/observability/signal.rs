use tokio::signal;

pub async fn shutdown_signal() {
    let ctrl_c = async {
        match signal::ctrl_c().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("Failed to install Ctrl+C handler: {}", e);
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(e) => {
                tracing::error!("Failed to install terminate signal handler: {}", e);
                std::future::pending().await
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = async { std::future::pending().await };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("received shutdown signal, stopping gracefully");
}
