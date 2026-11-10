use ores_otel_collector_staging::{router, CollectorState};
use reqwest::Url;
use std::{env, net::SocketAddr};

#[tokio::main]
async fn main() {
    if start().await.is_err() {
        // No runtime values, credentials or endpoint contents in diagnostics.
        eprintln!("staging collector startup/configuration failed");
        std::process::exit(1);
    }
}

async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let bind: SocketAddr = env::var("ORES_OTEL_STAGING_BIND")?.parse()?;
    if !bind.ip().is_loopback() {
        return Err("staging collector only accepts loopback bind".into());
    }
    let tenant = env::var("ORES_OTEL_STAGING_TENANT")?;
    let bearer = env::var("ORES_OTEL_STAGING_BEARER")?;
    let exporter: Url = env::var("ORES_OTEL_STAGING_EXPORTER")?.parse()?;
    let exporter_bearer = env::var("ORES_OTEL_STAGING_EXPORTER_BEARER")?;
    let state = CollectorState::new(tenant, bearer, exporter, exporter_bearer)?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; })
        .await?;
    Ok(())
}
