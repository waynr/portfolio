use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use anyhow::Result;
use axum::middleware;
use clap::Parser;

use portfolio_backend_postgres::{PgRepository, PgRepositoryFactory};
use portfolio_http::{add_basic_repository_extensions, Portfolio};

mod config;
use crate::config::{Config, RepositoryBackend};

#[derive(Parser)]
struct Cli {
    #[arg(short, long)]
    config_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .compact()
        .init();

    tracing::info!("info enabled");
    tracing::warn!("warning enabled");
    tracing::debug!("debug enabled");
    tracing::trace!("trace enabled");

    // load configuration
    let mut dev_config = File::open(cli.config_file.unwrap_or("./dev-config.yml".into()))?;
    let mut s = String::new();
    dev_config.read_to_string(&mut s)?;
    let config: Config = serde_yaml::from_str(&s)?;

    // initialize persistence layer
    let portfolio = match config.backend {
        RepositoryBackend::Postgres(cfg) => {
            let manager = cfg.get_manager().await?;
            Portfolio::<PgRepositoryFactory, PgRepository>::new(manager)
        }
    };

    if let Some(repositories) = config.static_repositories {
        portfolio
            .initialize_static_repositories(repositories)
            .await?;
    }

    let router = match portfolio.router() {
        Err(e) => return Err(e.into()),
        Ok(r) => r,
    };

    let router = router.route_layer(middleware::from_fn_with_state(
        portfolio.clone(),
        add_basic_repository_extensions,
    ));

    // run HTTP server
    axum::Server::bind(&"0.0.0.0:13030".parse()?)
        .serve(router.into_make_service())
        .await?;

    Ok(())
}
