use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use clap::Parser;
use portfolio_core::{http, Result};
use portfolio_backend_postgres::{PgS3Repository, PgS3RepositoryFactory};

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
    match config.backend {
        RepositoryBackend::PostgresS3(cfg) => {
            let manager = cfg.get_manager().await?;
            let portfolio = portfolio_core::Portfolio::new(manager);

            if let Some(repositories) = config.static_repositories {
                portfolio
                    .initialize_static_repositories(repositories)
                    .await?;
            }

            // run HTTP server
            http::serve::<PgS3RepositoryFactory, PgS3Repository>(portfolio).await
        }
    }
}
