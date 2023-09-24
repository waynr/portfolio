use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use clap::Parser;

use portfolio::http;
use portfolio::Result;
use portfolio::{Config, MetadataBackend, ObjectsBackend};

#[derive(Parser)]
struct Cli {
    #[arg(short, long)]
    config_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // load configuration
    let mut dev_config = File::open(cli.config_file.unwrap_or("./dev-config.yml".into()))?;
    let mut s = String::new();
    dev_config.read_to_string(&mut s)?;
    let config: Config = serde_yaml::from_str(&s)?;

    // initialize persistence layer
    let mut metadata = match config.metadata {
        MetadataBackend::Postgres(cfg) => cfg.new_metadata().await?,
    };
    let objects = match config.objects {
        ObjectsBackend::S3(cfg) => cfg.new_objects().await?,
    };

    if let Some(registries) = config.static_registries {
        metadata.initialize_static_registries(registries).await?;
    }

    let portfolio = portfolio::Portfolio::new(objects, metadata);

    // run HTTP server
    http::serve(portfolio).await
}
