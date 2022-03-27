use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use axum::{
    extract::Path,
    routing::{delete, get, post, patch},
    Router,
};

use portfolio::{Config, MetadataBackend};
use portfolio::Result;
use portfolio::metadata::PostgresConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let mut dev_config = File::open("./dev-config.yml")?;
    let mut s = String::new();
    dev_config.read_to_string(&mut s)?;
    let config: Config = serde_yaml::from_str(&s)?;
    let metadata = match config.metadata {
        MetadataBackend::Postgres(cfg) => cfg.new_metadata().await?,
    };

    serve().await;
    Ok(())
}

async fn serve() {
    let blobs = Router::new().route( "/:digest", get(notimplemented).delete(notimplemented).head(notimplemented))
        .route("/uploads/", post(notimplemented))
        .route("/uploads/:reference", patch(notimplemented).put(notimplemented));
    let manifests = Router::new().route("/:reference", delete(notimplemented).put(notimplemented).get(notimplemented).head(notimplemented));

    let tags = Router::new().route("/list", get(notimplemented));

    let app = Router::new().route("/v2", get(hello_world))
        .nest("/v2/:repository/blobs", blobs)
        .nest("/v2/:repository/manifests", manifests)
        .nest("/v2/:repository/tags", tags);

    axum::Server::bind(&"0.0.0.0:13030".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}


async fn notimplemented(Path(params): Path<HashMap<String, String>>) -> String { 
    format!("not implemented\n{:?}", params)
}

async fn hello_world() -> String {
    "hello world".to_string()
}
