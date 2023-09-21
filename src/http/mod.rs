use std::collections::HashMap;

use axum::{
    extract::{Extension, Path},
    routing::get,
    Router,
};

pub(crate) mod blobs;
mod manifests;
pub(crate) mod middleware;
mod tags;

use crate::errors::Result;
use crate::metadata::PostgresMetadata;
use crate::objects::ObjectStore;
use crate::registry::Registry;

pub async fn serve<O: ObjectStore>(
    metadata: PostgresMetadata,
    objects: O,
) -> Result<()> {
    let registry = Registry::new("meow".to_string(), metadata, objects).await?;
    let blobs = blobs::router::<O>()
        .layer(Extension(registry.clone()));
    let manifests = manifests::router()
        .layer(Extension(registry.clone()));
    let tags = tags::router()
        .layer(Extension(registry.clone()));

    let app = Router::new()
        .route("/v2", get(hello_world))
        .nest("/v2/:repository/blobs", blobs)
        .nest("/v2/:repository/manifests", manifests)
        .nest("/v2/:repository/tags", tags);

    axum::Server::bind(&"0.0.0.0:13030".parse()?)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn notimplemented(Path(params): Path<HashMap<String, String>>) -> String {
    format!("not implemented\n{:?}", params)
}

async fn hello_world() -> String {
    "hello world".to_string()
}
