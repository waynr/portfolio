use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Extension, Path},
    routing::get,
    Router,
};

pub(crate) mod blobs;
mod manifests;
mod tags;
use crate::metadata::PostgresMetadata;
use crate::objects::S3;

pub async fn serve(metadata: Arc<PostgresMetadata>, objects: Arc<S3>) {
    let blobs = blobs::router()
        .layer(Extension(metadata.clone()))
        .layer(Extension(objects.clone()));
    let manifests = manifests::router()
        .layer(Extension(metadata.clone()))
        .layer(Extension(objects.clone()));
    let tags = tags::router()
        .layer(Extension(metadata.clone()))
        .layer(Extension(objects.clone()));

    let app = Router::new()
        .route("/v2", get(hello_world))
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
