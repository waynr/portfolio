use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, TypedHeader},
    headers::ContentLength,
    http::header::{HeaderMap, HeaderName, HeaderValue},
    http::Request,
    routing::{delete, get, patch, post},
    Router,
};
use axum_macros::debug_handler;

use http::StatusCode;
use hyper::body::Body;

use portfolio::metadata::PostgresMetadata;
use portfolio::objects::{StreamObjectBody, S3};
use portfolio::OciDigest;
use portfolio::Result;
use portfolio::{Config, MetadataBackend, ObjectsBackend};

#[tokio::main]
async fn main() -> Result<()> {
    let mut dev_config = File::open("./dev-config.yml")?;
    let mut s = String::new();
    dev_config.read_to_string(&mut s)?;
    let config: Config = serde_yaml::from_str(&s)?;
    let metadata = match config.metadata {
        MetadataBackend::Postgres(cfg) => cfg.new_metadata().await?,
    };

    let objects = match config.objects {
        ObjectsBackend::S3(cfg) => cfg.new_objects().await?,
    };

    serve(Arc::new(metadata), Arc::new(objects)).await;
    Ok(())
}

async fn serve(metadata: Arc<PostgresMetadata>, objects: Arc<S3>) {
    let blobs = Router::new()
        .route(
            "/:digest",
            get(notimplemented)
                .delete(notimplemented)
                .head(notimplemented),
        )
        .route("/uploads/", post(blobs_uploads_post))
        .layer(Extension(metadata))
        .layer(Extension(objects))
        .route(
            "/uploads/:reference",
            patch(notimplemented).put(notimplemented),
        );
    let manifests = Router::new().route(
        "/:reference",
        delete(notimplemented)
            .put(notimplemented)
            .get(notimplemented)
            .head(notimplemented),
    );

    let tags = Router::new().route("/list", get(notimplemented));

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

#[debug_handler]
async fn blobs_uploads_post(
    Path(path_params): Path<HashMap<String, String>>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
) -> (StatusCode, HeaderMap, &'static str) {
    let mut headers = HeaderMap::new();
    let repo_name = path_params.get("repository").unwrap();
    // <algo>:<digest>
    let blob_digest_string = query_params.get("digest").unwrap();
    let blob_digest: OciDigest = blob_digest_string.try_into().unwrap();
    let location = format!(
        "http://127.0.0.1:13030/v2/{}/blobs/{}",
        repo_name, blob_digest_string,
    );

    // insert metadata
    metadata
        .clone()
        .insert_blob(blob_digest_string)
        .await
        .unwrap();

    // upload blob
    let digester = blob_digest.digester();
    let body = StreamObjectBody::from_body(request.into_body(), digester);
    objects.clone().upload_blob(body.into()).await.unwrap();

    // validate digest
    // validate content length

    headers.insert(
        HeaderName::from_static("Location"),
        HeaderValue::from_str(&location).unwrap(),
    );
    (StatusCode::CREATED, headers, "")
}

async fn hello_world() -> String {
    "hello world".to_string()
}
