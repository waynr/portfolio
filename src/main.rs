use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, TypedHeader},
    headers::{ContentLength, ContentType},
    http::header::{HeaderMap, HeaderName, HeaderValue},
    http::Request,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
    Router,
};
use axum_macros::debug_handler;

use http::StatusCode;
use hyper::body::Body;

use uuid::Uuid;

use portfolio::metadata::PostgresMetadata;
use portfolio::objects::{StreamObjectBody, S3};
use portfolio::DistributionErrorCode;
use portfolio::Error;
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
        .route("/uploads/", post(blobs_post))
        .layer(Extension(metadata))
        .layer(Extension(objects))
        .route(
            "/uploads/:session_uuid",
            patch(notimplemented).put(blobs_put),
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

async fn blobs_post(
    Path(path_params): Path<HashMap<String, String>>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
) -> Result<Response> {
    let repo_name = path_params
        .get("repository")
        .ok_or_else(|| Error::MissingPathParameter("repository"))?;
    let digest: &String = match query_params.get("digest") {
        None => return blobs_post_session_id(repo_name, metadata).await,
        Some(dgst) => dgst,
    };
    upload_blob(
        repo_name,
        digest,
        content_length,
        request,
        metadata,
        objects,
    )
    .await
}

async fn blobs_put(
    Path(path_params): Path<HashMap<String, String>>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
) -> Result<Response> {
    let repo_name = path_params
        .get("repository")
        .ok_or_else(|| Error::MissingPathParameter("repository"))?;
    let digest: &String = match query_params.get("digest") {
        None => return blobs_post_session_id(repo_name, metadata).await,
        Some(dgst) => dgst,
    };
    let session_uuid = match path_params.get("session_uuid") {
        None => return Err(Error::MissingPathParameter("session_uuid")),
        Some(uuid_str) => Uuid::parse_str(uuid_str)?,
    };

    // verify the session uuid in the url exists, otherwise reject the put
    match metadata.get_session(session_uuid).await {
        Ok(_) => (),
        // TODO: may need to distinguish later between database connection errors and session not
        // found errors
        Err(_) => return Err(Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown)),
    };

    let response = upload_blob(
        repo_name,
        digest,
        content_length,
        request,
        metadata.clone(),
        objects,
    )
    .await?;

    match metadata.delete_session(session_uuid).await {
        Ok(_) => (),
        Err(e) => {
            // TODO: this should use a logging library
            eprintln!("{:?}", e);
        },
    };

    Ok(response)
}

async fn upload_blob(
    repo_name: &str,
    digest: &str,
    content_length: ContentLength,
    request: Request<Body>,
    metadata: Arc<PostgresMetadata>,
    objects: Arc<S3>,
) -> Result<Response> {
    let oci_digest: OciDigest = digest.try_into()?;

    // upload blob
    let digester = oci_digest.digester();
    let body = StreamObjectBody::from_body(request.into_body(), digester);
    objects
        .clone()
        .upload_blob(digest, body.into())
        .await
        .unwrap();

    // validate digest
    // validate content length

    // insert metadata
    metadata.clone().insert_blob(digest).await.unwrap();

    let location = format!("/v2/{}/blobs/{}", repo_name, digest,);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("Location"),
        HeaderValue::from_str(&location).unwrap(),
    );
    Ok((StatusCode::CREATED, headers, "").into_response())
}

async fn blobs_post_session_id(
    repo_name: &str,
    metadata: Arc<PostgresMetadata>,
) -> Result<Response> {
    let session = metadata.new_upload_session().await?;
    let location = format!("/v2/{}/blobs/{}", repo_name, session.uuid,);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("Location"),
        HeaderValue::from_str(&location).unwrap(),
    );
    Ok((StatusCode::ACCEPTED, headers, "").into_response())
}

async fn hello_world() -> String {
    "hello world".to_string()
}
