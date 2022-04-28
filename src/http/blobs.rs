use std::collections::HashMap;
use std::sync::Arc;

use ::http::StatusCode;
use axum::{
    extract::{Extension, Path, Query, TypedHeader},
    headers::{ContentLength, ContentType},
    http::header::{HeaderMap, HeaderName, HeaderValue},
    http::Request,
    response::{IntoResponse, Response},
    routing::{get, patch, post},
    Router,
};
use hyper::body::Body;

use chrono::NaiveDate;
use sqlx::types::Json;
use uuid::Uuid;

use crate::{
    http::notimplemented,
    metadata::PostgresMetadata,
    objects::{StreamObjectBody, S3},
    DigestState, DistributionErrorCode, Error, OciDigest, Result,
};

pub struct UploadSession {
    pub uuid: Uuid,
    pub start_date: NaiveDate,
    pub digest_state: Option<Json<DigestState>>,
}

async fn uploads_post(
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
) -> Result<Response> {
    let repo_name = path_params
        .get("repository")
        .ok_or_else(|| Error::MissingPathParameter("repository"))?;
    let digest: &String = match query_params.get("digest") {
        None => return upload_session_id(repo_name, metadata).await,
        Some(dgst) => dgst,
    };
    if let Some(TypedHeader(length)) = content_length {
        upload_blob(
            repo_name,
            digest,
            length,
            request,
            metadata,
            objects,
        )
        .await?;
    }
    Err(Error::MissingHeader("ContentLength"))
}

async fn uploads_put(
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
        None => return upload_session_id(repo_name, metadata).await,
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
        Err(_) => {
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::BlobUploadUnknown,
            ))
        }
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
        }
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

async fn upload_session_id(repo_name: &str, metadata: Arc<PostgresMetadata>) -> Result<Response> {
    let session: UploadSession = metadata.new_upload_session().await?;
    let location = format!("/v2/{}/blobs/{}", repo_name, session.uuid,);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("Location"),
        HeaderValue::from_str(&location).unwrap(),
    );
    Ok((StatusCode::ACCEPTED, headers, "").into_response())
}

pub fn router() -> Router {
    Router::new()
        .route(
            "/:digest",
            get(notimplemented)
                .delete(notimplemented)
                .head(notimplemented),
        )
        .route("/uploads/", post(uploads_post))
        .route(
            "/uploads/:session_uuid",
            patch(notimplemented).put(uploads_put),
        )
}
