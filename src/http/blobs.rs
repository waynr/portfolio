use std::collections::HashMap;
use std::sync::Arc;

use ::http::StatusCode;
use axum::{
    extract::{Extension, Path, Query, TypedHeader},
    headers::{ContentLength, ContentRange, ContentType},
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
    objects::{ChunkInfo, StreamObjectBody, S3},
    DigestState, DistributionErrorCode, Error, OciDigest, Result,
};

pub struct UploadSession {
    pub uuid: Uuid,
    pub start_date: NaiveDate,
    pub digest_state: Option<Json<DigestState>>,
    pub chunk_info: Option<Json<ChunkInfo>>,
}

// /v2/<repo>/blobs/upload
//
// two use cases:
// * upload the entire blob body
//   * must include 'digest' query param
//   * must include 'ContentLength' query param
// * initiate upload session for POST-PUT or POST-PATCH-PUT sequence
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
    match query_params.get("digest") {
        None => {
            match content_length {
                // according to docs:
                //   * https://github.com/opencontainers/distribution-spec/blob/main/spec.md#post-then-put
                // ContentLength is not required for POST in POST-PUT monolithic upload.
                //
                // however, according to conformance tests it's also not required for
                // POST-PATCH-PUT chunked uploads:
                //   * https://github.com/opencontainers/distribution-spec/blob/dd38b7ed8a995fc2f6e730a4deae60e2c0ee92fe/conformance/02_push_test.go#L24
                //
                // i'm sure if we looked at the docker registry client, it would probably do
                // something different than both conformance and spec docs.
                //
                // here we are just going to allow missing ContentLength when the digest is missing
                // (implying either POST-PUT or POST-PATCH-PUT sequence)
                None => (),
                Some(TypedHeader(length)) => {
                    if length.0 > 0 {
                        return Err(Error::MissingHeader(
                            "ContentLength must be 0 to start new session",
                        ));
                    }
                }
            }
            upload_session_id(repo_name, metadata).await
        }
        Some(dgst) => {
            if let Some(TypedHeader(length)) = content_length {
                upload_blob(repo_name, dgst, length, request, metadata, objects).await
            } else {
                Err(Error::MissingHeader("ContentLength"))
            }
        }
    }
}

// /v2/<repo>/blobs/upload/<session>
//
// two use cases:
//
// * POST-PUT monolithic upload
//   * entire blob must be in the body of PUT
//   * must include 'digest' query param
//   * should close out session
// * POST-PATCH-PUT chunked upload
//   * PUT body may contain final chunk
//     * if body containers final chunk, must include ContentLength and ContentRange header
//   * must include 'digest' query param, referring to the digest of the entire blob (not the final
//   chunk)
//
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

async fn uploads_patch(
    Path(path_params): Path<HashMap<String, String>>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    TypedHeader(content_range): TypedHeader<ContentRange>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    request: Request<Body>,
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
) -> Result<Response> {
    let repo_name = path_params
        .get("repository")
        .ok_or_else(|| Error::MissingPathParameter("repository"))?;
    let session_uuid = match path_params.get("session_uuid") {
        None => return Err(Error::MissingPathParameter("session_uuid")),
        Some(uuid_str) => Uuid::parse_str(uuid_str)?,
    };

    // retrieve the session from metadata backend
    let mut session = match metadata.get_session(session_uuid).await {
        Ok(session) => session,
        // TODO: may need to distinguish later between database connection errors and session not
        // found errors
        Err(_) => {
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::BlobUploadUnknown,
            ))
        }
    };

    let mut range_end: u64 = 0;

    let mut chunk_info = match session.chunk_info {
        Some(Json(info)) => info,
        None => {
            objects
                .clone()
                .initiate_chunked_upload(&session.uuid)
                .await?
        }
    };

    // verify the request's ContentRange against the last chunk's end of range
    if let Some(range) = content_range.bytes_range() {
        if range.0 != chunk_info.last_range_end {
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::BlobUploadInvalid,
            ));
        }
        range_end = range.1;
    }

    objects
        .clone()
        .upload_chunk(&mut chunk_info, request.into_body())
        .await?;

    chunk_info.last_range_end = range_end;
    session.chunk_info = Some(Json(chunk_info));

    // TODO: validate content length of chunk
    // TODO: update incremental digest state on session

    metadata.update_session(&session).await?;

    let location = format!("/v2/{}/blobs/{}", repo_name, session.uuid);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("Location"),
        HeaderValue::from_str(&location).unwrap(),
    );
    Ok((StatusCode::CREATED, headers, "").into_response())
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

    // TODO: validate digest
    // TODO: validate content length

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
            patch(uploads_patch).put(uploads_put),
        )
}
