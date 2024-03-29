use std::collections::HashMap;
use std::str::FromStr;

use ::http::StatusCode;
use axum::body::StreamBody;
use axum::extract::{Extension, Path, Query, TypedHeader};
use axum::headers::{ContentLength, ContentType};
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::http::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::Router;
use headers::Header;
use hyper::body::Body;
use uuid::Uuid;

use portfolio_core::{Error as CoreError, OciDigest};

use super::errors::{Error, Result};
use super::headers::{ContentRange, Range};
use super::ArcRepositoryStore;

pub fn router() -> Router {
    Router::new()
        .route(
            "/:digest",
            get(get_blob).delete(delete_blob).head(head_blob),
        )
        .route("/uploads/", post(uploads_post))
        .route(
            "/uploads/:session_uuid",
            patch(uploads_patch).put(uploads_put).get(uploads_get),
        )
}

async fn get_blob(
    Extension(repository): Extension<ArcRepositoryStore>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let blob_store = repository.get_blob_store();

    if let Some((blob, body)) = blob_store.get(&oci_digest).await? {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(digest)?,
        );
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(blob.bytes_on_disk().to_string().as_str())?,
        );
        Ok((StatusCode::OK, headers, StreamBody::new(body)).into_response())
    } else {
        Err(CoreError::BlobUnknown(None).into())
    }
}

async fn head_blob(
    Extension(repository): Extension<ArcRepositoryStore>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let blob_store = repository.get_blob_store();

    if let Some(blob) = blob_store.head(&oci_digest).await? {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(digest)?,
        );
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(blob.bytes_on_disk().to_string().as_str())?,
        );
        Ok((StatusCode::OK, headers, "").into_response())
    } else {
        Err(CoreError::BlobUnknown(None).into())
    }
}

// /v2/<repo>/blobs/upload
//
// two use cases:
// * upload the entire blob body
//   * must include 'digest' query param
//   * must include 'ContentLength' query param
// * initiate upload session for POST-PUT or POST-PATCH-PUT sequence
async fn uploads_post(
    Extension(repository): Extension<ArcRepositoryStore>,
    content_length: Option<TypedHeader<ContentLength>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Result<Response> {
    let session_store = repository.get_upload_session_store();

    let mount = query_params.get("mount");
    let from = query_params.get("from");
    match (mount, from) {
        (Some(digest), Some(_dontcare)) => {
            let mut headers = HeaderMap::new();
            let oci_digest: OciDigest = digest.as_str().try_into()?;

            let store = repository.get_blob_store();
            if !store.head(&oci_digest).await?.is_some() {
                let session = session_store.new_upload_session().await?;

                let location =
                    format!("/v2/{}/blobs/uploads/{}", repository.name(), session.uuid(),);
                let mut headers = HeaderMap::new();
                headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
                headers.insert(
                    HeaderName::from_str("docker-upload-uuid")?,
                    HeaderValue::from_str(session.uuid().to_string().as_str())?,
                );
                return Ok((StatusCode::ACCEPTED, headers, "").into_response());
            }

            let location = format!("/v2/{}/blobs/{}", repository.name(), digest);
            headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
            return Ok((StatusCode::CREATED, headers, "").into_response());
        }
        (_, _) => {}
    }

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
            let session = session_store.new_upload_session().await?;

            let location = format!("/v2/{}/blobs/uploads/{}", repository.name(), session.uuid(),);
            let mut headers = HeaderMap::new();
            headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
            headers.insert(
                HeaderName::from_str("docker-upload-uuid")?,
                HeaderValue::from_str(session.uuid().to_string().as_str())?,
            );
            Ok((StatusCode::ACCEPTED, headers, "").into_response())
        }
        Some(dgst) => {
            if let Some(TypedHeader(length)) = content_length {
                let oci_digest: OciDigest = dgst.as_str().try_into()?;
                let mut store = repository.get_blob_store();
                store
                    .put(&oci_digest, length.0, request.into_body())
                    .await?;

                let location = format!("/v2/{}/blobs/{}", repository.name(), dgst);
                let mut headers = HeaderMap::new();
                headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
                Ok((StatusCode::CREATED, headers, "").into_response())
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
    Extension(repository): Extension<ArcRepositoryStore>,
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    content_type: Option<TypedHeader<ContentType>>,
    content_range: Option<TypedHeader<ContentRange>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Result<Response> {
    let digest: &str = query_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let session_uuid_str = path_params
        .get("session_uuid")
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;
    let session_uuid = Uuid::parse_str(session_uuid_str).map_err(CoreError::from)?;

    let start = content_range.map(|TypedHeader(content_range)| content_range.start);

    // retrieve the session or fail if it doesn't exist
    // TODO: refactor this method to avoid retrieving the upload session twice (once here, once in
    // the resume method)
    let session_store = repository.get_upload_session_store();
    let session = session_store
        .get_upload_session(&session_uuid)
        .await
        .map_err(|_| CoreError::BlobUploadUnknown(None))?;

    // determine if this is a monolithic POST-PUT or the final request in a chunked POST-PATCH-PUT
    // sequence
    let response = match session.upload_id() {
        // POST-PATCH-PUT
        Some(_) => {
            let store = repository.get_blob_store();
            let session = if let (
                // TODO: what if there is a body but none of the content headers are set? technically
                // this would be a client bug, but it could also result in data corruption and as such
                // should probably be handled here. this should probably result in a 400 bad request
                // error if we can detect it
                // TODO: what should we do with ContentType?
                Some(TypedHeader(_content_type)),
                Some(TypedHeader(content_length)),
            ) = (content_type, content_length)
            {
                let mut writer = store.resume(&session_uuid, start).await?;
                let session = writer.write(content_length.0, request.into_body()).await?;

                // TODO: validate content length of chunk
                // TODO: update incremental digest state on session
                session
            } else {
                let mut writer = store.resume(&session_uuid, start).await?;
                writer.finalize(&oci_digest).await?
            };

            let session_store = repository.get_upload_session_store();
            match session_store.delete_session(&session.uuid()).await {
                Ok(_) => (),
                Err(e) => {
                    tracing::warn!("failed to delete session: {e:?}");
                }
            };

            let location = format!("/v2/{}/blobs/{}", repository.name(), digest);
            let mut headers = HeaderMap::new();
            headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
            headers.insert(
                HeaderName::from_str("docker-upload-uuid")?,
                HeaderValue::from_str(&session_uuid_str)?,
            );
            (StatusCode::CREATED, headers, "").into_response()
        }
        // POST-PUT
        None => match (content_type, content_length) {
            (Some(TypedHeader(_content_type)), Some(TypedHeader(content_length))) => {
                let mut store = repository.get_blob_store();
                store
                    .put(&oci_digest, content_length.0, request.into_body())
                    .await?;

                let location = format!("/v2/{}/blobs/{}", repository.name(), digest);
                let mut headers = HeaderMap::new();
                headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
                headers.insert(
                    HeaderName::from_str("docker-upload-uuid")?,
                    HeaderValue::from_str(session_uuid_str)?,
                );
                (StatusCode::CREATED, headers, "").into_response()
            }
            _ => return Err(CoreError::SizeInvalid(None).into()),
        },
    };

    Ok(response)
}

async fn uploads_patch(
    Extension(repository): Extension<ArcRepositoryStore>,
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    content_range: Option<TypedHeader<ContentRange>>,
    request: Request<Body>,
) -> Result<Response> {
    let session_uuid_str = path_params
        .get("session_uuid")
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;
    let session_uuid = Uuid::parse_str(session_uuid_str).map_err(CoreError::from)?;

    let start = content_range.map(|TypedHeader(content_range)| content_range.start);

    let store = repository.get_blob_store();
    let mut writer = store.resume(&session_uuid, start).await?;
    let session = if let Some(TypedHeader(content_length)) = content_length {
        writer.write(content_length.0, request.into_body()).await?
    } else {
        writer.write_chunked(request.into_body()).await?
    };

    // TODO: validate content length of chunk
    // TODO: update incremental digest state on session

    let mut headers = HeaderMap::new();

    let location = format!("/v2/{}/blobs/uploads/{}", repository.name(), session_uuid);
    headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
    headers.insert(
        HeaderName::from_str("docker-upload-uuid")?,
        HeaderValue::from_str(session_uuid_str)?,
    );

    let range = Range {
        start: 0,
        end: session.last_range_end() as u64,
    };
    let range: String = (&range).into();
    headers.insert(Range::name(), HeaderValue::from_str(&range).expect("meow"));

    Ok((StatusCode::ACCEPTED, headers, "").into_response())
}

async fn uploads_get(
    Extension(repository): Extension<ArcRepositoryStore>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let session_uuid_str = path_params
        .get("session_uuid")
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;
    let session_uuid = Uuid::parse_str(session_uuid_str).map_err(CoreError::from)?;

    // retrieve the session or fail if it doesn't exist
    let session_store = repository.get_upload_session_store();
    let session = session_store
        .get_upload_session(&session_uuid)
        .await
        .map_err(|_| CoreError::BlobUploadUnknown(None))?;

    let mut headers = HeaderMap::new();

    let location = format!("/v2/{}/blobs/uploads/{}", repository.name(), session_uuid);
    headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
    headers.insert(
        HeaderName::from_str("docker-upload-uuid")?,
        HeaderValue::from_str(session_uuid_str)?,
    );

    let range = Range {
        start: 0,
        end: session.last_range_end() as u64,
    };
    let range: String = (&range).into();
    headers.insert(Range::name(), HeaderValue::from_str(&range).expect("meow"));

    Ok((StatusCode::NO_CONTENT, headers, "").into_response())
}

async fn delete_blob(
    Extension(repository): Extension<ArcRepositoryStore>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingPathParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let mut store = repository.get_blob_store();

    store.delete(&oci_digest).await?;

    Ok((StatusCode::ACCEPTED, "").into_response())
}
