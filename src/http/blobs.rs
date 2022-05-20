use std::collections::HashMap;
use std::sync::Arc;

use ::http::StatusCode;
use aws_sdk_s3::types::ByteStream;
use axum::{
    body::StreamBody,
    extract::{Extension, Path, Query, TypedHeader},
    headers::{ContentLength, ContentRange, ContentType},
    http::header::{self, HeaderMap, HeaderName, HeaderValue},
    http::Request,
    response::{IntoResponse, Response},
    routing::{get, patch, post},
    Router,
};
use hyper::body::Body;

use uuid::Uuid;

use crate::{
    http::notimplemented,
    metadata::{PostgresMetadata, Registry, Repository},
    objects::{StreamObjectBody, UploadSession, S3},
    DistributionErrorCode, Error, OciDigest, Result,
};

pub fn router() -> Router {
    Router::new()
        .route(
            "/:digest",
            get(get_blob).delete(notimplemented).head(head_blob),
        )
        .route("/uploads/", post(uploads_post))
        .route(
            "/uploads/:session_uuid",
            patch(uploads_patch).put(uploads_put),
        )
}

async fn get_blob(
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let registry = metadata.get_registry("meow").await?;
    match path_params.get("repository") {
        Some(s) => metadata.get_repository(&registry.id, s).await?,
        None => return Err(Error::MissingPathParameter("repository")),
    };
    let digest: &String = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;

    if let Some(blob) = metadata.get_blob(&registry.id, digest).await? {
        let stream_body: StreamBody<ByteStream> =
            objects.get_blob(&blob.digest.as_str().try_into()?).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(digest).unwrap(),
        );
        Ok((StatusCode::OK, stream_body).into_response())
    } else {
        Err(Error::DistributionSpecError(
            DistributionErrorCode::BlobUnknown,
        ))
    }
}

async fn head_blob(
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let registry = metadata.get_registry("meow").await?;
    match path_params.get("repository") {
        Some(s) => metadata.get_repository(&registry.id, s).await?,
        None => return Err(Error::MissingPathParameter("repository")),
    };
    let digest: &String = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;

    if metadata.blob_exists(&registry.id, digest).await? {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(digest).unwrap(),
        );
        Ok((StatusCode::OK, headers, "").into_response())
    } else {
        Err(Error::DistributionSpecError(
            DistributionErrorCode::BlobUnknown,
        ))
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
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Result<Response> {
    let registry = metadata.get_registry("meow").await?;
    let repository = match path_params.get("repository") {
        Some(s) => metadata.get_repository(&registry.id, s).await?,
        None => return Err(Error::MissingPathParameter("repository")),
    };
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
            upload_session_id(&repository.name, metadata).await
        }
        Some(dgst) => {
            if let Some(TypedHeader(length)) = content_length {
                upload_blob(
                    &registry,
                    &repository,
                    dgst,
                    length,
                    request,
                    metadata,
                    objects,
                )
                .await
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
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    content_type: Option<TypedHeader<ContentType>>,
    content_range: Option<TypedHeader<ContentRange>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Result<Response> {
    let registry = metadata.get_registry("meow").await?;
    let repository = match path_params.get("repository") {
        Some(s) => metadata.get_repository(&registry.id, s).await?,
        None => return Err(Error::MissingPathParameter("repository")),
    };
    let digest: &str = query_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;
    let session_uuid = path_params
        .get("session_uuid")
        .map(|s| Uuid::parse_str(s))
        .transpose()?
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;

    // retrieve the session or fail if it doesn't exist
    let mut session = metadata
        .get_session(session_uuid)
        .await
        .map_err(|_| Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown))?;

    // determine if this is a monolithic POST-PUT or the final request in a chunked POST-PATCH-PUT
    // sequence
    let response = match session.upload_id {
        // POST-PATCH-PUT
        Some(_) => {
            if let (
                Some(TypedHeader(content_range)),
                // TODO: what should we do with ContentType?
                Some(TypedHeader(_content_type)),
                Some(TypedHeader(content_length)),
            ) = (content_range, content_type, content_length)
            {
                upload_chunk(
                    &mut session,
                    content_length,
                    content_range,
                    request,
                    objects.clone(),
                    metadata.clone(),
                )
                .await?;
            }

            // TODO: what if there is a body but none of the content headers are set? technically
            // this would be a client bug, but it could also result in data corruption and as such
            // should probably be handled here. this should probably result in a 400 bad request
            // error if we can detect it

            if !objects.blob_exists(&oci_digest).await? {
                let chunks = metadata.get_chunks(&session).await?;
                objects
                    .finalize_chunked_upload(&session, chunks, &oci_digest)
                    .await?;
            } else {
                objects.abort_chunked_upload(&session).await?;
            }

            if !metadata.blob_exists(&registry.id, digest).await? {
                metadata.insert_blob(&registry.id, digest).await?;
            }
            let location = format!("/v2/{}/blobs/{}", repository.name, digest);
            let mut headers = HeaderMap::new();
            headers.insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
            (StatusCode::CREATED, headers, "").into_response()
        }
        // POST-PUT
        None => {
            match (content_type, content_length) {
                (Some(TypedHeader(_content_type)), Some(TypedHeader(content_length))) => {
                    let response = upload_blob(
                        &registry,
                        &repository,
                        digest,
                        content_length,
                        request,
                        metadata.clone(),
                        objects.clone(),
                    )
                    .await?;

                    if !metadata.blob_exists(&registry.id, digest).await? {
                        metadata.insert_blob(&registry.id, digest).await?;
                    }
                    response
                }
                _ => {
                    return Err(Error::DistributionSpecError(DistributionErrorCode::SizeInvalid))
                }
            }
        }
    };

    match metadata.delete_session(&session).await {
        Ok(_) => (),
        Err(e) => {
            // TODO: this should use a logging library
            eprintln!("{:?}", e);
        }
    };

    Ok(response)
}

async fn uploads_patch(
    Extension(metadata): Extension<Arc<PostgresMetadata>>,
    Extension(objects): Extension<Arc<S3>>,
    Path(path_params): Path<HashMap<String, String>>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    TypedHeader(content_range): TypedHeader<ContentRange>,
    TypedHeader(_content_type): TypedHeader<ContentType>,
    request: Request<Body>,
) -> Result<Response> {
    let registry = metadata.get_registry("meow").await?;
    let repository = match path_params.get("repository") {
        Some(s) => metadata.get_repository(&registry.id, s).await?,
        None => return Err(Error::MissingPathParameter("repository")),
    };
    let session_uuid = path_params
        .get("session_uuid")
        .map(|s| Uuid::parse_str(s))
        .transpose()?
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;

    // retrieve the session or fail if it doesn't exist
    let mut session = metadata
        .get_session(session_uuid)
        .await
        .map_err(|_| Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown))?;

    match session.upload_id {
        Some(_) => (),
        None => {
            objects
                .clone()
                .initiate_chunked_upload(&mut session)
                .await?
        }
    };

    upload_chunk(
        &mut session,
        content_length,
        content_range,
        request,
        objects.clone(),
        metadata.clone(),
    )
    .await?;

    // TODO: validate content length of chunk
    // TODO: update incremental digest state on session

    let location = format!("/v2/{}/blobs/uploads/{}", repository.name, session.uuid);
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
    Ok((StatusCode::ACCEPTED, headers, "").into_response())
}

async fn upload_blob(
    registry: &Registry,
    repository: &Repository,
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
        .upload_blob(&oci_digest, body.into(), content_length.0)
        .await
        .unwrap();

    // TODO: validate digest
    // TODO: validate content length

    // insert metadata
    if !metadata.blob_exists(&registry.id, digest).await? {
        metadata.clone().insert_blob(&registry.id, digest).await?;
    }

    let location = format!("/v2/{}/blobs/{}", repository.name, digest,);
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
    Ok((StatusCode::CREATED, headers, "").into_response())
}

async fn upload_chunk(
    session: &mut UploadSession,
    content_length: ContentLength,
    content_range: ContentRange,
    request: Request<Body>,
    objects: Arc<S3>,
    metadata: Arc<PostgresMetadata>,
) -> Result<()> {
    let mut range_end: i64 = 0;
    // verify the request's ContentRange against the last chunk's end of range
    if let Some((begin, end)) = content_range.bytes_range() {
        if begin != 0 && begin as i64 != session.last_range_end + 1 {
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::BlobUploadInvalid,
            ));
        }
        range_end = end as i64;
    }

    let chunk = objects
        .clone()
        .upload_chunk(session, content_length.0, request.into_body())
        .await?;

    session.last_range_end = range_end;

    metadata.insert_chunk(&session, &chunk).await?;
    metadata.update_session(&session).await?;

    // TODO: validate content length of chunk
    // TODO: update incremental digest state on session

    Ok(())
}

async fn upload_session_id(repo_name: &str, metadata: Arc<PostgresMetadata>) -> Result<Response> {
    let session: UploadSession = metadata.new_upload_session().await?;
    let location = format!("/v2/{}/blobs/uploads/{}", repo_name, session.uuid,);
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, HeaderValue::from_str(&location).unwrap());
    Ok((StatusCode::ACCEPTED, headers, "").into_response())
}
