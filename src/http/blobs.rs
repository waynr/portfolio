use std::collections::HashMap;

use ::http::StatusCode;
use axum::{
    extract::{Extension, Path, Query, TypedHeader},
    headers::{ContentLength, ContentType},
    http::header::{self, HeaderMap, HeaderName, HeaderValue},
    http::Request,
    response::{IntoResponse, Response},
    routing::{get, patch, post},
    Router,
};
use headers::Header;
use hyper::body::Body;

use uuid::Uuid;

use crate::{
    http::headers::{ContentRange, Range},
    http::notimplemented,
    objects::ObjectStore,
    registry::registries::Registry,
    registry::UploadSession,
    DistributionErrorCode, Error, OciDigest, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new()
        .route(
            "/:digest",
            get(get_blob::<O>)
                .delete(notimplemented)
                .head(head_blob::<O>),
        )
        .route("/uploads/", post(uploads_post::<O>))
        .route(
            "/uploads/:session_uuid",
            patch(uploads_patch::<O>)
                .put(uploads_put::<O>)
                .get(uploads_get::<O>),
        )
}

async fn get_blob<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    if !registry.repository_exists(repo_name).await? {
        return Err(Error::DistributionSpecError(
            DistributionErrorCode::NameUnknown,
        ));
    }

    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let blob_store = registry.get_blob_store();

    if let Some(stream_body) = blob_store.get_blob(&oci_digest).await? {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(digest)?,
        );
        Ok((StatusCode::OK, headers, stream_body).into_response())
    } else {
        Err(Error::DistributionSpecError(
            DistributionErrorCode::BlobUnknown,
        ))
    }
}

async fn head_blob<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    if !registry.repository_exists(repo_name).await? {
        return Err(Error::DistributionSpecError(
            DistributionErrorCode::NameUnknown,
        ));
    }

    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let blob_store = registry.get_blob_store();

    if blob_store.blob_exists(&oci_digest).await? {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(digest)?,
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
async fn uploads_post<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    if !registry.repository_exists(repo_name).await? {
        registry.create_repository(repo_name).await?;
    }

    let mount = query_params.get("mount");
    let from = query_params.get("from");
    match (mount, from) {
        (Some(digest), Some(_dontcare)) => {
            let mut headers = HeaderMap::new();
            let oci_digest: OciDigest = digest.as_str().try_into()?;

            let store = registry.get_blob_store();
            if !store.blob_exists(&oci_digest).await? {
                let session: UploadSession = registry.new_upload_session().await?;

                let location = format!("/v2/{}/blobs/uploads/{}", repo_name, session.uuid,);
                let mut headers = HeaderMap::new();
                headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
                return Ok((StatusCode::ACCEPTED, headers, "").into_response())
            }

            let location = format!("/v2/{}/blobs/{}", &repo_name, digest);
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
            let session: UploadSession = registry.new_upload_session().await?;

            let location = format!("/v2/{}/blobs/uploads/{}", repo_name, session.uuid,);
            let mut headers = HeaderMap::new();
            headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
            Ok((StatusCode::ACCEPTED, headers, "").into_response())
        }
        Some(dgst) => {
            if let Some(TypedHeader(length)) = content_length {
                let oci_digest: OciDigest = dgst.as_str().try_into()?;
                let mut store = registry.get_blob_store();
                store.upload(&oci_digest, length.0, request.into_body()).await?;

                let location = format!("/v2/{}/blobs/{}", &repo_name, dgst);
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
async fn uploads_put<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    content_length: Option<TypedHeader<ContentLength>>,
    content_type: Option<TypedHeader<ContentType>>,
    content_range: Option<TypedHeader<ContentRange>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    if !registry.repository_exists(repo_name).await? {
        return Err(Error::DistributionSpecError(
            DistributionErrorCode::NameUnknown,
        ));
    }

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
    let mut session = registry
        .get_upload_session(&session_uuid)
        .await
        .map_err(|_| Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown))?;

    // determine if this is a monolithic POST-PUT or the final request in a chunked POST-PATCH-PUT
    // sequence
    let response = match session.upload_id {
        // POST-PATCH-PUT
        Some(_) => {
            let store = registry.get_blob_store();
            if let Some(TypedHeader(content_range)) = content_range {
                session.validate_range(&content_range)?;
            }

            if let (
                // TODO: what if there is a body but none of the content headers are set? technically
                // this would be a client bug, but it could also result in data corruption and as such
                // should probably be handled here. this should probably result in a 400 bad request
                // error if we can detect it
                // TODO: what should we do with ContentType?
                Some(TypedHeader(_content_type)),
                Some(TypedHeader(content_length)),
            ) = (content_type, content_length)
            {
                let mut writer = store.resume(&mut session).await?;
                let _written = writer.write(content_length.0, request.into_body());

                // TODO: validate content length of chunk
                // TODO: update incremental digest state on session
            } else {
                let mut writer = store.resume(&mut session).await?;
                writer.finalize(&oci_digest).await?;
            }

            let location = format!("/v2/{}/blobs/{}", repo_name, digest);
            let mut headers = HeaderMap::new();
            headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
            (StatusCode::CREATED, headers, "").into_response()
        }
        // POST-PUT
        None => match (content_type, content_length) {
            (Some(TypedHeader(_content_type)), Some(TypedHeader(content_length))) => {
                let mut store = registry.get_blob_store();
                store
                    .upload(&oci_digest, content_length.0, request.into_body())
                    .await?;

                let location = format!("/v2/{}/blobs/{}", repo_name, digest);
                let mut headers = HeaderMap::new();
                headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
                (StatusCode::CREATED, headers, "").into_response()
            }
            _ => {
                return Err(Error::DistributionSpecError(
                    DistributionErrorCode::SizeInvalid,
                ))
            }
        },
    };

    match registry.delete_session(&session).await {
        Ok(_) => (),
        Err(e) => {
            tracing::warn!("failed to delete session: {e:?}");
        }
    };

    Ok(response)
}

async fn uploads_patch<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    content_range: Option<TypedHeader<ContentRange>>,
    TypedHeader(_content_type): TypedHeader<ContentType>,
    request: Request<Body>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    if !registry.repository_exists(repo_name).await? {
        return Err(Error::DistributionSpecError(
            DistributionErrorCode::NameUnknown,
        ));
    }

    let session_uuid = path_params
        .get("session_uuid")
        .map(|s| Uuid::parse_str(s))
        .transpose()?
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;

    // retrieve the session or fail if it doesn't exist
    let mut session = registry
        .get_upload_session(&session_uuid)
        .await
        .map_err(|_| Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown))?;

    let range: Option<Range> = if let Some(TypedHeader(content_range)) = content_range {
        session.validate_range(&content_range)?;
        Some(content_range.into())
    } else {
        None
    };

    let store = registry.get_blob_store();
    {
        let mut writer = store.resume(&mut session).await?;
        let _written = writer.write(content_length.0, request.into_body()).await?;
    }

    // TODO: validate content length of chunk
    // TODO: update incremental digest state on session

    let mut headers = HeaderMap::new();

    let location = format!("/v2/{}/blobs/uploads/{}", repo_name, session_uuid);
    headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);

    if let Some(range) = range {
        let range: String = (&range).into();
        headers.insert(Range::name(), HeaderValue::from_str(&range).expect("meow"));
    }

    Ok((StatusCode::ACCEPTED, headers, "").into_response())
}

async fn uploads_get<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let session_uuid = path_params
        .get("session_uuid")
        .map(|s| Uuid::parse_str(s))
        .transpose()?
        .ok_or_else(|| Error::MissingPathParameter("session_uuid"))?;

    // retrieve the session or fail if it doesn't exist
    let session = registry
        .get_upload_session(&session_uuid)
        .await
        .map_err(|_| Error::DistributionSpecError(DistributionErrorCode::BlobUploadUnknown))?;

    let mut headers = HeaderMap::new();

    let location = format!("/v2/{}/blobs/uploads/{}", repo_name, session_uuid);
    headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);

    let range = Range {
        start: 0,
        end: session.last_range_end as u64,
    };
    let range: String = (&range).into();
    headers.insert(Range::name(), HeaderValue::from_str(&range).expect("meow"));

    Ok((StatusCode::NO_CONTENT, headers, "").into_response())
}
