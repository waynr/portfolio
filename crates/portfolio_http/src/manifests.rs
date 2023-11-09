use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use axum::body::{Bytes, StreamBody};
use axum::extract::{DefaultBodyLimit, Extension, Path};
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, TypedHeader};
use headers::{ContentLength, ContentType};
use http::StatusCode;

use portfolio_core::registry::{ManifestRef, ManifestSpec, RepositoryStore};
use portfolio_core::Error as CoreError;

use super::errors::{Error, Result};

pub fn router() -> Router {
    Router::new()
        .route(
            "/:reference",
            get(get_manifest)
                .delete(delete_manifest)
                .put(put_manifest)
                .head(head_manifest),
        )
        .layer(DefaultBodyLimit::max(6 * 1024 * 1024))
}

async fn head_manifest(
    Extension(repository): Extension<Arc<dyn RepositoryStore>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let mstore = repository.get_manifest_store();
    let manifest = mstore.head(&manifest_ref).await?;

    if let Some(manifest) = manifest {
        let mut headers = HeaderMap::new();
        let dgst: String = manifest.digest().into();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(dgst.as_str())?,
        );
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(manifest.bytes_on_disk().to_string().as_str())?,
        );
        return Ok((StatusCode::OK, headers, "").into_response());
    }

    Err(CoreError::ManifestBlobUnknown(None).into())
}

async fn get_manifest(
    Extension(repository): Extension<Arc<dyn RepositoryStore>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let mstore = repository.get_manifest_store();
    let (manifest, body) = if let Some((m, b)) = mstore.get(&manifest_ref).await? {
        (m, b)
    } else {
        return Err(CoreError::ManifestUnknown(None).into());
    };

    let mut headers = HeaderMap::new();
    let dgst: String = manifest.digest().into();
    headers.insert(
        HeaderName::from_lowercase(b"docker-content-digest")?,
        HeaderValue::from_str(dgst.as_str())?,
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(manifest.bytes_on_disk().to_string().as_str())?,
    );
    if let Some(mt) = manifest.media_type() {
        let content_type: String = mt.clone().into();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_str(content_type.as_str())?,
        );
    }
    Ok((StatusCode::OK, headers, StreamBody::new(body)).into_response())
}

/// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pushing-manifests
async fn put_manifest(
    Extension(repository): Extension<Arc<dyn RepositoryStore>>,
    content_type: Option<TypedHeader<ContentType>>,
    content_length: Option<TypedHeader<ContentLength>>,
    Path(path_params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Result<Response> {
    let mref = path_params
        .get("reference")
        .ok_or_else(|| Error::MissingPathParameter("reference"))?;
    let manifest_ref = ManifestRef::from_str(mref)?;

    // we need to deserialize the request body into a type we can use to determine how to represent
    // it in the database, but according to distribution spec we also need to store the exact byte
    // representation provided by the client. because there is a good chance of information loss
    // when cycling from the serialized form to a deserialized form and back again, we take a
    // slight memory hit by deserializing it non-destructively from &Bytes such that we can still
    // pass the &Bytes on to the storage backend unmodified.
    let mut manifest = ManifestSpec::try_from(&bytes).map_err(|e| {
        tracing::warn!("error deserializing manifest: {e:?}");
        CoreError::ManifestInvalid(None)
    })?;

    match (manifest.media_type(), content_type) {
        (Some(_mt), None) => {
            // in theory we should error here, but it's going to be a pain in the ass if any
            // clients out there actually do neglect to include a content type so the best thing to
            // do is just allow it. relevant spec wording:
            // > If a manifest includes a mediaType field, clients MUST set the Content-Type header
            // to the value specified by the mediaType field.
            tracing::warn!("client neglected to include content type in header");
        }
        (Some(mt), Some(TypedHeader(ct))) => {
            if mt != ct.to_string().as_str().into() {
                return Err(CoreError::ManifestInvalid(None).into());
            }
        }
        (None, Some(TypedHeader(ct))) => {
            let s = ct.to_string();
            manifest.set_media_type(s.as_str());
        }
        (None, None) => {
            tracing::warn!(
                "neither mediaType content-type header included for manifest: {:?}",
                bytes
            );
            manifest.infer_media_type()?;
            if let Some(m) = manifest.media_type() {
                tracing::warn!("inferred media type as: {m}");
            }
        }
    }

    if let Some(TypedHeader(content_length)) = content_length {
        if content_length.0 > 4 * 1024 * 1024 {
            return Err(CoreError::SizeInvalid(None).into());
        }
    }

    let mut mstore = repository.get_manifest_store();
    let calculated_digest = mstore.put(&manifest_ref, &manifest, bytes).await?;

    let location = format!("/v2/{}/manifests/{}", repository.name(), mref);
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, HeaderValue::from_str(&location)?);
    headers.insert(
        HeaderName::from_lowercase(b"docker-content-digest")?,
        HeaderValue::from_str(String::from(calculated_digest).as_ref())?,
    );

    if let Some(subject) = manifest.subject() {
        headers.insert(
            HeaderName::from_lowercase(b"oci-subject")?,
            HeaderValue::from_str(subject.digest().as_str())?,
        );
    }

    Ok((StatusCode::CREATED, headers, "").into_response())
}

async fn delete_manifest(
    Extension(repository): Extension<Arc<dyn RepositoryStore>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let mref = path_params
        .get("reference")
        .ok_or_else(|| Error::MissingPathParameter("reference"))?;
    let manifest_ref = ManifestRef::from_str(mref)?;

    let mut mstore = repository.get_manifest_store();
    mstore.delete(&manifest_ref).await?;

    Ok((StatusCode::ACCEPTED, "").into_response())
}
