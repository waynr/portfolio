use std::collections::HashMap;
use std::str::FromStr;

use axum::{
    body::{Bytes, StreamBody},
    extract::{DefaultBodyLimit, Extension, Path},
    http::header::{self, HeaderMap, HeaderName, HeaderValue},
    response::{IntoResponse, Response},
    routing::get,
    Router, TypedHeader,
};
use headers::{ContentLength, ContentType};
use http::StatusCode;

use crate::{
    metadata::ManifestRef, objects::ObjectStore, registry::manifests::ManifestSpec,
    registry::registries::Repository, DistributionErrorCode, Error, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new()
        .route(
            "/:reference",
            get(get_manifest::<O>)
                .delete(delete_manifest::<O>)
                .put(put_manifest::<O>)
                .head(head_manifest::<O>),
        )
        .layer(DefaultBodyLimit::max(6 * 1024 * 1024))
}

async fn head_manifest<O: ObjectStore>(
    Extension(repository): Extension<Repository<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let mstore = repository.get_manifest_store();
    let manifest = mstore.head_manifest(&manifest_ref).await?;

    if let Some(manifest) = manifest {
        let mut headers = HeaderMap::new();
        let dgst: String = manifest.digest.into();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(dgst.as_str())?,
        );
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(manifest.bytes_on_disk.to_string().as_str())?,
        );
        return Ok((StatusCode::OK, headers, "").into_response());
    }

    Err(Error::DistributionSpecError(
        DistributionErrorCode::ManifestBlobUnknown,
    ))
}

async fn get_manifest<O: ObjectStore>(
    Extension(repository): Extension<Repository<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let mstore = repository.get_manifest_store();
    let (manifest, body) = if let Some((m, b)) = mstore.get_manifest(&manifest_ref).await? {
        (m, b)
    } else {
        return Err(Error::DistributionSpecError(
            DistributionErrorCode::ManifestUnknown,
        ));
    };

    let mut headers = HeaderMap::new();
    let dgst: String = manifest.digest.into();
    headers.insert(
        HeaderName::from_lowercase(b"docker-content-digest")?,
        HeaderValue::from_str(dgst.as_str())?,
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(manifest.bytes_on_disk.to_string().as_str())?,
    );
    if let Some(mt) = manifest.media_type {
        let content_type: String = mt.into();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_str(content_type.as_str())?,
        );
    }
    Ok((StatusCode::OK, headers, StreamBody::new(body)).into_response())
}

/// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pushing-manifests
async fn put_manifest<O: ObjectStore>(
    Extension(repository): Extension<Repository<O>>,
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
        Error::DistributionSpecError(DistributionErrorCode::ManifestInvalid)
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
                return Err(Error::DistributionSpecError(
                    DistributionErrorCode::ManifestInvalid,
                ));
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
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::SizeInvalid,
            ));
        }
    }

    let mut mstore = repository.get_manifest_store();
    let calculated_digest = mstore.upload(&manifest_ref, &manifest, bytes).await?;

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

async fn delete_manifest<O: ObjectStore>(
    Extension(repository): Extension<Repository<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let mref = path_params
        .get("reference")
        .ok_or_else(|| Error::MissingPathParameter("reference"))?;
    let manifest_ref = ManifestRef::from_str(mref)?;

    let mut mstore = repository.get_manifest_store();
    match mstore.delete(&manifest_ref).await {
        Ok(_) => Ok((StatusCode::ACCEPTED, "").into_response()),
        Err(e @ Error::DistributionSpecError(DistributionErrorCode::ContentReferenced)) => {
            Ok(e.into_response())
        }
        Err(_) => Ok((StatusCode::NOT_FOUND, "").into_response()),
    }
}
