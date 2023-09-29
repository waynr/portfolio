use std::collections::HashMap;
use std::str::FromStr;

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path},
    http::header::{HeaderMap, HeaderName, HeaderValue},
    response::{IntoResponse, Response},
    routing::get,
    Router, TypedHeader,
};
use headers::{ContentLength, ContentType};
use http::StatusCode;

use crate::{
    http::notimplemented,
    metadata::ManifestRef,
    objects::ObjectStore,
    registry::registries::Registry,
    registry::manifests::ManifestSpec,
    DistributionErrorCode, Error, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new()
        .route(
            "/:reference",
            get(get_manifest::<O>)
                .delete(notimplemented)
                .put(put_manifest::<O>)
                .head(head_manifest::<O>),
        )
        .layer(DefaultBodyLimit::max(6 * 1024 * 1024))
}

async fn head_manifest<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let repository = match registry.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::NameUnknown,
            ));
        }
        Ok(r) => r,
    };

    let mstore = repository.get_manifest_store();
    let manifest = mstore.get_manifest(&manifest_ref).await?;

    if let Some(manifest) = manifest {
        let mut headers = HeaderMap::new();
        let dgst: String = manifest.digest.into();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(dgst.as_str())?,
        );
        return Ok((StatusCode::OK, headers, "").into_response());
    }

    Err(Error::DistributionSpecError(
        DistributionErrorCode::ManifestBlobUnknown,
    ))
}

async fn get_manifest<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let repository = match registry.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::NameUnknown,
            ));
        }
        Ok(r) => r,
    };

    let mstore = repository.get_manifest_store();
    let manifest = mstore.get_manifest(&manifest_ref).await?;

    if let Some(manifest) = manifest {
        let body = manifest.body.ok_or(Error::DistributionSpecError(
            DistributionErrorCode::ManifestUnknown,
        ))?;
        let mut headers = HeaderMap::new();
        let dgst: String = manifest.digest.into();
        headers.insert(
            HeaderName::from_lowercase(b"docker-content-digest")?,
            HeaderValue::from_str(dgst.as_str())?,
        );
        let content_type: String = manifest.media_type.into();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_str(content_type.as_str())?,
        );
        return Ok((StatusCode::OK, headers, body).into_response());
    }

    Err(Error::DistributionSpecError(
        DistributionErrorCode::ManifestUnknown,
    ))
}

/// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pushing-manifests
async fn put_manifest<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    content_type: Option<TypedHeader<ContentType>>,
    content_length: Option<TypedHeader<ContentLength>>,
    Path(path_params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let manifest_ref = ManifestRef::from_str(
        path_params
            .get("reference")
            .ok_or_else(|| Error::MissingQueryParameter("reference"))?,
    )?;

    let repository = match registry.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::NameUnknown,
            ));
        }
        Ok(r) => r,
    };

    // we need to deserialize the request body into a type we can use to determine how to represent
    // it in the database, but according to distribution spec we also need to store the exact byte
    // representation provided by the client. because there is a good chance of information loss
    // when cycling from the serialized form to a deserialized form and back again, we take a
    // slight memory hit by deserializing it non-destructively from &Bytes such that we can still
    // pass the &Bytes on to the storage backend unmodified.
    let manifest = ManifestSpec::try_from(&bytes).map_err(|e| {
        tracing::warn!("error deserializing manifest: {e:?}");
        Error::DistributionSpecError(DistributionErrorCode::ManifestInvalid)
    })?;

    if let Some(TypedHeader(content_length)) = content_length {
        if content_length.0 > 4 * 1024 * 1024 {
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::SizeInvalid,
            ));
        }
    }

    let mstore = repository.get_manifest_store();
    mstore.upload(&manifest_ref, &manifest, bytes).await?;

    Err(Error::DistributionSpecError(
        DistributionErrorCode::ManifestUnknown,
    ))
}
