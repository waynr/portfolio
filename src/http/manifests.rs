use std::collections::HashMap;
use std::str::FromStr;

use axum::{
    extract::{Extension, Path},
    http::header::{HeaderMap, HeaderName, HeaderValue},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use http::StatusCode;

use crate::{
    http::notimplemented, metadata::ManifestRef, objects::ObjectStore,
    registry::registries::Registry, DistributionErrorCode, Error, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new().route(
        "/:reference",
        get(get_manifest::<O>)
            .delete(notimplemented)
            .put(notimplemented)
            .head(head_manifest::<O>),
    )
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
