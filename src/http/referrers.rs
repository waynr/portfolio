use std::collections::HashMap;

use axum::{
    extract::{Extension, Path, Query},
    http::header::{self, HeaderMap, HeaderName, HeaderValue},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use http::StatusCode;
use oci_spec::image::MediaType;
use serde::Deserialize;

use crate::{
    http::empty_string_as_none, objects::ObjectStore, registry::registries::Registry,
    DistributionErrorCode, Error, OciDigest, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new().route("/:digest", get(get_referrers::<O>))
}

#[derive(Debug, Deserialize)]
struct GetParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    artifact_type: Option<String>,
}

async fn get_referrers<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    Query(params): Query<GetParams>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

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
    let image_index = mstore
        .get_referrers(&oci_digest, params.artifact_type.clone())
        .await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(MediaType::ImageIndex.to_string().as_str())?,
    );

    if let Some(artifact_type) = &params.artifact_type {
        headers.insert(
            HeaderName::from_lowercase(b"oci-filters-applied")?,
            HeaderValue::from_str(artifact_type.as_str())?,
        );
    }

    Ok((StatusCode::OK, headers, Json(image_index)).into_response())
}
