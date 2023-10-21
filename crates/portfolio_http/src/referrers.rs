use std::collections::HashMap;

use axum::extract::{Extension, Path, Query};
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use http::StatusCode;
use oci_spec::image::MediaType;
use serde::Deserialize;

use portfolio_core::registry::{ManifestStore, RepositoryStore};
use portfolio_core::{Error, OciDigest, Result};

use super::empty_string_as_none;

pub fn router<R: RepositoryStore>() -> Router {
    Router::new().route("/:digest", get(get_referrers::<R>))
}

#[derive(Debug, Deserialize)]
struct GetParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    artifact_type: Option<String>,
}

async fn get_referrers<R: RepositoryStore>(
    Extension(repository): Extension<R>,
    Path(path_params): Path<HashMap<String, String>>,
    Query(params): Query<GetParams>,
) -> Result<Response> {
    let digest: &str = path_params
        .get("digest")
        .ok_or_else(|| Error::MissingQueryParameter("digest"))?;
    let oci_digest: OciDigest = digest.try_into()?;

    let mstore = repository.get_manifest_store();
    let image_index = mstore
        .get_referrers(&oci_digest, params.artifact_type.clone())
        .await
        .map_err(|e| e.into())?;

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
