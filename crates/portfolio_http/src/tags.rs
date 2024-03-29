use axum::extract::{Extension, Query};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use http::StatusCode;
use serde::Deserialize;

use super::empty_string_as_none;
use super::errors::Result;
use super::ArcRepositoryStore;

pub fn router() -> Router {
    Router::new().route("/list", get(get_tags))
}

#[derive(Debug, Deserialize)]
struct GetListParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    n: Option<i64>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    last: Option<String>,
}

async fn get_tags(
    Extension(repository): Extension<ArcRepositoryStore>,
    Query(params): Query<GetListParams>,
) -> Result<Response> {
    let mstore = repository.get_manifest_store();
    let tags_list = mstore.get_tags_list(params.n, params.last).await?;

    Ok((StatusCode::OK, Json(tags_list)).into_response())
}
