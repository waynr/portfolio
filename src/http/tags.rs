use axum::extract::{Extension, Query};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use http::StatusCode;
use serde::Deserialize;

use crate::http::empty_string_as_none;
use crate::registry::RepositoryStore;
use crate::Result;

pub fn router<R: RepositoryStore>() -> Router {
    Router::new().route("/list", get(get_tags::<R>))
}

#[derive(Debug, Deserialize)]
struct GetListParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    n: Option<i64>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    last: Option<String>,
}

async fn get_tags<R: RepositoryStore>(
    Extension(repository): Extension<R>,
    Query(params): Query<GetListParams>,
) -> Result<Response> {
    let tags_list = repository.get_tags(params.n, params.last).await?;

    Ok((StatusCode::OK, Json(tags_list)).into_response())
}
