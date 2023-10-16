use axum::{
    extract::{Extension, Query},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use http::StatusCode;
use serde::Deserialize;

use crate::{
    http::empty_string_as_none, objects::ObjectStore, registry::repositories::Repository,
    Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new().route("/list", get(get_tags::<O>))
}

#[derive(Debug, Deserialize)]
struct GetListParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    n: Option<i64>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    last: Option<String>,
}

async fn get_tags<O: ObjectStore>(
    Extension(repository): Extension<Repository<O>>,
    Query(params): Query<GetListParams>,
) -> Result<Response> {
    let tags_list = repository.get_tags(params.n, params.last).await?;

    Ok((StatusCode::OK, Json(tags_list)).into_response())
}
