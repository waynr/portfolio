use std::collections::HashMap;

use axum::{
    extract::{Extension, Path, Query},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use http::StatusCode;
use serde::Deserialize;

use crate::{
    http::empty_string_as_none, objects::ObjectStore, registry::registries::Registry,
    DistributionErrorCode, Error, Result,
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
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    Query(params): Query<GetListParams>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let repository = match registry.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::NameUnknown,
            ));
        }
        Ok(r) => r,
    };

    let tags_list = Json(repository.get_tags(params.n, params.last).await?);

    Ok((StatusCode::OK, tags_list).into_response())
}
