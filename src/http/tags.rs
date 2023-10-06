use std::collections::HashMap;

use axum::{
    extract::{Extension, Path, Query},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use http::StatusCode;

use crate::{
    objects::ObjectStore, registry::registries::Registry, DistributionErrorCode, Error, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new().route("/list", get(get_tags::<O>))
}

async fn get_tags<O: ObjectStore>(
    Extension(registry): Extension<Registry<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    Query(query_params): Query<HashMap<String, String>>,
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

    let tags_list = Json(repository.get_tags().await?);

    Ok((StatusCode::OK, tags_list).into_response())
}
