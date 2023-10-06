use std::collections::HashMap;
use std::str::FromStr;

use axum::{
    extract::{Extension, Path, Query},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use http::StatusCode;
use serde::{de, Deserialize, Deserializer};

use crate::{
    objects::ObjectStore, registry::registries::Registry, DistributionErrorCode, Error, Result,
};

pub fn router<O: ObjectStore>() -> Router {
    Router::new().route("/list", get(get_tags::<O>))
}

/// Serde deserialization decorator to map empty Strings to None,
fn empty_string_as_none<'de, D, T>(de: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
    }
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
