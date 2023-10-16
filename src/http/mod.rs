use serde::{de, Deserialize, Deserializer};
use std::collections::HashMap;
use std::str::FromStr;

use axum::{
    extract::{Path, State},
    http::header::{self, HeaderMap, HeaderName, HeaderValue},
    http::{Request, StatusCode},
    middleware::{self as axum_middleware, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use http::Response as HttpResponse;
use http_body::Body;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::{self, TraceLayer};

pub mod headers;

pub(crate) mod blobs;
mod manifests;
mod referrers;
mod tags;

use crate::errors::{DistributionErrorCode, Error, Result};
use crate::objects::ObjectStore;
use crate::Portfolio;

async fn auth<B, O: ObjectStore>(
    State(portfolio): State<Portfolio<O>>,
    Path(path_params): Path<HashMap<String, String>>,
    mut req: Request<B>,
    next: Next<B>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let registry = match portfolio.get_registry("meow").await {
        Err(_) => {
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::Unauthorized,
            ))
        }
        Ok(r) => r,
    };

    // NOTE/TODO: for now we automatically insert a repository if it's not already there but in the
    // future we need to implement some kind of limit
    let repository = match registry.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::NameUnknown,
            ));
        }
        Ok(Some(r)) => r,
        Ok(None) => registry.insert_repository(repo_name).await?,
    };

    req.extensions_mut().insert(registry);
    req.extensions_mut().insert(repository);

    Ok(next.run(req).await)
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

fn maybe_get_content_length(response: &HttpResponse<impl Body>) -> Option<HeaderValue> {
    if let Some(size) = response.body().size_hint().exact() {
        Some(
            HeaderValue::from_str(&size.to_string())
                .expect("size should have valid to_string conversion"),
        )
    } else {
        None
    }
}

pub async fn serve<O: ObjectStore>(portfolio: Portfolio<O>) -> Result<()> {
    let blobs = blobs::router::<O>();
    let manifests = manifests::router::<O>();
    let referrers = referrers::router::<O>();
    let tags = tags::router::<O>();

    let repository = Router::new()
        .nest("/blobs", blobs)
        .nest("/manifests", manifests)
        .nest("/referrers", referrers)
        .nest("/tags", tags)
        .route_layer(axum_middleware::from_fn_with_state(portfolio.clone(), auth));

    let app = Router::new()
        .route("/v2/", get(version))
        .nest("/v2/:repository", repository)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().include_headers(true))
                .on_response(trace::DefaultOnResponse::new())
                .on_request(trace::DefaultOnRequest::new()),
        )
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_str("docker-distribution-api-version")?,
            HeaderValue::from_str("registry/2.0")?,
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_str("content-type")?,
            HeaderValue::from_str("application/json")?,
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CONTENT_LENGTH,
            maybe_get_content_length,
        ));

    axum::Server::bind(&"0.0.0.0:13030".parse()?)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn version() -> Result<Response> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str("application/json")?,
    );
    Ok((StatusCode::OK, headers, "{}").into_response())
}
