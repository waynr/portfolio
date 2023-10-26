use std::collections::HashMap;
use std::marker::PhantomData;
use std::str::FromStr;

use axum::extract::{Path, State};
use axum::middleware::Next;
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use http::Response as HttpResponse;
use http_body::Body;
use serde::{de, Deserialize, Deserializer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::{self, TraceLayer};

mod errors;
pub use errors::Error;
pub use errors::Result;
pub mod headers;

pub(crate) mod blobs;
mod manifests;
mod referrers;
mod tags;

use portfolio_core::registry::RepositoryStore;
use portfolio_core::registry::RepositoryStoreManager;
use portfolio_core::DistributionErrorCode;

#[derive(Clone, Deserialize)]
pub struct RepositoryDefinition {
    pub name: String,
}

pub async fn add_basic_repository_extensions<B, M, R>(
    State(portfolio): State<Portfolio<M, R>>,
    Path(path_params): Path<HashMap<String, String>>,
    mut req: Request<B>,
    next: Next<B>,
) -> Result<Response>
where
    M: RepositoryStoreManager,
    R: RepositoryStore,
{
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    // NOTE/TODO: for now we automatically insert a repository if it's not already there but in the
    // future we need to implement some kind of limit
    let repository = match portfolio.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::NameUnknown,
            ));
        }
        Ok(Some(r)) => r,
        Ok(None) => portfolio
            .insert_repository(repo_name)
            .await
            .map_err(|e| e.into())?,
    };

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

async fn version() -> Result<Response> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str("application/json")?,
    );
    Ok((StatusCode::OK, headers, "{}").into_response())
}

#[derive(Clone)]
pub struct Portfolio<M, R>
where
    M: RepositoryStoreManager,
    R: RepositoryStore,
{
    manager: M,
    phantom: PhantomData<R>,
}

impl<M: RepositoryStoreManager, R: RepositoryStore> Portfolio<M, R> {
    pub fn new(manager: M) -> Self {
        Self {
            manager,
            phantom: PhantomData,
        }
    }

    pub async fn initialize_static_repositories(
        &self,
        repositories: Vec<RepositoryDefinition>,
    ) -> std::result::Result<(), M::Error> {
        for repository_config in repositories {
            match self.get_repository(&repository_config.name).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    tracing::info!(
                        "static repository '{}' not found, inserting into DB",
                        repository_config.name,
                    );
                    self.insert_repository(&repository_config.name).await?
                }
                Err(e) => return Err(e),
            };
        }
        Ok(())
    }

    pub async fn get_repository(
        &self,
        name: &str,
    ) -> std::result::Result<Option<M::RepositoryStore>, M::Error> {
        Ok(self.manager.get(name).await?)
    }

    pub async fn insert_repository(
        &self,
        name: &str,
    ) -> std::result::Result<M::RepositoryStore, M::Error> {
        Ok(self.manager.create(name).await?)
    }

    pub fn router(&self) -> Result<axum::Router> {
        let blobs = blobs::router::<R>();
        let manifests = manifests::router::<R>();
        let referrers = referrers::router::<R>();
        let tags = tags::router::<R>();

        let repository = Router::new()
            .nest("/blobs", blobs)
            .nest("/manifests", manifests)
            .nest("/referrers", referrers)
            .nest("/tags", tags);

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

        Ok(app)
    }
}
