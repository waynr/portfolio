//! # Portfolio HTTP
//!
//! `portfolio_http` provides an implementation of the [Distribution
//! Spec](https://github.com/opencontainers/distribution-spec) that is generic over traits defined
//! in [`portfolio_core`] and therefore compatible with any number of possible implementations.
//!
//! ## Example `main.rs`
//!
//! Below is an example taken from the [`portfolio`] crate that demonstrates how one might
//! initialize an Axum HTTP server using a suitable backend implementation -- in this case, the
//! Postgres + S3 implementation found in [`portfolio_backend_postgres`].
//!
//! ```rust
//! use std::fs::File;
//! use std::io::Read;
//! use std::path::PathBuf;
//!
//! use anyhow::Result;
//! use axum::middleware;
//! use clap::Parser;
//!
//! use portfolio_backend_postgres::{PgRepository, PgRepositoryFactory};
//! use portfolio_http::{add_basic_repository_extensions, Portfolio};
//!
//! mod config;
//! use crate::config::{Config, RepositoryBackend};
//!
//! #[derive(Parser)]
//! struct Cli {
//!     #[arg(short, long)]
//!     config_file: Option<PathBuf>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let cli = Cli::parse();
//!
//!     // load configuration
//!     let mut dev_config = File::open(cli.config_file.unwrap_or("./dev-config.yml".into()))?;
//!     let mut s = String::new();
//!     dev_config.read_to_string(&mut s)?;
//!     let config: Config = serde_yaml::from_str(&s)?;
//!
//!     // initialize persistence layer
//!     let portfolio = match config.backend {
//!         RepositoryBackend::Postgres(cfg) => {
//!             let manager = cfg.get_manager().await?;
//!             Portfolio::<PgRepositoryFactory, PgRepository>::new(manager)
//!         }
//!     };
//!
//!     // configure static repositories
//!     if let Some(repositories) = config.static_repositories {
//!         portfolio
//!             .initialize_static_repositories(repositories)
//!             .await?;
//!     }
//!
//!     // retrieve axum router from Portfolio instance
//!     let router = match portfolio.router() {
//!         Err(e) => return Err(e.into()),
//!         Ok(r) => r,
//!     };
//!
//!     // add necessary Tower layer to inject RepositoryStore instances to all routes in the
//!     // router.
//!     let router = router.route_layer(middleware::from_fn_with_state(
//!         portfolio.clone(),
//!         add_basic_repository_extensions,
//!     ));
//!
//!     // run axum HTTP server
//!     axum::Server::bind(&"0.0.0.0:13030".parse()?)
//!         .serve(router.into_make_service())
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use http::Response as HttpResponse;
use http_body::Body;
use serde::{de, Deserialize, Deserializer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::{self, TraceLayer};

mod errors;
pub(crate) use errors::Error;
pub(crate) use errors::Result;

pub(crate) mod blobs;
pub(crate) mod headers;
mod manifests;
mod referrers;
mod tags;

use portfolio_core::registry::RepositoryStore;
use portfolio_core::registry::RepositoryStoreManager;
use portfolio_core::Error as CoreError;

/// Configuration struct defining parameters for statically-defined repositories initialized at
/// program startup if they don't already exist.
#[derive(Clone, Deserialize)]
pub struct RepositoryDefinition {
    /// Name of repository to initialize.
    pub name: String,
}

/// Adds a [`axum::Extension`] containing a [`RepositoryStore`] for use in HTTP handlers. This is
/// not included in the default [`axum::Router`] returned by [`self::Portfolio`] to enable users
/// to add their own logic to determin how repositories are created or accessed.
pub async fn add_basic_repository_extensions<B>(
    State(portfolio): State<Portfolio>,
    Path(path_params): Path<HashMap<String, String>>,
    mut req: Request<B>,
    next: Next<B>,
) -> Result<Response> {
    let repo_name = match path_params.get("repository") {
        Some(s) => s,
        None => return Err(Error::MissingPathParameter("repository")),
    };

    let repository = match portfolio.get_repository(repo_name).await {
        Err(e) => {
            tracing::warn!("error retrieving repository: {e:?}");
            return Err(CoreError::NameUnknown(None).into());
        }
        Ok(Some(r)) => r,
        Ok(None) => portfolio.insert_repository(repo_name).await?,
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

/// Centralizes management of Portfolio registries and provides an [`axum::Router`] that implements
/// the [Distribution Spec](https://github.com/opencontainers/distribution-spec).
#[derive(Clone)]
pub struct Portfolio {
    manager: Arc<dyn RepositoryStoreManager>,
}

impl Portfolio {
    pub fn new(manager: Arc<dyn RepositoryStoreManager>) -> Self {
        Self { manager }
    }

    pub async fn initialize_static_repositories(
        &self,
        repositories: Vec<RepositoryDefinition>,
    ) -> std::result::Result<(), portfolio_core::Error> {
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

    async fn get_repository(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Box<dyn RepositoryStore + Send + Sync>>, portfolio_core::Error>
    {
        Ok(self.manager.get(name).await?)
    }

    async fn insert_repository(
        &self,
        name: &str,
    ) -> std::result::Result<Box<dyn RepositoryStore>, portfolio_core::Error> {
        Ok(self.manager.create(name).await?)
    }

    /// Return an [`axum::Router`] that implements the Distribution Specification.
    pub fn router(&self) -> Result<axum::Router> {
        let blobs = blobs::router();
        let manifests = manifests::router();
        let referrers = referrers::router();
        let tags = tags::router();

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
