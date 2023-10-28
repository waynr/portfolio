use serde::Deserialize;

use portfolio_backend_postgres::PgRepositoryConfig;
use portfolio_http::RepositoryDefinition;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub backend: RepositoryBackend,
    pub static_repositories: Option<Vec<RepositoryDefinition>>,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum RepositoryBackend {
    Postgres(PgRepositoryConfig),
}
