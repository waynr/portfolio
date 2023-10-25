use serde::Deserialize;

use portfolio_backend_postgres::PgS3RepositoryConfig;
use portfolio_http::RepositoryDefinition;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub backend: RepositoryBackend,
    pub static_repositories: Option<Vec<RepositoryDefinition>>,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum RepositoryBackend {
    PostgresS3(PgS3RepositoryConfig),
}
