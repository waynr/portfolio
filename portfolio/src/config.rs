use serde::Deserialize;

use crate::registry::impls::postgres_s3::config::PgS3RepositoryConfig;

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

#[derive(Clone, Deserialize)]
pub struct RepositoryDefinition {
    pub name: String,
}
