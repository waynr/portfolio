use serde::Deserialize;

use crate::registry::PostgresConfig;
use crate::registry::S3Config;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub metadata: MetadataBackend,
    pub objects: ObjectsBackend,
    pub static_registries: Option<Vec<RegistryDefinition>>,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum MetadataBackend {
    Postgres(PostgresConfig),
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectsBackend {
    S3(S3Config),
}

#[derive(Clone, Deserialize)]
pub struct RegistryDefinition {
    pub name: String,
    pub repositories: Vec<RepositoryDefinition>,
}

#[derive(Clone, Deserialize)]
pub struct RepositoryDefinition {
    pub name: String,
}
