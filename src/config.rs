use serde::Deserialize;

use crate::metadata;
use crate::objects;

#[derive(Deserialize)]
pub struct Config {
    pub metadata: MetadataBackend,
    pub objects: ObjectsBackend,
    pub static_registries: Option<Vec<RegistryDefinition>>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum MetadataBackend {
    Postgres(metadata::PostgresConfig),
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum ObjectsBackend {
    S3(objects::S3Config),
}

#[derive(Deserialize)]
pub struct RegistryDefinition {
    pub name: String,
    pub repositories: Vec<RepositoryDefinition>,
}

#[derive(Deserialize)]
pub struct RepositoryDefinition {
    pub name: String,
}
