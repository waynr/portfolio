use serde::Deserialize;

use crate::metadata;
use crate::objects;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub metadata: MetadataBackend,
    pub objects: ObjectsBackend,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum MetadataBackend {
    Postgres(metadata::PostgresConfig),
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectsBackend {
    S3(objects::S3Config),
}
