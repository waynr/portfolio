use serde::Deserialize;

use crate::metadata;
use crate::objects;

#[derive(Deserialize)]
pub struct Config {
    pub metadata: MetadataBackend,
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
